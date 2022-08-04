use std::{collections::HashMap, io::Result as IoResultExt, sync, sync::Arc, thread::JoinHandle};

use ::models::{FieldInfo, InMemPoint, SeriesInfo, Tag, ValueType};
use futures::stream::SelectNextSome;
use models::{FieldId, SeriesId, Timestamp};
use parking_lot::{Mutex, RwLock};
use protos::models::Points;
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use snafu::ResultExt;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use trace::{debug, error, info, trace, warn};

use crate::memcache::MemRaw;
use crate::{
    compaction::{run_flush_memtable_job, FlushReq},
    context::GlobalContext,
    error::{self, Result},
    file_manager::{self, FileManager},
    file_utils,
    index::forward_index::ForwardIndex,
    kv_option::{DBOptions, Options, QueryOption, TseriesFamDesc, TseriesFamOpt, WalConfig},
    memcache::{DataType, MemCache},
    record_file::Reader,
    summary,
    summary::{Summary, SummaryProcessor, SummaryTask, VersionEdit},
    tseries_family::{TimeRange, Version},
    tsm::TsmTombstone,
    version_set,
    version_set::VersionSet,
    wal::{self, WalEntryType, WalManager, WalTask},
    Error, Task,
};

pub struct Entry {
    pub series_id: u64,
}

pub struct TsKv {
    options: Arc<Options>,
    version_set: Arc<RwLock<VersionSet>>,

    wal_sender: UnboundedSender<WalTask>,
    forward_index: Arc<RwLock<ForwardIndex>>,

    flush_task_sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>,
    summary_task_sender: UnboundedSender<SummaryTask>,
}

impl TsKv {
    pub async fn open(opt: Options) -> Result<Self> {
        let shared_options = Arc::new(opt);
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        let (version_set, summary) =
            Self::recover(shared_options.clone(), flush_task_sender.clone()).await;
        let mut fidx = ForwardIndex::new(&shared_options.forward_index_conf.path);
        fidx.load_cache_file()
            .await
            .map_err(|err| Error::LogRecordErr { source: err })?;
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel();
        let core = Self {
            options: shared_options,
            forward_index: Arc::new(RwLock::new(fidx)),
            version_set,
            wal_sender,
            flush_task_sender,
            summary_task_sender: summary_task_sender.clone(),
        };
        core.run_wal_job(wal_receiver);
        core.run_flush_job(
            flush_task_receiver,
            summary.global_context(),
            summary.version_set(),
            summary_task_sender.clone(),
        );
        core.run_summary_job(summary, summary_task_receiver, summary_task_sender);

        Ok(core)
    }

    async fn recover(
        opt: Arc<Options>,
        flush_task_sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>,
    ) -> (Arc<RwLock<VersionSet>>, Summary) {
        if !file_manager::try_exists(&opt.db.db_path) {
            std::fs::create_dir_all(&opt.db.db_path)
                .context(error::IOSnafu)
                .unwrap();
        }
        let summary_file = file_utils::make_summary_file(&opt.db.db_path, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(&opt.db).await.unwrap()
        } else {
            Summary::new(&opt.db).await.unwrap()
        };
        let version_set = summary.version_set().clone();
        let wal_manager = WalManager::new(opt.wal.clone());
        wal_manager
            .recover(
                version_set.clone(),
                summary.global_context().clone(),
                flush_task_sender,
            )
            .await
            .unwrap();

        (version_set.clone(), summary)
    }

    pub async fn write(
        &self,
        write_batch: WritePointsRpcRequest,
    ) -> Result<WritePointsRpcResponse> {
        let shared_write_batch = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch)
            .context(error::InvalidFlatbufferSnafu)?;

        // get or create forward index
        for point in fb_points.points().unwrap() {
            let info = SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?;
            self.forward_index
                .write()
                .add_series_info_if_not_exists(info)
                .await
                .context(error::ForwardIndexErrSnafu)?;
        }

        // write wal
        let (cb, rx) = oneshot::channel();
        self.wal_sender
            .send(WalTask::Write {
                points: shared_write_batch.clone(),
                cb,
            })
            .map_err(|err| Error::Send)?;
        let (seq, _) = rx.await.context(error::ReceiveSnafu)??;
        self.insert_cache(seq, &fb_points).await;
        Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        })
    }

    pub async fn read_point(&self, sid: SeriesId, time_range: &TimeRange, field_id: FieldId) {
        let version_set = self.version_set.read();
        if let Some(tsf) = version_set.get_tsfamily_immut(sid) {
            // get data from memcache
            if let Some(mem_entry) = tsf.cache().read().data_cache.get(&field_id) {
                info!("memcache::{}::{}", sid.clone(), field_id);
                mem_entry.read_cell(time_range);
            }

            // get data from delta_memcache
            if let Some(mem_entry) = tsf.delta_cache().read().data_cache.get(&field_id) {
                info!("delta memcache::{}::{}", sid.clone(), field_id);
                mem_entry.read_cell(time_range);
            }

            // get data from immut_delta_memcache
            for mem_cache in tsf.delta_immut_cache().iter() {
                if mem_cache.read().flushed {
                    continue;
                }
                if let Some(mem_entry) = mem_cache.read().data_cache.get(&field_id) {
                    info!("delta im_memcache::{}::{}", sid.clone(), field_id);
                    mem_entry.read_cell(time_range);
                }
            }

            // get data from im_memcache
            for mem_cache in tsf.im_cache().iter() {
                if mem_cache.read().flushed {
                    continue;
                }
                if let Some(mem_entry) = mem_cache.read().data_cache.get(&field_id) {
                    info!("im_memcache::{}::{}", sid.clone(), field_id);
                    mem_entry.read_cell(time_range);
                }
            }

            // get data from levelinfo
            for level_info in tsf.version().read().levels_info.iter() {
                if level_info.level == 0 {
                    continue;
                }
                info!("levelinfo::{}::{}", sid.clone(), field_id);
                level_info.read_columnfile(tsf.tf_id(), field_id, time_range);
            }

            // get data from delta
            let level_info = &tsf.version().read().levels_info;
            if !level_info.is_empty() {
                info!("delta::{}::{}", sid.clone(), field_id);
                level_info[0].read_columnfile(tsf.tf_id(), field_id, time_range);
            }
        } else {
            warn!("ts_family with sid {} not found.", sid);
        }
    }

    pub async fn read(&self, sids: Vec<SeriesId>, time_range: &TimeRange, fields: Vec<FieldId>) {
        for sid in sids {
            for field_id in fields.iter() {
                self.read_point(sid, time_range, *field_id).await;
            }
        }
    }

    pub async fn delete_series(
        &self,
        sids: Vec<SeriesId>,
        min: Timestamp,
        max: Timestamp,
    ) -> Result<()> {
        let series_infos = self.forward_index.read().get_series_info_list(&sids);
        let timerange = TimeRange {
            max_ts: max,
            min_ts: min,
        };
        let path = self.options.db.db_path.clone();
        for series_info in series_infos {
            let vs = self.version_set.read();
            if let Some(tsf) = vs.get_tsfamily_immut(series_info.series_id()) {
                tsf.delete_cache(&TimeRange {
                    min_ts: min,
                    max_ts: max,
                })
                .await;
                let version = tsf.version().read();
                for level in version.levels_info() {
                    if level.ts_range.overlaps(&timerange) {
                        for column_file in level.files.iter() {
                            if column_file.range().overlaps(&timerange) {
                                let field_ids: Vec<FieldId> = series_info
                                    .field_infos()
                                    .iter()
                                    .map(|f| f.field_id())
                                    .collect();
                                let mut tombstone =
                                    TsmTombstone::open_for_write(&path, column_file.file_id())?;
                                tombstone.add_range(&field_ids, min, max)?;
                                tombstone.flush()?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn insert_cache(&self, seq: u64, ps: &Points<'_>) {
        if let Some(points) = ps.points() {
            let mut version_set = self.version_set.write();
            for point in points.iter() {
                let p = InMemPoint::from(point);
                let sid = p.series_id();
                if let Some(tsf) = version_set.get_tsfamily(sid) {
                    for f in p.fields().iter() {
                        tsf.put_mutcache(
                            &mut MemRaw {
                                seq,
                                ts: point.timestamp() as i64,
                                field_id: f.field_id(),
                                field_type: f.value_type,
                                val: &f.value,
                            },
                            self.flush_task_sender.clone(),
                        )
                        .await
                    }
                } else {
                    warn!("ts_family for sid {} not found.", sid);
                }
            }
        }
    }

    fn run_wal_job(&self, mut receiver: UnboundedReceiver<WalTask>) {
        warn!("job 'WAL' starting.");
        let wal_opt = self.options.wal.clone();
        let mut wal_manager = WalManager::new(wal_opt);
        let f = async move {
            while let Some(x) = receiver.recv().await {
                match x {
                    WalTask::Write { points, cb } => {
                        // write wal
                        let ret = wal_manager.write(WalEntryType::Write, &points).await;
                        let send_ret = cb.send(ret);
                        match send_ret {
                            Ok(wal_result) => {}
                            Err(err) => {
                                warn!("send WAL write result failed.")
                            }
                        }
                    }
                }
            }
        };
        tokio::spawn(f);
        warn!("job 'WAL' started.");
    }

    fn run_flush_job(
        &self,
        mut receiver: UnboundedReceiver<Arc<Mutex<Vec<FlushReq>>>>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        sender: UnboundedSender<SummaryTask>,
    ) {
        let f = async move {
            while let Some(x) = receiver.recv().await {
                run_flush_memtable_job(
                    x.clone(),
                    ctx.clone(),
                    HashMap::new(),
                    version_set.clone(),
                    sender.clone(),
                )
                .await
                .unwrap();
            }
        };
        tokio::spawn(f);
        warn!("Flush task handler started");
    }

    fn run_summary_job(
        &self,
        summary: Summary,
        mut summary_task_receiver: UnboundedReceiver<SummaryTask>,
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) {
        let f = async move {
            let mut summary_processor = summary::SummaryProcessor::new(Box::new(summary));
            while let Some(x) = summary_task_receiver.recv().await {
                debug!("Apply Summary task");
                summary_processor.batch(x);
                summary_processor.apply().await;
            }
        };
        tokio::spawn(f);
        warn!("Summary task handler started");
    }

    pub fn start(tskv: TsKv, mut req_rx: UnboundedReceiver<Task>) {
        warn!("job 'main' starting.");
        let f = async move {
            while let Some(command) = req_rx.recv().await {
                match command {
                    Task::WritePoints { req, tx } => {
                        warn!("writing points.");
                        match tskv.write(req).await {
                            Ok(resp) => {
                                let _ret = tx.send(Ok(resp));
                            }
                            Err(err) => {
                                let _ret = tx.send(Err(err));
                            }
                        }
                        warn!("write points completed.");
                    }
                    _ => panic!("unimplemented."),
                }
            }
        };

        tokio::spawn(f);
        warn!("job 'main' started.");
    }

    pub fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        self.version_set.clone()
    }
    pub async fn query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}
