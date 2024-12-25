use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cache::AsyncCache;
use datafusion::arrow::record_batch::RecordBatch;
use memory_pool::{MemoryPool, MemoryPoolRef};
use meta::error::MetaError;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeId;
use models::predicate::domain::ColumnDomains;
use models::schema::database_schema::{make_owner, split_owner};
use models::{SeriesId, SeriesKey};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::broadcast::{self, Sender as BroadcastSender};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use trace::{debug, error, info, warn};

use crate::compaction::job::CompactJob;
use crate::compaction::metrics::{CompactionType, VnodeCompactionMetrics};
use crate::compaction::{self, check, pick_compaction, CompactTask};
use crate::database::Database;
use crate::error::{FileSystemSnafu, IndexErrSnafu, MetaSnafu, TskvResult};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::index::IndexResult;
use crate::kv_option::{Options, StorageOptions};
use crate::summary::{Summary, SummaryTask};
use crate::tsfamily::super_version::SuperVersion;
use crate::tsfamily::tseries_family::TseriesFamily;
use crate::version_set::VersionSet;
use crate::vnode_store::VnodeStorage;
use crate::{file_utils, Engine, TsKvContext};
// TODO: A small summay channel capacity can cause a block
pub const COMPACT_REQ_CHANNEL_CAP: usize = 1024;
pub const SUMMARY_REQ_CHANNEL_CAP: usize = 1024;

pub struct TsKv {
    ctx: Arc<TsKvContext>,
    meta_manager: MetaRef,
    compact_job: CompactJob,
    runtime: Arc<Runtime>,
    vnodes: Arc<RwLock<HashMap<VnodeId, VnodeStorage>>>,
    metrics: Arc<MetricsRegister>,
    _memory_pool: Arc<dyn MemoryPool>,
    close_sender: BroadcastSender<Sender<()>>,
}

impl TsKv {
    pub async fn open(
        meta_manager: MetaRef,
        options: Options,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        metrics: Arc<MetricsRegister>,
    ) -> TskvResult<TsKv> {
        let (compact_task_sender, compact_task_receiver) = mpsc::channel(COMPACT_REQ_CHANNEL_CAP);
        let (summary_task_sender, summary_task_receiver) = mpsc::channel(SUMMARY_REQ_CHANNEL_CAP);

        let shared_options = Arc::new(options);
        let (version_set, summary) = Self::recover_summary(
            runtime.clone(),
            memory_pool.clone(),
            meta_manager.clone(),
            shared_options.clone(),
            metrics.clone(),
        )
        .await?;

        let ctx = Arc::new(TsKvContext {
            version_set,
            compact_task_sender,
            summary_task_sender,
            runtime: runtime.clone(),
            options: shared_options.clone(),
            global_ctx: summary.global_context(),
        });

        let (close_sender, _close_receiver) = broadcast::channel(1);
        let compact_job = CompactJob::new(runtime.clone(), ctx.clone(), metrics.clone());
        let core = Self {
            ctx,
            meta_manager,
            _memory_pool: memory_pool,
            compact_job,
            close_sender,
            metrics,
            runtime,
            vnodes: Default::default(),
        };

        core.run_summary_job(summary, summary_task_receiver);
        core.run_flush_cold_vnode_job();
        core.compact_job
            .start_merge_compact_task_job(compact_task_receiver)
            .await;
        core.compact_job.start_vnode_compaction_job().await;
        Ok(core)
    }

    pub fn context(&self) -> Arc<TsKvContext> {
        self.ctx.clone()
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_summary(
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        meta: MetaRef,
        opt: Arc<Options>,
        metrics: Arc<MetricsRegister>,
    ) -> TskvResult<(Arc<RwLock<VersionSet>>, Summary)> {
        let summary_dir = opt.storage.summary_dir();
        LocalFileSystem::create_dir_if_not_exists(Some(&summary_dir)).context(FileSystemSnafu)?;
        let summary_file = file_utils::make_summary_file(&summary_dir, 0);
        let summary = if LocalFileSystem::try_exists(&summary_file) {
            Summary::recover(meta, opt, runtime, memory_pool, metrics.clone())
                .await
                .unwrap()
        } else {
            Summary::new(opt, runtime, meta, memory_pool, metrics)
                .await
                .unwrap()
        };
        let version_set = summary.version_set();

        Ok((version_set, summary))
    }

    fn run_summary_job(
        &self,
        mut summary: Summary,
        mut summary_task_receiver: Receiver<SummaryTask>,
    ) {
        self.runtime.spawn(async move {
            let mut roll_file_time_stamp = models::utils::now_timestamp_secs();

            while let Some(task) = summary_task_receiver.recv().await {
                info!("Apply Summary task: {:?}", task.request.version_edit);
                let result = summary.apply_version_edit(&task.request).await;
                if let Err(err) = task.call_back.send(result) {
                    trace::info!("Response apply version edit failed: {:?}", err);
                }

                // Try to rolling summary file every 10mins
                if models::utils::now_timestamp_secs() - roll_file_time_stamp > 10 * 60 {
                    if let Err(err) = summary.roll_summary_file().await {
                        error!("roll summary file failed: {:?}", err);
                    } else {
                        roll_file_time_stamp = models::utils::now_timestamp_secs();
                    }
                }
            }
        });

        info!("Summary task handler started");
    }

    fn run_flush_cold_vnode_job(&self) {
        let tskv_ctx = self.ctx.clone();
        let compact_trigger_cold_duration = tskv_ctx.options.storage.compact_trigger_cold_duration;
        let compact_trigger_file_num = tskv_ctx.options.storage.compact_trigger_file_num;
        let compact_task_sender = tskv_ctx.compact_task_sender.clone();
        if compact_trigger_cold_duration == Duration::ZERO {
            return;
        }

        let vnodes = self.vnodes.clone();
        self.runtime.spawn(async move {
            let mut cold_check_interval = tokio::time::interval(compact_trigger_cold_duration);
            loop {
                cold_check_interval.tick().await;

                let dbs = tskv_ctx.version_set.read().await.get_all_db().clone();
                for (_, db) in dbs {
                    let ts_families = db.read().await.ts_families().clone();
                    for (tf_id, ts_family) in ts_families {
                        let last_modified = ts_family.read().await.get_last_modified().await;
                        if let Some(last_modified) = last_modified {
                            if last_modified.elapsed() < compact_trigger_cold_duration {
                                continue;
                            }

                            let vnode_opt = vnodes.read().await.get(&tf_id).cloned();
                            if let Some(vnode) = vnode_opt {
                                if let Err(e) = vnode.flush(true, true, false).await {
                                    trace::error!("flush code vnode {} faild: {:?}", tf_id, e)
                                }
                            }
                        }
                        let version = ts_family.read().await.super_version().version.clone();
                        let levels = version.levels_info();
                        let task = if levels[0].files.len() as u32 >= compact_trigger_file_num {
                            CompactTask::Delta(tf_id)
                        } else {
                            CompactTask::Normal(tf_id)
                        };
                        if let Err(e) = compact_task_sender.send(task).await {
                            warn!("Scheduler(vnode: {tf_id}): Failed to send compact task: {task}: {e}");
                        }
                        version.tsm_reader_cache().clear().await
                    }
                }
            }
        });
    }

    async fn sync_indexs(&self) -> IndexResult<()> {
        let vnodes_guard = self.vnodes.read().await;
        for (_, vnode_storage) in vnodes_guard.iter() {
            vnode_storage.sync_index().await;
        }
        Ok(())
    }

    pub async fn get_db(&self, tenant: &str, database: &str) -> Option<Arc<RwLock<Database>>> {
        self.ctx.version_set.read().await.get_db(tenant, database)
    }

    pub(crate) async fn get_db_or_else_create(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> TskvResult<Arc<RwLock<Database>>> {
        if let Some(db) = self.ctx.version_set.read().await.get_db(tenant, db_name) {
            return Ok(db);
        }

        let db_schema = self
            .meta_manager
            .tenant_meta(tenant)
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetaSnafu)?
            .get_db_schema(db_name)
            .context(MetaSnafu)?
            .ok_or(MetaError::DatabaseNotFound {
                database: db_name.to_string(),
            })
            .context(MetaSnafu)?;

        let db = self
            .ctx
            .version_set
            .write()
            .await
            .create_db(db_schema)
            .await?;
        Ok(db)
    }

    pub(crate) async fn get_tsfamily_or_else_create(
        &self,
        id: VnodeId,
        db: Arc<RwLock<Database>>,
    ) -> TskvResult<Arc<RwLock<TseriesFamily>>> {
        let mut db = db.write().await;
        if let Some(tf) = db.get_tsfamily(id) {
            return Ok(tf);
        }

        db.create_tsfamily(id, self.ctx.clone()).await
    }
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn open_tsfamily(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<VnodeStorage> {
        let database = self.get_db_or_else_create(tenant, db_name).await?;

        let ts_index = database.write().await.get_ts_index_or_add(vnode_id).await?;
        let ts_family = self
            .get_tsfamily_or_else_create(vnode_id, database.clone())
            .await?;

        let vnode = VnodeStorage::new(vnode_id, database, ts_index, ts_family, self.ctx.clone());
        self.vnodes.write().await.insert(vnode_id, vnode.clone());

        Ok(vnode)
    }

    async fn remove_tsfamily(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<()> {
        self.vnodes.write().await.remove(&vnode_id);

        if let Some(db) = self.ctx.version_set.read().await.get_db(tenant, database) {
            let mut db_wlock = db.write().await;
            db_wlock.del_ts_index(vnode_id);
            db_wlock
                .del_tsfamily(vnode_id, self.ctx.summary_task_sender.clone())
                .await;

            let ts_dir = self
                .ctx
                .options
                .storage
                .ts_family_dir(&make_owner(tenant, database), vnode_id);
            match std::fs::remove_dir_all(&ts_dir) {
                Ok(()) => {
                    info!("Removed TsFamily directory '{}'", ts_dir.display());
                }
                Err(e) => {
                    error!(
                        "Failed to remove TsFamily directory '{}': {}",
                        ts_dir.display(),
                        e
                    );
                }
            }
        }

        Ok(())
    }

    async fn flush_tsfamily(
        &self,
        _tenant: &str,
        _database: &str,
        vnode_id: VnodeId,
        trigger_compact: bool,
    ) -> TskvResult<()> {
        let vnode_opt = self.vnodes.read().await.get(&vnode_id).cloned();
        if let Some(vnode) = vnode_opt {
            vnode.flush(true, true, trigger_compact).await?;
        }

        Ok(())
    }

    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
        vnode_id: VnodeId,
        filter: &ColumnDomains<String>,
    ) -> TskvResult<Vec<SeriesId>> {
        let (schema, ts_index) = match self.ctx.version_set.read().await.get_db(tenant, database) {
            Some(db) => {
                let db = db.read().await;
                let schema = match db.get_table_schema(tab).await? {
                    None => return Ok(vec![]),
                    Some(schema) => schema,
                };
                let ts_index = match db.get_ts_index(vnode_id) {
                    Some(ts_index) => ts_index,
                    None => return Ok(vec![]),
                };
                (schema, ts_index)
            }
            None => return Ok(vec![]),
        };

        let res = ts_index
            .read()
            .await
            .get_series_ids_by_domains(&schema, filter)
            .await?;

        Ok(res)
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        _table: &str,
        vnode_id: VnodeId,
        series_id: &[SeriesId],
    ) -> TskvResult<Vec<SeriesKey>> {
        if let Some(db) = self.ctx.version_set.read().await.get_db(tenant, database) {
            Ok(db
                .read()
                .await
                .get_series_key(vnode_id, series_id)
                .await
                .context(IndexErrSnafu)?)
        } else {
            Ok(vec![])
        }
    }

    async fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<Option<Arc<SuperVersion>>> {
        let version_set = self.ctx.version_set.read().await;
        // Comment it, It's not a error, Maybe the data not right!
        // if !version_set.db_exists(tenant, database) {
        //     return Err(SchemaError::DatabaseNotFound {
        //         database: database.to_string(),
        //     }
        //     .into());
        // }
        if let Some(tsf) = version_set
            .get_tsfamily_by_name_id(tenant, database, vnode_id)
            .await
        {
            Ok(Some(tsf.read().await.super_version()))
        } else {
            debug!(
                "ts_family {} with db name '{}' not found.",
                vnode_id, database
            );
            Ok(None)
        }
    }

    fn get_storage_options(&self) -> Arc<StorageOptions> {
        self.ctx.options.storage.clone()
    }

    async fn compact(&self, vnode_ids: Vec<VnodeId>) -> TskvResult<()> {
        for vnode_id in vnode_ids {
            if let Some(ts_family) = self
                .ctx
                .version_set
                .read()
                .await
                .get_tsfamily_by_tf_id(vnode_id)
                .await
            {
                // TODO: stop current and prevent next flush and compaction.
                if !ts_family.read().await.can_compaction() {
                    warn!("forbidden compaction on moving vnode {}", vnode_id);
                    return Ok(());
                }

                let owner = ts_family.read().await.owner();
                let (tenant, db_name) = split_owner(&owner);

                if let Err(e) = self.flush_tsfamily(tenant, db_name, vnode_id, true).await {
                    error!("Failed to flush vnode {}: {:?}", vnode_id, e);
                }

                let version = ts_family.read().await.version();
                if let Some(req) = pick_compaction(CompactTask::Manual(vnode_id), version).await {
                    let vnode_compaction_metrics = VnodeCompactionMetrics::new(
                        &self.metrics,
                        self.ctx.options.storage.node_id,
                        vnode_id,
                        CompactionType::Manual,
                        self.ctx.options.storage.collect_compaction_metrics,
                    );
                    match compaction::run_compaction_job(
                        req,
                        self.ctx.global_ctx.clone(),
                        vnode_compaction_metrics,
                    )
                    .await
                    {
                        Ok(Some((version_edit, file_metas))) => {
                            let (summary_tx, _summary_rx) = oneshot::channel();
                            let _ = self
                                .ctx
                                .summary_task_sender
                                .send(SummaryTask::new(
                                    ts_family.clone(),
                                    version_edit,
                                    Some(file_metas),
                                    None,
                                    summary_tx,
                                ))
                                .await;

                            // let _ = summary_rx.await;
                        }
                        Ok(None) => {
                            info!("There is nothing to compact.");
                        }
                        Err(e) => {
                            error!("Compaction job failed: {:?}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_vnode_hash_tree(&self, vnode_id: VnodeId) -> TskvResult<RecordBatch> {
        for database in self.ctx.version_set.read().await.get_all_db().values() {
            let db = database.read().await;
            if let Some(ts_family) = db.ts_families().get(&vnode_id).cloned() {
                drop(db);

                let owner = ts_family.read().await.owner();
                let (tenant, db_name) = split_owner(&owner);
                self.flush_tsfamily(tenant, db_name, vnode_id, false)
                    .await?;

                return check::vnode_checksum(ts_family).await;
            }
        }

        Ok(RecordBatch::new_empty(check::vnode_table_checksum_schema()))
    }

    async fn close(&self) {
        let (tx, mut rx) = mpsc::channel(1);
        if let Err(e) = self.close_sender.send(tx) {
            error!("Failed to broadcast close signal: {:?}", e);
        }
        while let Some(_x) = rx.recv().await {
            continue;
        }
        self.sync_indexs()
            .await
            .expect("Tskv Index haven't been sync.");

        info!("TsKv closed");
    }
}

impl std::fmt::Debug for TsKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tskv engine type")
    }
}
