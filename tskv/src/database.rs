use std::{
    collections::HashMap,
    path,
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use crate::error::{self, Result};
use parking_lot::RwLock;
use protos::models::{Point, Points};
use snafu::ResultExt;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use trace::error;

use ::models::{FieldInfo, InMemPoint, SeriesInfo, Tag, ValueType};

use crate::tseries_family::LevelInfo;
use crate::{
    index::db_index,
    kv_option::{TseriesFamDesc, TseriesFamOpt},
    memcache::MemCache,
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::{TseriesFamily, Version},
};

#[derive(Debug)]
pub struct Database {
    name: String,
    index: Arc<RwLock<db_index::DBIndex>>,
    ts_families: HashMap<u32, Arc<RwLock<TseriesFamily>>>,
}

impl Database {
    pub fn new(name: &String, path: &String) -> Self {
        Self {
            index: db_index::index_manger(path).write().get_db_index(&name),
            name: name.to_string(),
            ts_families: HashMap::new(),
        }
    }

    pub fn open_tsfamily(&mut self, ver: Arc<Version>) {
        let opt = ver.ts_family_opt();

        let tf = TseriesFamily::new(
            ver.tf_id(),
            ver.tf_name().to_string(),
            MemCache::new(ver.tf_id(), opt.max_memcache_size, ver.last_seq, false),
            ver.clone(),
            opt,
        );
        self.ts_families
            .insert(ver.tf_id(), Arc::new(RwLock::new(tf)));
    }

    pub async fn switch_memcache(&self, tf_id: u32, seq: u64) {
        if let Some(tf) = self.ts_families.get(&tf_id) {
            let mut tf = tf.write();
            let mem = Arc::new(RwLock::new(MemCache::new(
                tf_id,
                tf.options().max_memcache_size,
                seq,
                false,
            )));
            tf.switch_memcache(mem).await;
        }
    }

    // todo: Maybe TseriesFamily::new() should be refactored.
    #[allow(clippy::too_many_arguments)]
    pub fn add_tsfamily(
        &mut self,
        tsf_id: u32,
        seq_no: u64,
        file_id: u64,
        opt: Arc<TseriesFamOpt>,
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) -> Arc<RwLock<TseriesFamily>> {
        let ver = Arc::new(Version::new(
            tsf_id,
            self.name.clone(),
            opt.clone(),
            file_id,
            LevelInfo::init_levels(opt.clone()),
            i64::MIN,
        ));

        let tf = TseriesFamily::new(
            tsf_id,
            self.name.clone(),
            MemCache::new(tsf_id, opt.max_memcache_size, seq_no, false),
            ver,
            opt,
        );
        let tf = Arc::new(RwLock::new(tf));
        self.ts_families.insert(tsf_id, tf.clone());

        let mut edit = VersionEdit::new();
        edit.add_tsfamily(tsf_id, self.name.clone());

        let edits = vec![edit];
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask {
            edits,
            cb: task_state_sender,
        };
        if let Err(e) = summary_task_sender.send(task) {
            error!("failed to send Summary task, {:?}", e);
        }

        tf
    }

    pub fn del_tsfamily(&mut self, tf_id: u32, summary_task_sender: UnboundedSender<SummaryTask>) {
        self.ts_families.remove(&tf_id);

        let mut edits = vec![];
        let mut edit = VersionEdit::new();
        edit.del_tsfamily(tf_id);
        edits.push(edit);
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask {
            edits,
            cb: task_state_sender,
        };
        if let Err(e) = summary_task_sender.send(task) {
            error!("failed to send Summary task, {:?}", e);
        }
    }

    pub fn build_mem_points(
        &self,
        points: flatbuffers::Vector<flatbuffers::ForwardsUOffset<Point>>,
    ) -> Result<Vec<InMemPoint>> {
        let mut mem_points = Vec::<_>::with_capacity(points.len());

        // get or create forward index
        for point in points {
            let mut info =
                SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?;
            let sid = self
                .index
                .write()
                .add_series_if_not_exists(&mut info)
                .context(error::IndexErrSnafu)?;

            let mut point = InMemPoint::from(point);
            point.series_id = sid;
            let fields = info.field_infos();

            for i in 0..fields.len() {
                point.fields[i].field_id = fields[i].field_id();
            }

            mem_points.push(point);
        }

        return Ok(mem_points);
    }

    pub fn version_edit(&self, last_seq: u64) -> (Vec<VersionEdit>, Vec<VersionEdit>) {
        let mut edits = vec![];
        let mut files = vec![];

        for (id, ts) in &self.ts_families {
            //tsfamily edit
            let mut edit = VersionEdit::new();
            edit.add_tsfamily(*id, self.name.clone());
            edits.push(edit);

            // file edit
            let mut edit = VersionEdit::new();
            let version = ts.read().version().clone();
            let max_level_ts = version.max_level_ts;
            for files in version.levels_info.iter() {
                for file in files.files.iter() {
                    let mut meta = CompactMeta::from(file.as_ref());
                    meta.tsf_id = files.tsf_id;
                    meta.high_seq = last_seq;
                    edit.add_file(meta, max_level_ts);
                }
            }
            files.push(edit);
        }

        (edits, files)
    }

    pub fn get_tsfamily(&self, id: u32) -> Option<&Arc<RwLock<TseriesFamily>>> {
        self.ts_families.get(&id)
    }

    pub fn tsf_num(&self) -> usize {
        self.ts_families.len()
    }

    pub fn ts_families(&self) -> &HashMap<u32, Arc<RwLock<TseriesFamily>>> {
        &self.ts_families
    }

    pub fn get_index(&self) -> &Arc<RwLock<db_index::DBIndex>> {
        return &self.index;
    }

    // todo: will delete in cluster version
    pub fn get_tsfamily_random(&self) -> Option<Arc<RwLock<TseriesFamily>>> {
        for (_, v) in &self.ts_families {
            return Some(v.clone());
        }

        None
    }
}
