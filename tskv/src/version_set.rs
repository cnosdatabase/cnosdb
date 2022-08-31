use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use parking_lot::RwLock;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use trace::error;

use crate::{
    database::Database,
    error::Result,
    index::db_index,
    kv_option::StorageOptions,
    memcache::MemCache,
    summary::{SummaryTask, VersionEdit},
    tseries_family::{LevelInfo, TseriesFamily, Version},
    Options,
};

#[derive(Debug)]
pub struct VersionSet {
    opt: Arc<Options>,

    dbs: HashMap<String, Arc<RwLock<Database>>>,
}

impl VersionSet {
    pub fn new(opt: Arc<Options>, ver_set: HashMap<u32, Arc<Version>>) -> Self {
        let mut dbs = HashMap::new();

        for (id, ver) in ver_set {
            let name = ver.database().to_string();
            let seq = ver.last_seq;

            let db = dbs
                .entry(name.clone())
                .or_insert_with(|| Arc::new(RwLock::new(Database::new(&name, opt.clone()))));

            db.write().open_tsfamily(ver);
        }

        Self { dbs, opt }
    }

    pub fn create_db(&mut self, name: &String) -> Arc<RwLock<Database>> {
        self.dbs
            .entry(name.clone())
            .or_insert_with(|| Arc::new(RwLock::new(Database::new(name, self.opt.clone()))))
            .clone()
    }

    pub fn get_all_db(&self) -> &HashMap<String, Arc<RwLock<Database>>> {
        return &self.dbs;
    }

    pub fn get_db(&self, name: &String) -> Option<Arc<RwLock<Database>>> {
        if let Some(v) = self.dbs.get(name) {
            return Some(v.clone());
        }

        None
    }

    pub fn tsf_num(&self) -> usize {
        let mut size = 0;
        for (_, db) in &self.dbs {
            size += db.read().tsf_num();
        }

        return size;
    }

    pub fn get_tsfamily_by_tf_id(&self, tf_id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        for (_, db) in &self.dbs {
            if let Some(v) = db.read().get_tsfamily(tf_id) {
                return Some(v.clone());
            }
        }

        None
    }

    // will delete in cluster version
    pub fn get_tsfamily_by_name(&self, name: &String) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(db) = self.dbs.get(name) {
            return db.read().get_tsfamily_random();
        }

        None
    }
}
