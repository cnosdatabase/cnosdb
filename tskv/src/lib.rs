#![recursion_limit = "256"]

use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
pub use compaction::check::vnode_table_checksum_schema;
use compaction::CompactTask;
use datafusion::arrow::record_batch::RecordBatch;
use memory_pool::MemoryPool;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::{NodeId, VnodeId};
use models::predicate::domain::ColumnDomains;
use models::{SeriesId, SeriesKey};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tsfamily::version::{Version, VersionEdit};
use vnode_store::VnodeStorage;

pub use crate::error::{TskvError, TskvResult};
pub use crate::kv_option::Options;
use crate::kv_option::StorageOptions;
pub use crate::kvcore::TsKv;
// todo: add a method for print tsm statistics
// pub use crate::tsm::print_tsm_statistics;
pub use crate::tsfamily::summary::print_summary_statistics;
use crate::tsfamily::super_version::SuperVersion;
pub use crate::wal::print_wal_statistics;

pub mod byte_utils;
mod compaction;
mod compute;
pub mod database;
pub mod error;
pub mod file_system;
pub mod file_utils;
pub mod index;
pub mod kv_option;
mod kvcore;
// TODO supposedly private
mod mem_cache;
pub mod reader;
mod record_file;
mod schema;
mod tsfamily;
pub mod tsm;
mod version_set;
pub mod vnode_store;
pub mod wal;

/// The column file ID is unique in a KV instance
/// and uniquely corresponds to one column file.
pub type ColumnFileId = u64;
type LevelId = u32;

pub type EngineRef = Arc<dyn Engine>;

#[derive(PartialEq, Eq, Hash)]
pub struct UpdateSetValue<K, V> {
    pub key: K,
    pub value: Option<V>,
}

#[async_trait]
pub trait Engine: Send + Sync + Debug {
    /// open a tsfamily, if already exist just return.
    async fn open_tsfamily(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<VnodeStorage>;

    /// Remove the storage unit(caches and files) managed by engine,
    /// then remove directory of the storage unit.
    async fn remove_tsfamily(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<()>;

    /// Flush all caches of the storage unit into a file.
    async fn flush_tsfamily(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
        trigger_compact: bool,
    ) -> TskvResult<()>;

    /// Read index of a storage unit, find series ids that matches the filter.
    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        vnode_id: VnodeId,
        filter: &ColumnDomains<String>,
    ) -> TskvResult<Vec<SeriesId>>;

    /// Read index of a storage unit, get `SeriesKey` of the geiven series id.
    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        vnode_id: VnodeId,
        series_id: &[SeriesId],
    ) -> TskvResult<Vec<SeriesKey>>;

    /// Get a `SuperVersion` that contains the latest version of caches and files
    /// of the storage unit.
    async fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
    ) -> TskvResult<Option<Arc<SuperVersion>>>;

    /// Get the storage options which was used to install the engine.
    fn get_storage_options(&self) -> Arc<StorageOptions>;

    /// For the specified storage units, flush all caches into files, then compact
    /// files into larger files.
    async fn compact(&self, vnode_ids: Vec<VnodeId>) -> TskvResult<()>;

    /// Get a compressed hash_tree(ID and checksum of each vnode) of engine.
    async fn get_vnode_hash_tree(&self, vnode_id: VnodeId) -> TskvResult<RecordBatch>;

    /// Close all background jobs of engine.
    async fn close(&self);
}

#[derive(Clone)]
pub struct TsKvContext {
    pub meta: MetaRef,
    pub runtime: Arc<Runtime>,
    pub options: Arc<Options>,
    pub metrics: Arc<MetricsRegister>,
    pub memory_pool: Arc<dyn MemoryPool>,
    pub compact_task_sender: Sender<CompactTask>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VnodeSnapshot {
    pub node_id: NodeId,
    pub vnode_id: VnodeId,
    pub last_seq_no: u64,
    pub create_time: String,
    pub version_edit: VersionEdit,

    //filed version using Option just for compat Serialize, Deserialize
    #[serde(skip_serializing, skip_deserializing)]
    pub version: Option<Arc<Version>>,
    #[serde(skip_serializing, skip_deserializing)]
    pub active_time: i64, //active timestamp
}

impl Display for VnodeSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "node id: {}, vnode id: {}, last seq no: {}, create time: {}, active time: {}, ve: {:?}",    
         self.node_id, self.vnode_id, self.last_seq_no, self.create_time, self.active_time, self.version_edit)
    }
}

pub mod test {
    pub use crate::mem_cache::memcache::test::{get_one_series_cache_data, put_rows_to_cache};
}
