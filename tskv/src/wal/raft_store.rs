use std::sync::Arc;

use openraft::EntryPayload;
use replication::errors::{ReplicationError, ReplicationResult};
use replication::{ApplyStorageRef, EntryStorage, RaftNodeId, RaftNodeInfo, TypeConfig};
use tokio::sync::Mutex;

use super::reader::WalRecordData;
use crate::file_system::file_manager;
use crate::wal::reader::{Block, WalReader};
use crate::wal::{writer, VnodeWal};
use crate::{file_utils, Error, Result};

pub type RaftEntry = openraft::Entry<TypeConfig>;
pub type RaftLogMembership = openraft::Membership<RaftNodeId, RaftNodeInfo>;
pub type RaftRequestForWalWrite = writer::Task;

pub struct RaftEntryStorage {
    inner: Arc<Mutex<RaftEntryStorageInner>>,
}

impl RaftEntryStorage {
    pub fn new(wal: VnodeWal) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RaftEntryStorageInner {
                wal,
                files_meta: vec![],
                entry_cache: cache::CircularKVCache::new(256),
            })),
        }
    }

    /// Read WAL files to recover
    pub async fn recover(&self, engine: ApplyStorageRef) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.recover(engine).await
    }
}

#[async_trait::async_trait]
impl EntryStorage for RaftEntryStorage {
    async fn append(&self, entries: &[RaftEntry]) -> ReplicationResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock().await;
        for ent in entries {
            let (wal_id, pos) = inner
                .wal
                .write_raft_entry(ent)
                .await
                .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?;
            //inner.wal.sync().await.unwrap();

            inner.mark_write_wal(ent.clone(), wal_id, pos);
        }
        Ok(())
    }

    async fn del_before(&self, seq_no: u64) -> ReplicationResult<()> {
        let mut inner = self.inner.lock().await;
        inner.mark_delete_before(seq_no);

        let _ = inner.wal.delete_wal_before_seq(seq_no).await;

        Ok(())
    }

    async fn del_after(&self, seq_no: u64) -> ReplicationResult<()> {
        let mut inner = self.inner.lock().await;
        inner.mark_delete_after(seq_no);

        // TODO delete data in file

        Ok(())
    }

    async fn entry(&self, seq_no: u64) -> ReplicationResult<Option<RaftEntry>> {
        let mut inner = self.inner.lock().await;

        inner.read_raft_entry(seq_no).await
    }

    async fn last_entry(&self) -> ReplicationResult<Option<RaftEntry>> {
        let mut inner = self.inner.lock().await;
        inner.wal_last_entry().await
    }

    async fn entries(&self, begin: u64, end: u64) -> ReplicationResult<Vec<RaftEntry>> {
        let mut inner = self.inner.lock().await;
        inner.read_raft_entry_range(begin, end).await
    }
}

struct WalFileMeta {
    file_id: u64,
    min_seq: u64,
    max_seq: u64,
    reader: WalReader,
    entry_index: Vec<(u64, u64)>, // seq -> pos
}

impl WalFileMeta {
    fn is_empty(&self) -> bool {
        self.min_seq == u64::MAX || self.max_seq == u64::MAX
    }

    fn intersection(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        if self.is_empty() {
            return None;
        }

        let start = self.min_seq.max(start);
        let end = (self.max_seq + 1).min(end); //[ ... )
        if start <= end {
            Some((start, end))
        } else {
            None
        }
    }

    fn mark_entry(&mut self, index: u64, pos: u64) {
        if self.min_seq == u64::MAX || self.min_seq > index {
            self.min_seq = index
        }

        if self.max_seq == u64::MAX || self.max_seq < index {
            self.max_seq = index
        }

        self.entry_index.push((index, pos));
    }

    fn del_befor(&mut self, index: u64) {
        if self.min_seq == u64::MAX || self.min_seq >= index {
            return;
        }

        self.min_seq = index;
        let idx = match self.entry_index.binary_search_by(|v| v.0.cmp(&index)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        self.entry_index.drain(0..idx);
    }

    fn del_after(&mut self, index: u64) {
        if self.max_seq == u64::MAX || self.max_seq < index {
            return;
        }

        if index == 0 {
            self.min_seq = u64::MAX;
            self.max_seq = u64::MAX;
            self.entry_index.clear();
            return;
        }

        self.max_seq = index - 1;
        let idx = match self.entry_index.binary_search_by(|v| v.0.cmp(&index)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.entry_index.drain(idx..);
    }

    async fn get_entry_by_index(&mut self, index: u64) -> ReplicationResult<Option<RaftEntry>> {
        if let Ok(idx) = self.entry_index.binary_search_by(|v| v.0.cmp(&index)) {
            let pos = self.entry_index[idx].1;
            self.get_entry(pos).await
        } else {
            Ok(None)
        }
    }

    async fn get_entry(&mut self, pos: u64) -> ReplicationResult<Option<RaftEntry>> {
        if let Some(record) = self
            .reader
            .read_wal_record_data(pos)
            .await
            .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?
        {
            if let Block::RaftLog(entry) = record.block {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}
struct RaftEntryStorageInner {
    wal: VnodeWal,
    files_meta: Vec<WalFileMeta>,
    entry_cache: cache::CircularKVCache<u64, RaftEntry>,
}

impl RaftEntryStorageInner {
    fn mark_write_wal(&mut self, entry: RaftEntry, wal_id: u64, pos: u64) {
        let index = entry.log_id.index;
        if let Some(item) = self
            .files_meta
            .iter_mut()
            .rev()
            .find(|item| item.file_id == wal_id)
        {
            item.mark_entry(index, pos);
        } else {
            let mut item = WalFileMeta {
                file_id: wal_id,
                min_seq: u64::MAX,
                max_seq: u64::MAX,
                entry_index: vec![],
                reader: self.wal.current_wal.new_reader(),
            };

            item.entry_index.reserve(8 * 1024);
            item.mark_entry(index, pos);
            self.files_meta.push(item);
        }

        self.entry_cache.put(index, entry);
    }

    fn mark_delete_before(&mut self, seq_no: u64) {
        if self.min_sequence() >= seq_no {
            return;
        }

        for item in self.files_meta.iter_mut() {
            if item.min_seq < seq_no {
                item.del_befor(seq_no);
            } else {
                break;
            }
        }

        self.entry_cache.del_before(seq_no);
    }

    fn mark_delete_after(&mut self, seq_no: u64) {
        if self.max_sequence() < seq_no {
            return;
        }

        for item in self.files_meta.iter_mut().rev() {
            if item.max_seq >= seq_no {
                item.del_after(seq_no);
            } else {
                break;
            }
        }

        self.entry_cache.del_after(seq_no);
    }

    fn entries_from_cache(&self, start: u64, end: u64) -> Option<Vec<RaftEntry>> {
        let mut entries = vec![];
        for index in start..end {
            if let Some(entry) = self.entry_cache.get(&index) {
                entries.push(entry.clone());
            } else {
                return None;
            }
        }

        Some(entries)
    }

    fn is_empty(&self) -> bool {
        if self.min_sequence() == u64::MAX || self.max_sequence() == u64::MAX {
            return true;
        }

        false
    }

    fn min_sequence(&self) -> u64 {
        if let Some(item) = self.files_meta.first() {
            return item.min_seq;
        }

        u64::MAX
    }

    fn max_sequence(&self) -> u64 {
        if let Some(item) = self.files_meta.last() {
            return item.max_seq;
        }

        u64::MAX
    }

    async fn wal_last_entry(&mut self) -> ReplicationResult<Option<RaftEntry>> {
        if let Some(entry) = self.entry_cache.last() {
            Ok(Some(entry.clone()))
        } else {
            Ok(None)
        }
    }

    async fn read_raft_entry_range(
        &mut self,
        start: u64,
        end: u64,
    ) -> ReplicationResult<Vec<RaftEntry>> {
        if let Some(entries) = self.entries_from_cache(start, end) {
            return Ok(entries);
        }

        let mut list = vec![];
        for item in self.files_meta.iter_mut() {
            if let Some((start, end)) = item.intersection(start, end) {
                for index in start..end {
                    if let Some(entry) = item.get_entry_by_index(index).await? {
                        list.push(entry);
                    }
                }
            }
        }

        Ok(list)
    }

    async fn read_raft_entry(&mut self, index: u64) -> ReplicationResult<Option<RaftEntry>> {
        if let Some(entry) = self.entry_cache.get(&index) {
            return Ok(Some(entry.clone()));
        }

        let location = match self
            .files_meta
            .iter_mut()
            .rev()
            .find(|item| (index >= item.min_seq) && (index <= item.max_seq))
        {
            Some(item) => item,
            None => return Ok(None),
        };

        location.get_entry_by_index(index).await
    }

    /// Read WAL files to recover: engine, index, cache.
    pub async fn recover(&mut self, engine: ApplyStorageRef) -> Result<()> {
        let wal_files = file_manager::list_file_names(self.wal.wal_dir());
        for file_name in wal_files {
            // If file name cannot be parsed to wal id, skip that file.
            let wal_id = match file_utils::get_wal_file_id(&file_name) {
                Ok(id) => id,
                Err(_) => continue,
            };
            let path = self.wal.wal_dir().join(&file_name);
            if !file_manager::try_exists(&path) {
                continue;
            }
            let reader = self.wal.wal_reader(wal_id).await?;
            let mut record_reader = reader.take_record_reader();
            loop {
                let record = record_reader.read_record().await;
                match record {
                    Ok(r) => {
                        if r.data.len() < 9 {
                            continue;
                        }

                        let wal_entry = WalRecordData::new(r.data);
                        if let Block::RaftLog(entry) = wal_entry.block {
                            if let EntryPayload::Normal(ref req) = entry.payload {
                                let ctx = replication::ApplyContext {
                                    index: entry.log_id.index,
                                    raft_id: self.wal.vnode_id as u64,
                                    apply_type: replication::APPLY_TYPE_WAL,
                                };
                                engine.apply(&ctx, req).await.unwrap();
                            }

                            self.mark_write_wal(entry, wal_id, r.pos);
                        }
                    }
                    Err(Error::Eof) => {
                        break;
                    }
                    Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                    Err(e) => {
                        trace::error!("Error reading wal: {:?}", e);
                        return Err(Error::WalTruncated);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicUsize;
    use std::sync::{atomic, Arc};

    use models::schema::make_owner;
    use replication::apply_store::HeedApplyStorage;
    use replication::node_store::NodeStorage;
    use replication::state_store::StateStorage;
    use replication::{ApplyStorageRef, EntryStorageRef, RaftNodeInfo};

    use crate::wal::raft_store::RaftEntryStorage;
    use crate::wal::VnodeWal;
    use crate::Result;

    pub async fn get_vnode_wal(dir: impl AsRef<Path>) -> Result<VnodeWal> {
        let dir = dir.as_ref();
        let owner = make_owner("cnosdb", "test_db");
        let owner = Arc::new(owner);
        let wal_option = crate::kv_option::WalOptions {
            enabled: true,
            path: dir.to_path_buf(),
            wal_req_channel_cap: 1024,
            max_file_size: 1024 * 1024,
            flush_trigger_total_file_size: 128,
            sync: false,
            sync_interval: std::time::Duration::from_secs(3600),
        };

        VnodeWal::new(Arc::new(wal_option), owner, 1234).await
    }

    pub async fn get_node_store(dir: impl AsRef<Path>) -> Arc<NodeStorage> {
        trace::debug!("----------------------------------------");
        let dir = dir.as_ref();
        let wal = get_vnode_wal(dir).await.unwrap();
        let entry = RaftEntryStorage::new(wal);
        let entry: EntryStorageRef = Arc::new(entry);

        let state = StateStorage::open(dir.join("state")).unwrap();
        let engine = HeedApplyStorage::open(dir.join("engine")).unwrap();

        let state = Arc::new(state);
        let engine: ApplyStorageRef = Arc::new(engine);

        let info = RaftNodeInfo {
            group_id: 2222,
            address: "127.0.0.1:12345".to_string(),
        };

        let storage = NodeStorage::open(1000, info, state, engine, entry).unwrap();

        Arc::new(storage)
    }

    #[test]
    fn test_wal_raft_storage_with_openraft_cases() {
        let dir = PathBuf::from("/tmp/test/wal/raft/1");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        trace::init_default_global_tracing(
            &dir,
            "test_wal_raft_storage_with_openraft_cases",
            "debug",
        );

        let case_id = AtomicUsize::new(0);
        if let Err(e) = openraft::testing::Suite::test_all(|| {
            let id = case_id.fetch_add(1, atomic::Ordering::Relaxed);
            get_node_store(dir.join(id.to_string()))
        }) {
            trace::error!("{e}");
            panic!("{e:?}");
        }
    }
}
