use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cache::{AsyncCache, ShardedAsyncCache};
use models::predicate::domain::TimeRange;
use models::{SeriesId, Timestamp};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::sync::RwLock;
use trace::error;
use utils::BloomFilter;

use crate::error::{RecordFileDecodeSnafu, RecordFileEncodeSnafu, TskvResult};
use crate::kv_option::{StorageOptions, DELTA_PATH, TSM_PATH};
use crate::tsfamily::column_file::ColumnFile;
use crate::tsfamily::level_info::LevelInfo;
use crate::tsm::page::PageMeta;
use crate::tsm::reader::TsmReader;
use crate::tsm::ColumnGroupID;
use crate::{byte_utils, file_utils, ColumnFileId, LevelId, VnodeId};

#[derive(Debug)]
pub struct Version {
    ts_family_id: VnodeId,
    owner: Arc<String>,
    storage_opt: Arc<StorageOptions>,
    /// The max seq_no of write batch in wal flushed to column file.
    last_seq: u64,
    /// The max timestamp of write batch in wal flushed to column file.
    max_level_ts: i64,
    levels_info: [LevelInfo; 5],
    tsm_reader_cache: Arc<ShardedAsyncCache<String, Arc<TsmReader>>>,
}

impl Version {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts_family_id: VnodeId,
        owner: Arc<String>,
        storage_opt: Arc<StorageOptions>,
        last_seq: u64,
        levels_info: [LevelInfo; 5],
        max_level_ts: i64,
        tsm_reader_cache: Arc<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            ts_family_id,
            owner,
            storage_opt,
            last_seq,
            max_level_ts,
            levels_info,
            tsm_reader_cache,
        }
    }

    /// Creates new Version using current Version and `VersionEdit`s.
    pub fn copy_apply_version_edits(
        &self,
        ve: VersionEdit,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) -> Version {
        let mut added_files: Vec<Vec<CompactMeta>> = vec![vec![]; 5];
        let mut deleted_files: Vec<HashSet<ColumnFileId>> = vec![HashSet::new(); 5];
        if !ve.add_files.is_empty() {
            ve.add_files.into_iter().for_each(|f| {
                added_files[f.level as usize].push(f);
            });
        }
        if !ve.del_files.is_empty() {
            ve.del_files.into_iter().for_each(|f| {
                deleted_files[f.level as usize].insert(f.file_id);
            });
        }

        let mut new_levels = LevelInfo::init_levels(
            self.owner.clone(),
            self.ts_family_id,
            self.storage_opt.clone(),
        );
        let weak_tsm_reader_cache = Arc::downgrade(&self.tsm_reader_cache);
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if deleted_files[file.level() as usize].contains(&file.file_id()) {
                    file.mark_deleted();
                    continue;
                }
                new_levels[level.level as usize].push_column_file(file.clone());
            }
            for file in added_files[level.level as usize].iter() {
                let series_filter = file_metas.remove(&file.file_id).unwrap_or_else(|| {
                    error!("missing bloom filter for file_id: {}", file.file_id);
                    Arc::new(BloomFilter::default())
                });
                new_levels[level.level as usize].push_compact_meta(
                    file,
                    RwLock::new(Some(series_filter)),
                    weak_tsm_reader_cache.clone(),
                );
            }
            new_levels[level.level as usize].update_time_range();
        }

        let mut new_version = Self {
            last_seq: max(self.last_seq, ve.seq_no),
            ts_family_id: self.ts_family_id,
            owner: self.owner.clone(),
            storage_opt: self.storage_opt.clone(),
            max_level_ts: self.max_level_ts,
            levels_info: new_levels,
            tsm_reader_cache: self.tsm_reader_cache.clone(),
        };
        new_version.update_max_level_ts();
        new_version
    }

    fn update_max_level_ts(&mut self) {
        if self.levels_info.is_empty() {
            return;
        }
        let mut max_ts = Timestamp::MIN;
        for level in self.levels_info.iter() {
            if level.files.is_empty() {
                continue;
            }
            for file in level.files.iter() {
                max_ts = file.time_range().max_ts.max(max_ts);
            }
        }

        self.max_level_ts = max_ts;
    }

    pub fn tf_id(&self) -> VnodeId {
        self.ts_family_id
    }

    pub fn owner(&self) -> Arc<String> {
        self.owner.clone()
    }

    pub fn levels_info(&self) -> &[LevelInfo; 5] {
        &self.levels_info
    }

    pub fn storage_opt(&self) -> Arc<StorageOptions> {
        self.storage_opt.clone()
    }

    // todo:
    pub fn get_ts_overlap(&self, _level: u32, _ts_min: i64, _ts_max: i64) -> Vec<Arc<ColumnFile>> {
        vec![]
    }

    pub async fn get_tsm_reader(&self, path: impl AsRef<Path>) -> TskvResult<Arc<TsmReader>> {
        let path = path.as_ref().display().to_string();
        let tsm_reader = match self.tsm_reader_cache.get(&path).await {
            Some(val) => val,
            None => match self.tsm_reader_cache.get(&path).await {
                Some(val) => val,
                None => {
                    let tsm_reader = Arc::new(TsmReader::open(&path).await?);
                    self.tsm_reader_cache.insert(path, tsm_reader.clone()).await;
                    tsm_reader
                }
            },
        };
        Ok(tsm_reader)
    }

    pub fn max_level_ts(&self) -> i64 {
        self.max_level_ts
    }

    pub fn tsm_reader_cache(&self) -> &Arc<ShardedAsyncCache<String, Arc<TsmReader>>> {
        &self.tsm_reader_cache
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_predicate: TimeRange,
    ) -> BTreeMap<ColumnFileId, BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>> {
        let mut result = BTreeMap::new();
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if file.is_deleted() || !file.overlap(&time_predicate) {
                    continue;
                }
                let reader = self.get_tsm_reader(file.file_path()).await.unwrap();
                let fid = reader.file_id();
                let sts = reader.statistics(series_ids, time_predicate).await.unwrap();
                result.insert(fid, sts);
            }
        }
        result
    }

    pub async fn remove_tsm_reader_cache(&self, path: impl AsRef<Path>) {
        let path = path.as_ref().display().to_string();
        self.tsm_reader_cache.remove(&path).await;
    }

    pub async fn unmark_compacting_files(&self, files_ids: &HashSet<ColumnFileId>) {
        if files_ids.is_empty() {
            return;
        }
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if files_ids.contains(&file.file_id()) {
                    let mut compacting = file.write_lock_compacting().await;
                    *compacting = false;
                }
            }
        }
    }

    #[cfg(test)]
    pub fn levels_info_mut(&mut self) -> &mut [LevelInfo; 5] {
        &mut self.levels_info
    }

    #[cfg(test)]
    pub fn inner(&self) -> Self {
        Self {
            ts_family_id: self.ts_family_id,
            owner: self.owner.clone(),
            storage_opt: self.storage_opt.clone(),
            last_seq: self.last_seq,
            max_level_ts: self.max_level_ts,
            levels_info: self.levels_info.clone(),
            tsm_reader_cache: self.tsm_reader_cache.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CompactMeta {
    pub file_id: ColumnFileId,
    pub file_size: u64,
    pub tsf_id: VnodeId,
    pub level: LevelId,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub is_delta: bool,
}

impl Default for CompactMeta {
    fn default() -> Self {
        Self {
            file_id: 0,
            file_size: 0,
            tsf_id: 0,
            level: 0,
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
            is_delta: false,
        }
    }
}

/// There are serial fields set with default value:
/// - tsf_id
/// - high_seq
/// - low_seq
impl From<&ColumnFile> for CompactMeta {
    fn from(file: &ColumnFile) -> Self {
        Self {
            file_id: file.file_id(),
            file_size: file.size(),
            level: file.level(),
            min_ts: file.time_range().min_ts,
            max_ts: file.time_range().max_ts,
            is_delta: file.is_delta(),
            ..Default::default()
        }
    }
}

impl CompactMeta {
    pub fn new(
        tsf_id: VnodeId,
        file_id: u64,
        file_size: u64,
        level: LevelId,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> CompactMeta {
        CompactMeta {
            file_id,
            file_size,
            tsf_id,
            level,
            min_ts,
            max_ts,
            is_delta: level == 0,
        }
    }

    pub fn new_del_file_part(
        level: LevelId,
        file_id: ColumnFileId,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> Self {
        CompactMeta {
            file_id,
            level,
            is_delta: level == 0,
            min_ts,
            max_ts,
            ..Default::default()
        }
    }

    pub fn file_path(
        &self,
        storage_opt: &StorageOptions,
        owner: &str,
        ts_family_id: VnodeId,
    ) -> PathBuf {
        if self.is_delta {
            let base_dir = storage_opt.delta_dir(owner, ts_family_id);
            file_utils::make_delta_file(base_dir, self.file_id)
        } else {
            let base_dir = storage_opt.tsm_dir(owner, ts_family_id);
            file_utils::make_tsm_file(base_dir, self.file_id)
        }
    }

    pub fn relative_path(&self) -> PathBuf {
        if self.is_delta {
            let base_dir = PathBuf::from(DELTA_PATH);
            file_utils::make_delta_file(base_dir, self.file_id)
        } else {
            let base_dir = PathBuf::from(TSM_PATH);
            file_utils::make_tsm_file(base_dir, self.file_id)
        }
    }

    pub async fn rename_file(
        &self,
        old_dir: &Path,
        new_dir: &Path,
        new_id: ColumnFileId,
    ) -> TskvResult<PathBuf> {
        let old_id = self.file_id;
        let (old_name, new_name) = if self.is_delta {
            (
                file_utils::make_delta_file(old_dir.join(DELTA_PATH), old_id),
                file_utils::make_delta_file(new_dir.join(DELTA_PATH), new_id),
            )
        } else {
            (
                file_utils::make_tsm_file(old_dir.join(TSM_PATH), old_id),
                file_utils::make_tsm_file(new_dir.join(TSM_PATH), new_id),
            )
        };

        trace::info!("rename file from {:?} to {:?}", &old_name, &new_name);
        file_utils::rename(&old_name, &new_name).await?;

        Ok(new_name)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct VersionEdit {
    pub seq_no: u64,
    pub file_id: ColumnFileId,
    pub max_level_ts: Timestamp,
    pub add_files: Vec<CompactMeta>,
    pub del_files: Vec<CompactMeta>,

    #[serde(skip)]
    pub partly_del_files: Vec<CompactMeta>,

    pub act_tsf: VnodeAction,
    pub tsf_id: VnodeId,
    pub tsf_name: String,
}

impl Default for VersionEdit {
    fn default() -> Self {
        Self {
            seq_no: 0,
            file_id: 0,
            max_level_ts: i64::MIN,
            add_files: vec![],
            del_files: vec![],
            partly_del_files: vec![],
            act_tsf: VnodeAction::Update,
            tsf_id: 0,
            tsf_name: String::from(""),
        }
    }
}

impl VersionEdit {
    pub fn new(vnode_id: VnodeId) -> Self {
        Self {
            tsf_id: vnode_id,
            ..Default::default()
        }
    }

    pub fn new_update_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            seq_no,
            act_tsf: VnodeAction::Update,
            tsf_name: owner,
            tsf_id: vnode_id,
            ..Default::default()
        }
    }

    pub fn new_add_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            seq_no,
            act_tsf: VnodeAction::Add,
            tsf_id: vnode_id,
            tsf_name: owner,

            ..Default::default()
        }
    }

    pub fn new_del_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            seq_no,
            act_tsf: VnodeAction::Delete,
            tsf_id: vnode_id,
            tsf_name: owner,

            ..Default::default()
        }
    }

    pub fn update_vnode_id(&mut self, new_id: u32) {
        self.tsf_id = new_id;

        for f in self.add_files.iter_mut() {
            f.tsf_id = new_id;
        }

        for f in self.del_files.iter_mut() {
            f.tsf_id = new_id;
        }
    }

    pub fn encode(&self) -> TskvResult<Vec<u8>> {
        bincode::serialize(self).context(RecordFileEncodeSnafu)
    }

    pub fn decode(buf: &[u8]) -> TskvResult<Self> {
        bincode::deserialize(buf).context(RecordFileDecodeSnafu)
    }

    pub fn encode_vec(data: &[Self]) -> TskvResult<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::with_capacity(data.len() * 32);
        for ve in data {
            let ve_buf = bincode::serialize(ve).context(RecordFileEncodeSnafu)?;
            let pos = buf.len();
            buf.resize(pos + 4 + ve_buf.len(), 0_u8);
            buf[pos..pos + 4].copy_from_slice((ve_buf.len() as u32).to_be_bytes().as_slice());
            buf[pos + 4..].copy_from_slice(&ve_buf);
        }

        Ok(buf)
    }

    pub fn decode_vec(buf: &[u8]) -> TskvResult<Vec<Self>> {
        let mut list: Vec<Self> = Vec::with_capacity(buf.len() / 32 + 1);
        let mut pos = 0_usize;
        while pos < buf.len() {
            if buf.len() - pos < 4 {
                break;
            }
            let len = byte_utils::decode_be_u32(&buf[pos..pos + 4]);
            pos += 4;
            if buf.len() - pos < len as usize {
                break;
            }
            let ve = Self::decode(&buf[pos..pos + len as usize])?;
            pos += len as usize;
            list.push(ve);
        }

        Ok(list)
    }

    pub fn add_file(&mut self, compact_meta: CompactMeta, max_level_ts: i64) {
        self.file_id = self.file_id.max(compact_meta.file_id);
        self.max_level_ts = max(self.max_level_ts, max_level_ts);
        self.add_files.push(compact_meta);
    }

    pub fn del_file(&mut self, level: LevelId, file_id: ColumnFileId, is_delta: bool) {
        self.del_files.push(CompactMeta {
            file_id,
            level,
            is_delta,
            ..Default::default()
        });
    }

    pub fn del_file_part(
        &mut self,
        level: LevelId,
        file_id: ColumnFileId,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) {
        self.partly_del_files.push(CompactMeta::new_del_file_part(
            level, file_id, min_ts, max_ts,
        ));
    }
}

impl Display for VersionEdit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq_no: {}, file_id: {}, add_files: {}, del_files: {}, act_tsf: {}, tsf_id: {}, tsf_name: {}, max_level_ts: {}",
               self.seq_no, self.file_id, self.add_files.len(), self.del_files.len(), self.act_tsf, self.tsf_id, self.tsf_name, self.max_level_ts)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum VnodeAction {
    Add,
    Delete,
    Update,
}

impl Display for VnodeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VnodeAction::Add => write!(f, "Add"),
            VnodeAction::Delete => write!(f, "Delete"),
            VnodeAction::Update => write!(f, "Update"),
        }
    }
}
