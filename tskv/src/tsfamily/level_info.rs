use std::sync::{Arc, Weak};

use cache::ShardedAsyncCache;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::{SeriesId, Timestamp};
use tokio::sync::RwLock as TokioRwLock;
use utils::BloomFilter;

use super::version::CompactMeta;
use crate::error::TskvResult;
use crate::file_utils::{make_delta_file, make_tsm_file};
use crate::kv_option::StorageOptions;
use crate::tsfamily::column_file::ColumnFile;
use crate::tsm::reader::TsmReader;

#[derive(Debug, Clone)]
pub struct LevelInfo {
    /// the time_range of column file is overlap in L0,
    /// the time_range of column file is not overlap in L0,
    pub files: Vec<Arc<ColumnFile>>,
    pub owner: Arc<String>,
    pub tsf_id: u32,
    pub storage_opt: Arc<StorageOptions>,
    pub level: u32,
    pub cur_size: u64,
    pub max_size: u64,
    pub time_range: TimeRange,
}

impl LevelInfo {
    pub fn init(
        database: Arc<String>,
        level: u32,
        tsf_id: u32,
        storage_opt: Arc<StorageOptions>,
    ) -> Self {
        let max_size = storage_opt.level_max_file_size(level);
        Self {
            files: Vec::new(),
            owner: database,
            tsf_id,
            storage_opt,
            level,
            cur_size: 0,
            max_size,
            time_range: TimeRange {
                min_ts: Timestamp::MAX,
                max_ts: Timestamp::MIN,
            },
        }
    }

    pub fn init_levels(
        database: Arc<String>,
        tsf_id: u32,
        storage_opt: Arc<StorageOptions>,
    ) -> [LevelInfo; 5] {
        [
            Self::init(database.clone(), 0, tsf_id, storage_opt.clone()),
            Self::init(database.clone(), 1, tsf_id, storage_opt.clone()),
            Self::init(database.clone(), 2, tsf_id, storage_opt.clone()),
            Self::init(database.clone(), 3, tsf_id, storage_opt.clone()),
            Self::init(database, 4, tsf_id, storage_opt),
        ]
    }

    pub fn push_compact_meta(
        &mut self,
        compact_meta: &CompactMeta,
        series_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
        tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) {
        let file_path = if compact_meta.is_delta {
            let base_dir = self.storage_opt.delta_dir(&self.owner, self.tsf_id);
            make_delta_file(base_dir, compact_meta.file_id)
        } else {
            let base_dir = self.storage_opt.tsm_dir(&self.owner, self.tsf_id);
            make_tsm_file(base_dir, compact_meta.file_id)
        };
        self.files.push(Arc::new(ColumnFile::with_compact_data(
            compact_meta,
            file_path,
            series_filter,
            tsm_reader_cache,
        )));
        self.tsf_id = compact_meta.tsf_id;
        self.cur_size += compact_meta.file_size;
        self.time_range.max_ts = self.time_range.max_ts.max(compact_meta.max_ts);
        self.time_range.min_ts = self.time_range.min_ts.min(compact_meta.min_ts);

        self.sort_file_asc();
    }

    pub fn push_column_file(&mut self, file: Arc<ColumnFile>) {
        self.cur_size += file.size();
        self.time_range.max_ts = self.time_range.max_ts.max(file.time_range().max_ts);
        self.time_range.min_ts = self.time_range.min_ts.min(file.time_range().min_ts);
        self.files.push(file);

        self.sort_file_asc();
    }

    /// Update time_range by a scan with files.
    /// If files is empty, time_range will be (i64::MAX, i64::MIN).
    pub(crate) fn update_time_range(&mut self) {
        let mut min_ts = Timestamp::MAX;
        let mut max_ts = Timestamp::MIN;
        for f in self.files.iter() {
            min_ts = min_ts.min(f.time_range().min_ts);
            max_ts = max_ts.max(f.time_range().max_ts);
        }
        self.time_range = TimeRange::new(min_ts, max_ts);
    }

    pub fn sort_file_asc(&mut self) {
        self.files
            .sort_by(|a, b| a.file_id().partial_cmp(&b.file_id()).unwrap());
    }

    pub fn disk_storage(&self) -> u64 {
        self.files.iter().map(|f| f.size()).sum()
    }

    pub fn level(&self) -> u32 {
        self.level
    }

    pub async fn overlaps_column_files(
        &self,
        time_ranges: &TimeRanges,
        series_id: SeriesId,
    ) -> TskvResult<Vec<Arc<ColumnFile>>> {
        let mut res = Vec::new();
        for file in self.files.iter() {
            if time_ranges.overlaps(file.time_range())
                && file.maybe_contains_series_id(series_id).await?
            {
                res.push(file.clone());
            }
        }
        res.sort_by_key(|f| *f.time_range());
        Ok(res)
    }
}
