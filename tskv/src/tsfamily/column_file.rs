use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use cache::{AsyncCache, ShardedAsyncCache};
use models::codec::Encoding;
use models::predicate::domain::TimeRange;
use models::{FieldId, SeriesId, SeriesKey};
use snafu::ResultExt;
use tokio::sync::{RwLock as AsyncRwLock, RwLockWriteGuard as AsyncRwLockWriteGuard};
use trace::{debug, error, info};
use utils::BloomFilter;

use super::version::CompactMeta;
use crate::error::{FileSystemSnafu, TskvResult};
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::FileSystem;
use crate::tsm::reader::TsmReader;
use crate::tsm::tombstone::tombstone_compact_tmp_path;
use crate::tsm::writer::TsmWriter;
use crate::{tsm, ColumnFileId, LevelId};

#[derive(Debug)]
pub struct ColumnFile {
    file_id: ColumnFileId,
    level: LevelId,
    time_range: TimeRange,
    size: u64,
    series_id_filter: AsyncRwLock<Option<Arc<BloomFilter>>>,
    deleted: AtomicBool,
    compacting: Arc<AsyncRwLock<bool>>,

    path: PathBuf,
    tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
}

impl ColumnFile {
    pub fn with_compact_data(
        meta: &CompactMeta,
        path: impl AsRef<Path>,
        series_id_filter: AsyncRwLock<Option<Arc<BloomFilter>>>,
        tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            file_id: meta.file_id,
            level: meta.level,
            time_range: TimeRange::new(meta.min_ts, meta.max_ts),
            size: meta.file_size,
            series_id_filter,
            deleted: AtomicBool::new(false),
            compacting: Arc::new(AsyncRwLock::new(false)),
            path: path.as_ref().into(),
            tsm_reader_cache,
        }
    }

    pub fn file_id(&self) -> ColumnFileId {
        self.file_id
    }

    pub fn level(&self) -> LevelId {
        self.level
    }

    pub fn is_delta(&self) -> bool {
        self.level == 0
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn file_path(&self) -> &PathBuf {
        &self.path
    }

    pub fn tombstone_path(&self) -> PathBuf {
        let mut path = self.path.clone();
        path.set_extension(tsm::TOMBSTONE_FILE_SUFFIX);
        path
    }

    pub fn overlap(&self, time_range: &TimeRange) -> bool {
        self.time_range.overlaps(time_range)
    }

    pub async fn maybe_contains_series_id(&self, series_id: SeriesId) -> TskvResult<bool> {
        let bloom_filter = self.load_bloom_filter().await?;
        let res = bloom_filter.maybe_contains(&series_id.to_be_bytes());
        Ok(res)
    }

    pub async fn load_bloom_filter(&self) -> TskvResult<Arc<BloomFilter>> {
        {
            if let Some(filter) = self.series_id_filter.read().await.as_ref() {
                return Ok(filter.clone());
            }
        }
        let mut filter_w = self.series_id_filter.write().await;
        if let Some(filter) = filter_w.as_ref() {
            return Ok(filter.clone());
        }
        let bloom_filter = if let Some(tsm_reader_cache) = self.tsm_reader_cache.upgrade() {
            let reader = match tsm_reader_cache
                .get(&format!("{}", self.path.display()))
                .await
            {
                Some(r) => r,
                None => {
                    let reader = TsmReader::open(&self.path).await?;
                    let reader = Arc::new(reader);
                    tsm_reader_cache
                        .insert(self.path.display().to_string(), reader.clone())
                        .await;
                    reader
                }
            };
            reader.footer().series().bloom_filter().clone()
        } else {
            TsmReader::open(&self.path)
                .await?
                .footer()
                .series()
                .bloom_filter()
                .clone()
        };
        let bloom_filter = Arc::new(bloom_filter);
        filter_w.replace(bloom_filter.clone());
        Ok(bloom_filter)
    }

    pub async fn contains_any_series_id(&self, series_ids: &[SeriesId]) -> TskvResult<bool> {
        for series_id in series_ids {
            if self.maybe_contains_series_id(*series_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn contains_any_field_id(&self, _series_ids: &[FieldId]) -> bool {
        unimplemented!("contains_any_field_id")
    }

    pub async fn update_tag_value(
        &self,
        series: &HashMap<SeriesId, SeriesKey>,
        encode_tsm_meta: Encoding,
    ) -> TskvResult<Option<PathBuf>> {
        let mut meta = if self
            .contains_any_series_id(&series.keys().copied().collect::<Vec<_>>())
            .await?
        {
            TsmReader::open(&self.path)
                .await?
                .tsm_meta_data()
                .as_ref()
                .clone()
        } else {
            return Ok(None);
        };
        meta.update_tag_value(series)?;
        let local_file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let writer = local_file_system
            .open_file_writer(&self.path, 1024)
            .await
            .context(FileSystemSnafu)?;
        TsmWriter::update_file_meta_data(
            self.file_id,
            self.path.clone(),
            writer,
            meta,
            encode_tsm_meta,
        )
        .await?;
        Ok(Some(self.path.clone()))
    }
}

impl ColumnFile {
    pub fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::Acquire)
    }

    pub fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::Release);
    }

    pub async fn is_compacting(&self) -> bool {
        *self.compacting.read().await
    }

    pub async fn write_lock_compacting(&self) -> AsyncRwLockWriteGuard<'_, bool> {
        self.compacting.write().await
    }

    pub async fn mark_compacting(&self) -> bool {
        let mut compacting = self.compacting.write().await;
        if *compacting {
            false
        } else {
            *compacting = true;
            true
        }
    }
}

impl Drop for ColumnFile {
    fn drop(&mut self) {
        debug!(
            "Removing tsm file {} and it's tombstone if exists.",
            self.file_id
        );
        if self.is_deleted() {
            let path = self.file_path();
            if let Some(cache) = self.tsm_reader_cache.upgrade() {
                let k = format!("{}", path.display());
                tokio::spawn(async move {
                    cache.remove(&k).await;
                });
            }
            if let Err(e) = std::fs::remove_file(path) {
                error!(
                    "Failed to remove tsm file {} at '{}': {e}",
                    self.file_id,
                    path.display()
                );
            } else {
                info!("Removed tsm file {} at '{}", self.file_id, path.display());
            }

            let tombstone_path = self.tombstone_path();
            if LocalFileSystem::try_exists(&tombstone_path) {
                if let Err(e) = std::fs::remove_file(&tombstone_path) {
                    error!(
                        "Failed to remove tsm tombstone '{}': {e}",
                        tombstone_path.display()
                    );
                } else {
                    info!("Removed tsm tombstone '{}", tombstone_path.display());
                }
            }

            match tombstone_compact_tmp_path(&tombstone_path) {
                Ok(path) => {
                    info!(
                        "Trying to remove tsm tombstone_compact_tmp: '{}'",
                        path.display()
                    );
                    if LocalFileSystem::try_exists(&path) {
                        if let Err(e) = std::fs::remove_file(&path) {
                            error!(
                                "Failed to remove tsm tombstone_compact_tmp '{}': {e}",
                                path.display()
                            );
                        } else {
                            info!("Removed tsm tombstone_compact_tmp '{}'", path.display());
                        }
                    }
                }
                Err(e) => error!(
                    "Failed to remove tsm tombstone_compact_tmp '{}', path invalid: {e}",
                    path.display()
                ),
            }
        }
    }
}

impl std::fmt::Display for ColumnFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ level:{}, file_id:{}, time_range:{}-{}, file size:{} }}",
            self.level, self.file_id, self.time_range.min_ts, self.time_range.max_ts, self.size,
        )
    }
}

#[cfg(test)]
impl ColumnFile {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_id: ColumnFileId,
        level: LevelId,
        time_range: TimeRange,
        size: u64,
        path: impl AsRef<Path>,
    ) -> Self {
        Self {
            file_id,
            level,
            time_range,
            size,
            series_id_filter: AsyncRwLock::new(None),
            deleted: AtomicBool::new(false),
            compacting: Arc::new(AsyncRwLock::new(false)),
            path: path.as_ref().into(),
            tsm_reader_cache: Weak::new(),
        }
    }

    pub fn set_series_id_filter(
        &mut self,
        series_id_filter: AsyncRwLock<Option<Arc<BloomFilter>>>,
    ) {
        self.series_id_filter = series_id_filter;
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::TimeUnit;
    use models::codec::Encoding;
    use models::predicate::domain::TimeRange;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesKey, Tag, ValueType};

    use crate::tsfamily::column_file::ColumnFile;
    use crate::tsm::reader::TsmReader;
    use crate::tsm::writer::test::{i64_column, ts_column};
    use crate::tsm::writer::TsmWriter;

    #[tokio::test]
    async fn test_update_tag() {
        let path = "/tmp/test/tskv/tsfamily/columnfile/test_update_tag".to_string();
        let mut writer = TsmWriter::open(&path, 1, 0, true, Encoding::Null)
            .await
            .unwrap();

        let tsm_file_path = writer.path().to_path_buf();
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(1, "t1".to_string(), ColumnType::Tag, Encoding::default()),
                TableColumn::new(
                    2,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![ts_column(vec![1, 2, 3]), i64_column(vec![1, 2, 3])],
        )
        .unwrap();

        let series1 = SeriesKey::default();
        let series_update = SeriesKey {
            tags: vec![Tag::new(vec![1], vec![2])],
            table: "test0".to_string(),
        };

        writer
            .write_record_batch(1, series1.clone(), schema.clone(), data1)
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let column_file = ColumnFile::new(1, 0, TimeRange::new(1, 3), 0, tsm_file_path);
        let path = column_file
            .update_tag_value(&HashMap::from([(1, series_update.clone())]), Encoding::Null)
            .await
            .unwrap();

        let reader = TsmReader::open(&path.unwrap()).await.unwrap();
        let series_read = reader
            .tsm_meta_data()
            .chunk()
            .get(&1)
            .unwrap()
            .series_key()
            .clone();

        assert_ne!(series_read, series1);
        assert_eq!(series_read, series_update);
    }
}
