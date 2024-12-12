use std::cmp::max;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use models::codec::Encoding;
use parking_lot::RwLock;
use trace::info;
use utils::BloomFilter;

use super::metrics::FlushMetrics;
use crate::compaction::FlushReq;
use crate::error::TskvResult;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_utils::make_delta_file;
use crate::mem_cache::memcache::{MemCache, MemCacheSeriesScanIterator};
use crate::tsfamily::version::{CompactMeta, VersionEdit};
use crate::tsm::writer::TsmWriter;
use crate::{ColumnFileId, VnodeId};

pub struct FlushTask {
    owner: String,
    tsf_id: VnodeId,
    memcache: Arc<RwLock<MemCache>>,
    tsm_meta_compress: Encoding,

    path_delta: PathBuf,
    current_delta_file_id: ColumnFileId,
}

impl FlushTask {
    pub async fn new(
        owner: String,
        tsf_id: VnodeId,
        memcache: Arc<RwLock<MemCache>>,
        path_tsm: PathBuf,
        tsm_meta_compress: Encoding,
    ) -> TskvResult<Self> {
        Ok(Self {
            owner,
            tsf_id,
            memcache,
            tsm_meta_compress,
            path_delta: path_tsm,
            current_delta_file_id: 0,
        })
    }

    pub fn clear_files(&mut self) {
        let tsm_path = make_delta_file(&self.path_delta, self.current_delta_file_id);
        if let Err(err) = LocalFileSystem::remove_if_exists(&tsm_path) {
            info!("delete flush tsm file: {:?} failed: {}", tsm_path, err);
        }
    }

    pub async fn run(
        &mut self,
        max_level_ts: i64,
        high_seq_no: u64,
        metrics: &mut FlushMetrics,
    ) -> TskvResult<(VersionEdit, HashMap<u64, Arc<BloomFilter>>)> {
        let owner = self.owner.clone();
        let mut version_edit = VersionEdit::new_update_vnode(self.tsf_id, owner, high_seq_no);

        let file_id = self.memcache.read().file_id();
        self.current_delta_file_id = file_id;
        let mut tsm_writer =
            TsmWriter::open(&self.path_delta, file_id, 0, true, self.tsm_meta_compress).await?;

        let mut tsm_writer_is_used = false;
        let series_iter = MemCacheSeriesScanIterator::new(self.memcache.clone());
        metrics.flush_series_count += series_iter.series_count();
        for series in series_iter {
            let instant = std::time::Instant::now();
            let (series_id, series_key, time_range, convert_result) = {
                let series = series.read();
                (
                    series.series_id,
                    series.series_key.clone(),
                    series.range,
                    series.convert_to_page()?,
                )
            };
            metrics.convert_to_page_time += instant.elapsed().as_millis() as u64;

            let instant = std::time::Instant::now();
            if let Some((schema, pages)) = convert_result {
                if !pages.is_empty() {
                    tsm_writer_is_used = true;
                    tsm_writer
                        .write_pages(
                            schema.clone(),
                            series_id,
                            series_key.clone(),
                            pages,
                            time_range,
                        )
                        .await?;
                }
            }
            metrics.writer_pages_time += instant.elapsed().as_millis() as u64;
        }
        metrics.write_tsm_pages_size += tsm_writer.size();

        let finish_instant = std::time::Instant::now();
        let mut files_meta = HashMap::new();
        let mut max_level_ts = max_level_ts;
        if tsm_writer_is_used {
            tsm_writer.finish().await?;
            files_meta.insert(
                tsm_writer.file_id(),
                Arc::new(tsm_writer.series_bloom_filter().clone()),
            );
            let tsm_meta = CompactMeta::new(
                self.tsf_id,
                tsm_writer.file_id(),
                tsm_writer.size(),
                0,
                tsm_writer.min_ts(),
                tsm_writer.max_ts(),
            );
            max_level_ts = max(max_level_ts, tsm_meta.max_ts);
            version_edit.add_file(tsm_meta, max_level_ts);
        } else {
            let path = tsm_writer.path();
            let result = LocalFileSystem::remove_if_exists(path);
            info!("Flush: remove unsed file: {:?}, {:?}", path, result);
        }

        metrics.writer_finish_time += finish_instant.elapsed().as_millis() as u64;
        metrics.write_tsm_file_size += tsm_writer.size();

        Ok((version_edit, files_meta))
    }
}

pub async fn flush_memtable(
    req: &FlushReq,
    mem: Arc<RwLock<MemCache>>,
) -> TskvResult<(VersionEdit, HashMap<u64, Arc<BloomFilter>>)> {
    let high_seq_no = mem.read().seq_no();
    let low_seq_no = mem.read().min_seq_no();

    req.flush_metrics.write().await.min_seq = low_seq_no;
    req.flush_metrics.write().await.max_seq = high_seq_no;

    info!(
        "Flush: running  {} seq: [{}-{}]",
        req, low_seq_no, high_seq_no,
    );

    // todo: build path by vnode data
    let (storage_opt, max_level_ts) = {
        let tsf_rlock = req.ts_family.read().await;
        tsf_rlock.update_last_modified().await;
        (tsf_rlock.storage_opt(), tsf_rlock.version().max_level_ts())
    };

    let owner = req.owner.clone();
    let path_delta = storage_opt.delta_dir(&req.owner, req.tf_id);
    let encoding = storage_opt.tsm_meta_compress;
    let mut flush_task = FlushTask::new(owner, req.tf_id, mem, path_delta, encoding).await?;

    let mut metrics = req.flush_metrics.write().await;
    let result = flush_task
        .run(max_level_ts, high_seq_no, &mut metrics)
        .await;
    let (version_edit, files_meta) = match result {
        Ok((ve, files_meta)) => (ve, files_meta),
        Err(err) => {
            flush_task.clear_files();
            return Err(err);
        }
    };

    info!(
        "Flush: completed: owner: {} tsf_id: {}, version edit: {:?}",
        req.owner, req.tf_id, version_edit
    );

    Ok((version_edit, files_meta))
}

#[cfg(test)]
pub mod flush_tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, RecordBatch, TimestampNanosecondArray};
    use arrow_schema::TimeUnit;
    use cache::ShardedAsyncCache;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesKey, ValueType};
    use parking_lot::lock_api::RwLock;
    use utils::dedup_front_by_key;

    use crate::compaction::flush::FlushTask;
    use crate::compaction::metrics::FlushMetrics;
    use crate::file_system::async_filesystem::LocalFileSystem;
    use crate::file_system::FileSystem;
    use crate::mem_cache::memcache::MemCache;
    use crate::mem_cache::row_data::{OrderedRowsData, RowData};
    use crate::mem_cache::series_data::RowGroup;
    use crate::tsfamily::level_info::LevelInfo;
    use crate::tsfamily::version::Version;
    use crate::tsm::reader::TsmReader;
    use crate::tsm::writer::TsmWriter;
    use crate::Options;

    fn f64_column(data: Vec<f64>) -> ArrayRef {
        Arc::new(Float64Array::from(data))
    }

    fn ts_column(data: Vec<i64>) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(data))
    }

    #[test]
    fn test_sort_dedup() {
        {
            let mut data = vec![(1, 11), (1, 12), (2, 21), (3, 3), (2, 22), (4, 41), (4, 42)];
            data.sort_by_key(|a| a.0);
            assert_eq!(
                &data,
                &vec![(1, 11), (1, 12), (2, 21), (2, 22), (3, 3), (4, 41), (4, 42)]
            );
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(&data, &vec![(1, 12), (2, 22), (3, 3), (4, 42)]);
        }
        {
            // Test dedup-front for list with no duplicated key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (2, "b2".into()),
                (3, "c3".into()),
                (4, "d4".into()),
            ];
            data.sort_by_key(|a| a.0);
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(
                &data,
                &vec![
                    (1, "a1".into()),
                    (2, "b2".into()),
                    (3, "c3".into()),
                    (4, "d4".into()),
                ]
            );
        }
        {
            // Test dedup-front for list with only one key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (1, "a2".into()),
                (1, "a3".into()),
                (1, "a4".into()),
            ];
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(&data, &vec![(1, "a4".into()),]);
        }
        {
            // Test dedup-front for list with shuffled multiply duplicated key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (1, "b1".into()),
                (2, "c2".into()),
                (3, "d3".into()),
                (2, "e2".into()),
                (4, "e4".into()),
                (4, "f4".into()),
            ];
            data.sort_by_key(|a| a.0);
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(
                &data,
                &vec![
                    (1, "b1".into()),
                    (2, "e2".into()),
                    (3, "d3".into()),
                    (4, "f4".into()),
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_flush_run() {
        let dir = "/tmp/test/flush/1";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::tskv::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let database = Arc::new("cnosdb.test".to_string());

        #[rustfmt::skip]
        let levels = [
            LevelInfo::init(database.clone(), 0, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 1, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 2, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 3, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 4, 0, opt.storage.clone()),
        ];
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));

        let version = Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            levels,
            5,
            tsm_reader_cache,
        );
        let sid = 1;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 0, 1000, 2, 1, &memory_pool);
        #[rustfmt::skip]
        let mut schema_1 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(4, "f_col_1".to_string(), ColumnType::Field(ValueType::Float), Default::default()),
            ],
        );
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        rows.insert(RowData {
            ts: 9,
            fields: vec![Some(FieldVal::Float(9.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows,
            size: 10,
        };

        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();

        let memcache = Arc::new(RwLock::new(mem_cache));
        let path_tsm = PathBuf::from("/tmp/test/flush/tsm1");
        let mut flush_task = FlushTask::new(
            database.to_string(),
            1,
            memcache,
            path_tsm.clone(),
            Encoding::Snappy,
        )
        .await
        .unwrap();

        let mut metrics = FlushMetrics::default();
        let (edit, _) = flush_task
            .run(version.max_level_ts(), 100, &mut metrics)
            .await
            .unwrap();

        let delta_info = edit.add_files.first().unwrap();

        let mut schema = TskvTableSchema::new(
            "test_tenant".to_string(),
            "test_db".to_string(),
            "test_table".to_string(),
            vec![
                TableColumn::new(
                    1,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "tag_col_1".to_string(),
                    ColumnType::Tag,
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "tag_col_2".to_string(),
                    ColumnType::Tag,
                    Encoding::default(),
                ),
                TableColumn::new(
                    4,
                    "f_col_1".to_string(),
                    ColumnType::Field(ValueType::Float),
                    Encoding::default(),
                ),
            ],
        );

        schema.schema_version = 1;
        let schema = Arc::new(schema);
        let data = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                ts_column(vec![1, 3, 6, 9]),
                f64_column(vec![1.0, 3.0, 6.0, 9.0]),
            ],
        )
        .unwrap();

        {
            let delta_writer =
                TsmWriter::open(&path_tsm, delta_info.file_id, 100, true, Encoding::Zstd)
                    .await
                    .unwrap();
            let delta_reader = TsmReader::open(delta_writer.path()).await.unwrap();
            let delta_data = delta_reader.read_record_batch(1, 0).await.unwrap();
            assert_eq!(delta_data, data);
        }
    }

    #[tokio::test]
    async fn test_flush_run_multi_memcache() {
        let dir = "/tmp/test/flush2/1";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::tskv::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let database = Arc::new("cnosdb.test".to_string());

        #[rustfmt::skip]
        let levels = [
            LevelInfo::init(database.clone(), 0, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 1, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 2, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 3, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 4, 0, opt.storage.clone()),
        ];
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));

        let version = Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            levels,
            5,
            tsm_reader_cache,
        );
        let sid = 1;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache1 = MemCache::new(1, 0, 1000, 2, 1, &memory_pool);
        let mem_cache2 = MemCache::new(1, 1, 1000, 2, 1, &memory_pool);

        #[rustfmt::skip]
        let mut schema_1 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(4, "f_col_1".to_string(), ColumnType::Field(ValueType::Float), Default::default()),
            ],
        );
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        rows.insert(RowData {
            ts: 9,
            fields: vec![Some(FieldVal::Float(9.0))],
        });
        #[rustfmt::skip]
        let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 9),
            rows,
            size: 10,
        };

        mem_cache1
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();

        mem_cache2
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();

        let mut metrics = FlushMetrics::default();
        let path_tsm = PathBuf::from("/tmp/test/flush2/tsm1");
        let mut flush_task1 = FlushTask::new(
            database.as_str().to_string(),
            1,
            Arc::new(RwLock::new(mem_cache1)),
            path_tsm.clone(),
            Encoding::Snappy,
        )
        .await
        .unwrap();
        let (edit, _) = flush_task1
            .run(version.max_level_ts(), 100, &mut metrics)
            .await
            .unwrap();
        assert_eq!(edit.add_files.len(), 1);

        let mut flush_task2 = FlushTask::new(
            database.as_str().to_string(),
            1,
            Arc::new(RwLock::new(mem_cache2)),
            path_tsm.clone(),
            Encoding::Zstd,
        )
        .await
        .unwrap();
        let (edit, _) = flush_task2
            .run(edit.max_level_ts, 100, &mut metrics)
            .await
            .unwrap();

        assert_eq!(edit.add_files.len(), 1);
        let tsm_files = LocalFileSystem::list_file_names(&path_tsm);
        assert_eq!(tsm_files.len(), 2);
    }
}
