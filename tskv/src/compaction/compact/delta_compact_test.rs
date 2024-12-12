#![cfg(test)]

use std::path::Path;

use arrow_schema::TimeUnit;
use cache::ShardedAsyncCache;
use models::codec::Encoding;
use models::predicate::domain::TimeRanges;
use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
use models::{Timestamp, ValueType};
use utils::id_generator::IDGenerator;

use super::test::*;
use super::*;
use crate::compaction::CompactTask;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::tsfamily::column_file::ColumnFile;
use crate::tsfamily::level_info::LevelInfo;
use crate::tsfamily::version::Version;
use crate::tsm::tombstone::{tombstone_compact_tmp_path, TsmTombstoneCache};
use crate::tsm::writer::TsmWriter;
use crate::tsm::TsmTombstone;
use crate::{file_utils, record_file, LevelId, Options};

#[tokio::test]
#[ignore = "Manually test"]
async fn test_big_delta_compaction() {
    let dir = "/tmp/test/big_delta_compaction/1";
    let tenant_database = Arc::new("cnosdb.benchmark".to_string());
    let opt = create_options(dir.to_string(), 1);

    let delta_dir = opt.storage.delta_dir(&tenant_database, 1);
    #[rustfmt::skip]
        let compact_req= prepare_delta_compaction(
        tenant_database,
        opt,
        5,
        vec![
            Arc::new(ColumnFile::new(1187, 0, (1626052320072000000, 1626057359280000000).into(), 0, delta_dir.join("_001187.delta"))),
            Arc::new(ColumnFile::new(1199, 0, (1626057360072000000, 1626074639280000000).into(), 0, delta_dir.join("_001199.delta"))),
            Arc::new(ColumnFile::new(1210, 0, (1626069600072000000, 1626073919280000000).into(), 0, delta_dir.join("_001210.delta"))),
            Arc::new(ColumnFile::new(1225, 0, (1626074640072000000, 1626086159280000000).into(), 0, delta_dir.join("_001225.delta"))),
        ],
        vec![],
        (1626053759280000001, 1626086159280000000).into(),
        1,
        0,
    );
    let (version_edit, _) = run_delta_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    println!("{version_edit}");
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_delta_compaction(
    tenant_database: Arc<String>,
    opt: Arc<Options>,
    next_file_id: ColumnFileId,
    delta_files: Vec<Arc<ColumnFile>>,
    tsm_files: Vec<Arc<ColumnFile>>,
    out_time_range: TimeRange,
    out_level: LevelId,
    out_level_max_ts: Timestamp,
) -> CompactReq {
    let vnode_id = 1;
    let version = Arc::new(Version::new(
        vnode_id,
        tenant_database.clone(),
        opt.storage.clone(),
        1,
        LevelInfo::init_levels(tenant_database, 0, opt.storage.clone()),
        out_level_max_ts,
        Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
    ));
    let mut files = delta_files;
    files.extend_from_slice(&tsm_files);

    CompactReq {
        compact_task: CompactTask::Delta(vnode_id),
        version,
        files,
        in_level: 0,
        out_level,
        out_time_range,
        file_id: IDGenerator::new(next_file_id),
    }
}

#[tokio::test]
async fn test_delta_compaction_1() {
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
            TableColumn::new(
                1,
                "f1".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
            TableColumn::new(
                2,
                "f2".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
            TableColumn::new(
                3,
                "f3".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
        ],
    );
    let schema = Arc::new(schema);

    let data1 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![4, 5, 6]),
            i64_column(vec![114, 115, 116]),
            i64_column(vec![124, 125, 126]),
            i64_column(vec![134, 135, 136]),
        ],
    )
    .unwrap();
    let data2 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![1, 2, 3]),
            i64_column(vec![211, 212, 213]),
            i64_column(vec![221, 222, 223]),
            i64_column(vec![231, 232, 233]),
        ],
    )
    .unwrap();

    let data3 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![7, 8, 9]),
            i64_column(vec![317, 318, 319]),
            i64_column(vec![327, 328, 329]),
            i64_column(vec![337, 338, 339]),
        ],
    )
    .unwrap();

    let expected_data = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            i64_column(vec![211, 212, 213, 114, 115, 116, 317, 318, 319]),
            i64_column(vec![221, 222, 223, 124, 125, 126, 327, 328, 329]),
            i64_column(vec![231, 232, 233, 134, 135, 136, 337, 338, 339]),
        ],
    )
    .unwrap();

    let data = vec![
        HashMap::from([(1, data1)]),
        HashMap::from([(1, data2)]),
        HashMap::from([(1, data3)]),
    ];

    let expected_data = HashMap::from([(1 as SeriesId, vec![expected_data])]);

    let dir = "/tmp/test/delta_compaction/1";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, schema, 0).await;
    let compact_req = prepare_delta_compaction(
        tenant_database,
        opt,
        next_file_id,
        files,
        vec![],
        (1, 9).into(),
        1,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_delta_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with duplicate timestamp.
#[tokio::test]
async fn test_delta_compaction_2() {
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
            TableColumn::new(
                1,
                "f1".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
            TableColumn::new(
                2,
                "f2".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
            TableColumn::new(
                3,
                "f3".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
            TableColumn::new(
                4,
                "f4".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
        ],
    );
    let schema = Arc::new(schema);
    let data1 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![1, 2, 3, 4]),
            i64_column(vec![111, 112, 113, 114]),
            i64_some_column(vec![None, None, None, None]),
            i64_column(vec![131, 132, 133, 134]),
            i64_some_column(vec![Some(141), Some(142), Some(143), None]),
        ],
    )
    .unwrap();

    let data2 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![4, 5, 6, 7]),
            i64_some_column(vec![Some(214), Some(215), Some(216), None]),
            i64_some_column(vec![Some(224), Some(225), Some(226), None]),
            i64_column(vec![234, 235, 236, 237]),
            i64_some_column(vec![None, None, None, None]),
        ],
    )
    .unwrap();

    let data3 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![7, 8, 9]),
            i64_some_column(vec![Some(317), Some(318), Some(319)]),
            i64_some_column(vec![Some(327), Some(328), Some(329)]),
            i64_some_column(vec![Some(337), Some(338), Some(339)]),
            i64_some_column(vec![None, None, None]),
        ],
    )
    .unwrap();

    let expected_data = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            i64_column(vec![111, 112, 113, 214, 215, 216, 317, 318, 319]),
            i64_some_column(vec![
                None,
                None,
                None,
                Some(224),
                Some(225),
                Some(226),
                Some(327),
                Some(328),
                Some(329),
            ]),
            i64_column(vec![131, 132, 133, 234, 235, 236, 337, 338, 339]),
            i64_some_column(vec![
                Some(141),
                Some(142),
                Some(143),
                None,
                None,
                None,
                None,
                None,
                None,
            ]),
        ],
    )
    .unwrap();

    let data = vec![
        HashMap::from([(1, data1)]),
        HashMap::from([(1, data2)]),
        HashMap::from([(1, data3)]),
    ];

    let expected_data = HashMap::from([(1 as SeriesId, vec![expected_data])]);

    let dir = "/tmp/test/delta_compaction/2";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, schema, 0).await;
    let compact_req = prepare_delta_compaction(
        tenant_database,
        opt,
        next_file_id,
        files,
        vec![],
        (1, 9).into(),
        1,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_delta_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with tombstones.
#[tokio::test]
async fn test_delta_compaction_3() {
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
            TableColumn::new(
                1,
                "f1".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
        ],
    );
    let schema = Arc::new(schema);

    let data1 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![timestamp_column(vec![1]), i64_column(vec![111])],
    )
    .unwrap();

    let data2 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![2, 3, 4]),
            i64_column(vec![212, 213, 214]),
        ],
    )
    .unwrap();

    let data3 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![4, 5, 6]),
            i64_column(vec![314, 315, 316]),
        ],
    )
    .unwrap();

    let data4 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![timestamp_column(vec![8, 9]), i64_column(vec![418, 419])],
    )
    .unwrap();

    let expected_data = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![1, 2, 3, 4, 5, 6, 8, 9]),
            i64_some_column(vec![
                Some(111),
                None,
                None,
                None,
                None,
                None,
                Some(418),
                Some(419),
            ]),
        ],
    )
    .unwrap();

    let data = vec![
        HashMap::from([(1, data1)]),
        HashMap::from([(1, data2)]),
        HashMap::from([(1, data3)]),
        HashMap::from([(1, data4)]),
    ];

    let expected_data = HashMap::from([(1 as SeriesId, vec![expected_data])]);

    let dir = "/tmp/test/delta_compaction/3";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, schema, 0).await;
    for f in files.iter().take(2 + 1).skip(1) {
        let tsm_tombstone = TsmTombstone::open(&dir, f.file_id()).await.unwrap();
        tsm_tombstone
            .add_range(&[(1, 1)], TimeRange::new(2, 6), None)
            .await
            .unwrap();
        tsm_tombstone.flush().await.unwrap();
    }
    let compact_req = prepare_delta_compaction(
        tenant_database,
        opt,
        next_file_id,
        files,
        vec![],
        (1, 9).into(),
        1,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_delta_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

type DataDesc = Vec<(ColumnFileId, Vec<(RecordBatch, Arc<TskvTableSchema>)>)>;

#[allow(clippy::too_many_arguments)]
async fn test_delta_compaction(
    dir: &str,
    delta_files_desc: DataDesc,
    tsm_files_desc: DataDesc,
    out_time_range: TimeRange,
    max_ts: Timestamp,
    expected_data_desc: Vec<RecordBatch>,
    expected_data_level: LevelId,
    expected_delta_tombstone_all_excluded: HashMap<ColumnFileId, TimeRanges>,
) {
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let tsm_dir = opt.storage.tsm_dir(&tenant_database, 1);
    if !LocalFileSystem::try_exists(&tsm_dir) {
        std::fs::create_dir_all(&tsm_dir).unwrap();
    }
    let delta_dir = opt.storage.delta_dir(&tenant_database, 1);
    if !LocalFileSystem::try_exists(&delta_dir) {
        std::fs::create_dir_all(&delta_dir).unwrap();
    }

    let mut tsm_files = Vec::new();
    for (tsm_sequence, args) in tsm_files_desc.into_iter() {
        let mut tsm_writer = TsmWriter::open(&tsm_dir, tsm_sequence, 0, false, Encoding::Null)
            .await
            .unwrap();
        for (record_batch, schema) in args.into_iter() {
            tsm_writer
                .write_record_batch(1, SeriesKey::default(), schema.clone(), record_batch)
                .await
                .unwrap();
        }
        tsm_writer.finish().await.unwrap();
        tsm_files.push(Arc::new(ColumnFile::new(
            tsm_sequence,
            2,
            TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
            tsm_writer.size(),
            tsm_writer.path(),
        )));
    }

    let mut delta_files = Vec::new();
    for (tsm_sequence, args) in delta_files_desc.into_iter() {
        let mut tsm_writer = TsmWriter::open(&delta_dir, tsm_sequence, 0, true, Encoding::Null)
            .await
            .unwrap();
        for (record_batch, schema) in args.into_iter() {
            tsm_writer
                .write_record_batch(1, SeriesKey::default(), schema.clone(), record_batch)
                .await
                .unwrap();
        }
        tsm_writer.finish().await.unwrap();
        delta_files.push(Arc::new(ColumnFile::new(
            tsm_sequence,
            0,
            TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
            tsm_writer.size(),
            tsm_writer.path(),
        )));
    }

    let mut compact_req = prepare_delta_compaction(
        tenant_database,
        opt,
        5,
        delta_files,
        tsm_files,
        out_time_range,
        expected_data_level,
        max_ts,
    );
    compact_req.in_level = 0;
    compact_req.out_level = expected_data_level;

    let (version_edit, _) = run_delta_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .expect("Delta compaction sucessfully generated some new files");

    check_column_file(
        tsm_dir,
        version_edit,
        HashMap::from([(1 as SeriesId, expected_data_desc)]),
        expected_data_level,
    )
    .await;
    check_delta_file_tombstone(delta_dir, expected_delta_tombstone_all_excluded).await;
}

async fn check_delta_file_tombstone(
    dir: impl AsRef<Path>,
    all_excludes: HashMap<ColumnFileId, TimeRanges>,
) {
    for (file_id, time_ranges) in all_excludes {
        let tombstone_path = file_utils::make_tsm_tombstone_file(&dir, file_id);
        let tombstone_path = tombstone_compact_tmp_path(&tombstone_path).unwrap();
        let mut record_reader = record_file::Reader::open(tombstone_path).await.unwrap();
        let tombstone = TsmTombstoneCache::load_from(&mut record_reader, false)
            .await
            .unwrap();
        assert_eq!(tombstone.all_excluded(), &time_ranges);
    }
}

/// Test compaction on level-0 (delta compaction) with multi-field.
#[tokio::test]
async fn test_big_delta_compaction_1() {
    let schema1 = TskvTableSchema::new(
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
            TableColumn::new(
                1,
                "f1".to_string(),
                ColumnType::Field(ValueType::Unsigned),
                Encoding::default(),
            ),
            TableColumn::new(
                2,
                "f2".to_string(),
                ColumnType::Field(ValueType::Integer),
                Encoding::default(),
            ),
            TableColumn::new(
                3,
                "f3".to_string(),
                ColumnType::Field(ValueType::Boolean),
                Encoding::default(),
            ),
        ],
    );
    let mut schema2 = schema1.clone();
    schema2.add_column(TableColumn::new(
        4,
        "f4".to_string(),
        ColumnType::Field(ValueType::Float),
        Encoding::default(),
    ));
    schema2.schema_version += 1;

    let schema1 = Arc::new(schema1);
    let schema2 = Arc::new(schema2);

    let delta_files_desc = vec![
        (
            2_u64,
            vec![
                (
                    RecordBatch::try_new(
                        schema1.to_record_data_schema(),
                        vec![
                            generate_column_ts(1, 1000),
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![]),
                        ],
                    )
                    .unwrap(),
                    schema1.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema1.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(1001, 2000),
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(500, 999)]),
                            generate_column_bool(1000, vec![(500, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema1.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema1.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(2001, 2500),
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                        ],
                    )
                    .unwrap(),
                    schema1.clone(),
                ),
            ],
        ),
        (
            3,
            vec![
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(1, 1000),
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(1001, 2000),
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![]),
                            generate_column_f64(1000, vec![(500, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(2001, 3000),
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(500, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(3001, 4000),
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(4001, 4500),
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                            generate_column_f64(500, vec![(0, 499)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
            ],
        ),
        (
            4,
            vec![
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(1001, 2000),
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(2001, 3000),
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![]),
                            generate_column_f64(1000, vec![(500, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(3001, 4000),
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(500, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(4001, 5000),
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(5001, 6000),
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
                (
                    RecordBatch::try_new(
                        schema2.clone().to_record_data_schema(),
                        vec![
                            generate_column_ts(6001, 6500),
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                            generate_column_f64(500, vec![(0, 499)]),
                        ],
                    )
                    .unwrap(),
                    schema2.clone(),
                ),
            ],
        ),
    ];

    // The target tsm file: [2001~5050]
    let max_level_ts = 5050;

    let tsm_file_desc = vec![(
        1,
        vec![
            (
                RecordBatch::try_new(
                    schema2.clone().to_record_data_schema(),
                    vec![
                        generate_column_ts(2001, 3000),
                        generate_column_u64(1000, vec![]),
                        generate_column_i64(1000, vec![]),
                        generate_column_bool(1000, vec![(0, 999)]),
                        generate_column_f64(1000, vec![(500, 999)]),
                    ],
                )
                .unwrap(),
                schema2.clone(),
            ),
            (
                RecordBatch::try_new(
                    schema2.clone().to_record_data_schema(),
                    vec![
                        generate_column_ts(3001, 4000),
                        generate_column_u64(1000, vec![(0, 999)]),
                        generate_column_i64(1000, vec![(0, 999)]),
                        generate_column_bool(1000, vec![]),
                        generate_column_f64(1000, vec![(0, 999)]),
                    ],
                )
                .unwrap(),
                schema2.clone(),
            ),
            (
                RecordBatch::try_new(
                    schema2.clone().to_record_data_schema(),
                    vec![
                        generate_column_ts(4001, 5000),
                        generate_column_u64(1000, vec![]),
                        generate_column_i64(1000, vec![]),
                        generate_column_bool(1000, vec![]),
                        generate_column_f64(1000, vec![(0, 999)]),
                    ],
                )
                .unwrap(),
                schema2.clone(),
            ),
            (
                RecordBatch::try_new(
                    schema2.clone().to_record_data_schema(),
                    vec![
                        generate_column_ts(5001, 6000),
                        generate_column_u64(1000, vec![(50, 999)]),
                        generate_column_i64(1000, vec![(0, 999)]),
                        generate_column_bool(1000, vec![(0, 999)]),
                        generate_column_f64(1000, vec![(0, 999)]),
                    ],
                )
                .unwrap(),
                schema2.clone(),
            ),
        ],
    )];

    let expected_data_target_level: Vec<RecordBatch> = vec![
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(2001, 3000),
                generate_column_u64(1000, vec![]),
                generate_column_i64(1000, vec![]),
                generate_column_bool(1000, vec![]),
                generate_column_f64(1000, vec![(500, 999)]),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(3001, 4000),
                generate_column_u64(1000, vec![]),
                generate_column_i64(1000, vec![]),
                generate_column_bool(1000, vec![]),
                generate_column_f64(1000, vec![(0, 999)]),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(4001, 5000),
                generate_column_u64(1000, vec![]),
                generate_column_i64(1000, vec![]),
                generate_column_bool(1000, vec![]),
                generate_column_f64(1000, vec![(0, 999)]),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(5001, 5050),
                generate_column_u64(50, vec![]),
                generate_column_i64(50, vec![(0, 49)]),
                generate_column_bool(50, vec![(0, 49)]),
                generate_column_f64(50, vec![(0, 49)]),
            ],
        )
        .unwrap(),
    ];

    test_delta_compaction(
        "/tmp/test/delta_compaction/big_1",
        delta_files_desc, // (1,2500), (1,4500), (1001,6500)
        tsm_file_desc,    // (2005,5050)
        (2001, 5050).into(),
        max_level_ts,
        expected_data_target_level,
        1,
        HashMap::from([
            (2, TimeRanges::new(vec![(2001, 5050).into()])),
            (3, TimeRanges::new(vec![(2001, 5050).into()])),
            (4, TimeRanges::new(vec![(2001, 5050).into()])),
        ]),
    )
    .await;
}
