#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::TimeUnit;
use cache::ShardedAsyncCache;
use models::codec::Encoding;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
use models::{Timestamp, ValueType};
use utils::id_generator::IDGenerator;

use super::test::*;
use super::*;
use crate::compaction::CompactTask;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::kv_option::Options;
use crate::tsfamily::column_file::ColumnFile;
use crate::tsfamily::level_info::LevelInfo;
use crate::tsfamily::version::Version;
use crate::tsm::writer::TsmWriter;
use crate::tsm::TsmTombstone;
use crate::ColumnFileId;

#[tokio::test]
#[ignore = "Manually test"]
async fn test_big_compaction() {
    let dir = "/tmp/test/big_compaction/1";
    let tenant_database = Arc::new("cnosdb.public".to_string());
    let opt = create_options(dir.to_string(), 1);

    let tsm_dir = opt.storage.tsm_dir(&tenant_database, 1);
    #[rustfmt::skip]
        let compact_req = prepare_compaction(
        tenant_database,
        opt,
        5,
        vec![
            Arc::new(ColumnFile::new(393, 0, (1704067260000000000, 1705263600000000000).into(), 0, tsm_dir.join("_000393.tsm"))),
            Arc::new(ColumnFile::new(394, 0, (1705263660000000000, 1705466820000000000).into(), 0, tsm_dir.join("_000394.tsm"))),
        ],
        1,
    );
    let (version_edit, _) = run_normal_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    println!("{version_edit}");
}

pub fn prepare_compaction(
    tenant_database: Arc<String>,
    opt: Arc<Options>,
    next_file_id: ColumnFileId,
    files: Vec<Arc<ColumnFile>>,
    max_level_ts: Timestamp,
) -> CompactReq {
    let vnode_id = 1;
    let version = Arc::new(Version::new(
        vnode_id,
        tenant_database.clone(),
        opt.storage.clone(),
        1,
        LevelInfo::init_levels(tenant_database, 0, opt.storage.clone()),
        max_level_ts,
        Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
    ));

    CompactReq {
        compact_task: CompactTask::Normal(vnode_id),
        version,
        files,
        in_level: 1,
        out_level: 2,
        out_time_range: TimeRange::all(),
        file_id: IDGenerator::new(next_file_id),
    }
}

/// Test compaction with ordered data.
#[tokio::test]
async fn test_compaction_fast() {
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
            timestamp_column(vec![1, 2, 3]),
            i64_column(vec![1, 2, 3]),
            i64_column(vec![1, 2, 3]),
            i64_column(vec![1, 2, 3]),
        ],
    )
    .unwrap();

    let data2 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![4, 5, 6]),
            i64_column(vec![4, 5, 6]),
            i64_column(vec![4, 5, 6]),
            i64_column(vec![4, 5, 6]),
        ],
    )
    .unwrap();

    let data3 = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![7, 8, 9]),
            i64_column(vec![7, 8, 9]),
            i64_column(vec![7, 8, 9]),
            i64_column(vec![7, 8, 9]),
        ],
    )
    .unwrap();

    let expected_data = RecordBatch::try_new(
        schema.to_record_data_schema(),
        vec![
            timestamp_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ],
    )
    .unwrap();

    let data = vec![
        HashMap::from([(1, data1)]),
        HashMap::from([(1, data2)]),
        HashMap::from([(1, data3)]),
    ];

    let expected_data = HashMap::from([(1 as SeriesId, vec![expected_data])]);

    let dir = "/tmp/test/compaction/0";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, schema, 2).await;
    let compact_req = prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_normal_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

#[tokio::test]
async fn test_compaction_1() {
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

    let dir = "/tmp/test/compaction/1";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, schema, 2).await;
    let compact_req = prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_normal_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with duplicate timestamp.
#[tokio::test]
async fn test_compaction_2() {
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
            i64_some_column(vec![Some(111), Some(112), Some(113), Some(114)]),
            i64_some_column(vec![None, None, None, None]),
            i64_some_column(vec![Some(131), Some(132), Some(133), Some(134)]),
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
            i64_some_column(vec![Some(234), Some(235), Some(236), Some(237)]),
            i64_some_column(vec![None, None, None, None]),
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

    let dir = "/tmp/test/compaction/2";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, schema, 2).await;
    let compact_req = prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_normal_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with tombstones.
#[tokio::test]
async fn test_compaction_3() {
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

    let dir = "/tmp/test/compaction/3";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, schema, 2).await;
    for f in files.iter().take(2 + 1).skip(1) {
        let tsm_tombstone = TsmTombstone::open(&dir, f.file_id()).await.unwrap();
        tsm_tombstone
            .add_range(&[(1, 1)], TimeRange::new(2, 6), None)
            .await
            .unwrap();
        tsm_tombstone.flush().await.unwrap();
    }
    let compact_req = prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_normal_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compaction without tombstones.
#[tokio::test]
async fn test_big_compaction_1() {
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
    let data_desc = [
        (
            1_u64,
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
            2,
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
            3,
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
    let expected_data: Vec<RecordBatch> = vec![
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(1, 1000),
                generate_column_u64(1000, vec![]),
                generate_column_i64(1000, vec![]),
                generate_column_bool(1000, vec![]),
                generate_column_f64(1000, vec![]),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(1001, 2000),
                generate_column_u64(1000, vec![]),
                generate_column_i64(1000, vec![]),
                generate_column_bool(1000, vec![]),
                generate_column_f64(1000, vec![]),
            ],
        )
        .unwrap(),
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
                generate_column_bool(1000, vec![(500, 999)]),
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
                generate_column_bool(1000, vec![(0, 999)]),
                generate_column_f64(1000, vec![(0, 999)]),
            ],
        )
        .unwrap(),
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
    ];
    let expected_data = HashMap::from([(1 as SeriesId, expected_data)]);

    let dir = "/tmp/test/compaction/big_1";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    if !LocalFileSystem::try_exists(&dir) {
        std::fs::create_dir_all(&dir).unwrap();
    }
    let max_level_ts = 6500;

    let mut column_files = Vec::new();
    for (tsm_sequence, args) in data_desc.into_iter() {
        let mut tsm_writer = TsmWriter::open(&dir, tsm_sequence, 0, false, Encoding::Snappy)
            .await
            .unwrap();
        for (record_batch, schema) in args.into_iter() {
            tsm_writer
                .write_record_batch(1, SeriesKey::default(), schema, record_batch)
                .await
                .unwrap();
        }
        tsm_writer.finish().await.unwrap();
        column_files.push(Arc::new(ColumnFile::new(
            tsm_sequence,
            2,
            TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
            tsm_writer.size(),
            tsm_writer.path(),
        )));
    }
    let next_file_id = 4_u64;

    let compact_req = prepare_compaction(
        tenant_database,
        opt,
        next_file_id,
        column_files,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_normal_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();

    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compaction with tombstones
#[tokio::test]
async fn test_big_compaction_2() {
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
    let data_desc = [
        // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
        (
            1_u64,
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
            2,
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
            3,
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
    let expected_data: Vec<RecordBatch> = vec![
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(1, 1000),
                generate_column_u64(1000, vec![(0, 499)]),
                generate_column_i64(1000, vec![]),
                generate_column_bool(1000, vec![]),
                generate_column_f64(1000, vec![]),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(1001, 2000),
                generate_column_u64(1000, vec![]),
                generate_column_i64(1000, vec![(0, 199)]),
                generate_column_bool(1000, vec![]),
                generate_column_f64(1000, vec![]),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema2.clone().to_record_data_schema(),
            vec![
                generate_column_ts(2001, 3000),
                generate_column_u64(1000, vec![]),
                generate_column_i64(1000, vec![]),
                generate_column_bool(1000, vec![(0, 699)]),
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
                generate_column_bool(1000, vec![(500, 999)]),
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
                generate_column_bool(1000, vec![(0, 999)]),
                generate_column_f64(1000, vec![(0, 999)]),
            ],
        )
        .unwrap(),
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
    ];
    let expected_data = HashMap::from([(1 as SeriesId, expected_data)]);

    let dir = "/tmp/test/compaction/big_2";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    if !LocalFileSystem::try_exists(&dir) {
        std::fs::create_dir_all(&dir).unwrap();
    }
    let max_level_ts = 6500;

    let mut column_files = Vec::new();
    for (tsm_sequence, args) in data_desc.into_iter() {
        let mut tsm_writer = TsmWriter::open(&dir, tsm_sequence, 0, false, Encoding::Zstd)
            .await
            .unwrap();
        for (record_batch, schema) in args.into_iter() {
            tsm_writer
                .write_record_batch(1, SeriesKey::default(), schema, record_batch)
                .await
                .unwrap();
        }
        tsm_writer.finish().await.unwrap();
        let tsm_tombstone = TsmTombstone::open(&dir, tsm_sequence).await.unwrap();
        tsm_tombstone
            .add_range(&[(1, 1)], TimeRange::new(0, 500), None)
            .await
            .unwrap();

        tsm_tombstone
            .add_range(&[(1, 2)], TimeRange::new(1001, 1200), None)
            .await
            .unwrap();

        tsm_tombstone
            .add_range(&[(1, 3)], TimeRange::new(2001, 2700), None)
            .await
            .unwrap();

        tsm_tombstone.flush().await.unwrap();
        column_files.push(Arc::new(ColumnFile::new(
            tsm_sequence,
            2,
            TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
            tsm_writer.size() as u64,
            tsm_writer.path(),
        )));
    }
    let next_file_id = 4_u64;
    let compact_req = prepare_compaction(
        tenant_database,
        opt,
        next_file_id,
        column_files,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_normal_compaction_job(compact_req, VnodeCompactionMetrics::fake())
        .await
        .unwrap()
        .unwrap();

    check_column_file(dir, version_edit, expected_data, out_level).await;
}
