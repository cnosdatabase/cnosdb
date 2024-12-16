#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use std::{mem, vec};

use config::tskv::Config;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use memory_pool::MemoryPoolRef;
use meta::error::MetaError;
use meta::model::{MetaClientRef, MetaRef};
use metrics::average::U64Average;
use metrics::count::U64Counter;
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;
use models::meta_data::{
    ExpiredBucketInfo, NodeId, ReplicationSet, ReplicationSetId, VnodeId, VnodeStatus,
};
use models::object_reference::ResolvedTable;
use models::oid::Identifier;
use models::predicate::domain::{ResolvedPredicate, ResolvedPredicateRef, TimeRange, TimeRanges};
use models::schema::resource_info::{ResourceInfo, ResourceOperator};
use models::schema::tskv_table_schema::{ColumnType, TskvTableSchemaRef};
use models::schema::{DEFAULT_CATALOG, TIME_FIELD_NAME, USAGE_SCHEMA};
use models::utils::now_timestamp_nanos;
use models::{record_batch_decode, SeriesKey, Tag};
use protocol_parser::lines_convert::{arrow_array_to_points, line_to_point};
use protocol_parser::Line;
use protos::kv_service::admin_command::Command::*;
use protos::kv_service::*;
use replication::multi_raft::MultiRaft;
use snafu::{IntoError, OptionExt, ResultExt};
use tokio::runtime::Runtime;
use trace::span_ext::SpanExt;
use trace::{debug, error, info, Span, SpanContext};
use tskv::EngineRef;
use utils::precision::{timestamp_convert, Precision};
use utils::BkdrHasher;

use crate::errors::{
    ArrowSnafu, BincodeSerdeSnafu, ColumnNotFoundSnafu, CommonSnafu, CoordinatorError,
    CoordinatorResult, FieldsIsEmptySnafu, MetaSnafu,
};
use crate::metrics::LPReporter;
use crate::raft::manager::RaftNodesManager;
use crate::raft::writer::TskvRaftWriter;
use crate::reader::table_scan::opener::TemporaryTableScanOpener;
use crate::reader::tag_scan::opener::TemporaryTagScanOpener;
use crate::reader::{CheckFuture, CheckedCoordinatorRecordBatchStream};
use crate::resource_manager::ResourceManager;
use crate::tskv_executor::{TskvAdminRequest, TskvLeaderExecutor};
use crate::{
    get_replica_all_info, get_vnode_all_info, Coordinator, QueryOption, ReplicationCmdType,
    SendableCoordinatorRecordBatchStream,
};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[derive(Clone)]
pub struct CoordService {
    node_id: u64,
    meta: MetaRef,
    config: Config,
    writer_count: Arc<AtomicUsize>,

    runtime: Arc<Runtime>,
    kv_inst: Option<EngineRef>,
    memory_pool: MemoryPoolRef,
    metrics: Arc<CoordServiceMetrics>,
    raft_manager: Arc<RaftNodesManager>,
}

#[derive(Debug)]
pub struct CoordServiceMetrics {
    coord_data_in: Metric<U64Counter>,
    coord_data_out: Metric<U64Counter>,
    coord_queries: Metric<U64Counter>,
    coord_writes: Metric<U64Counter>,

    sql_data_in: Metric<U64Counter>,
    sql_write_row: Metric<U64Counter>,
    sql_points_data_in: Metric<U64Counter>,

    write_lines_prepare: Metric<U64Average>,
    write_batch_prepare: Metric<U64Average>,
    write_replica_duration: Metric<U64Average>,
}

macro_rules! generate_coord_metrics_gets {
    ($IDENT: ident, $metrics_type: ty) => {
        impl CoordServiceMetrics {
            pub fn $IDENT(&self, tenant: &str, db: &str) -> $metrics_type {
                self.$IDENT.recorder(Self::tenant_db_labels(tenant, db))
            }
        }
    };
}
generate_coord_metrics_gets!(coord_data_in, U64Counter);
generate_coord_metrics_gets!(coord_data_out, U64Counter);
generate_coord_metrics_gets!(coord_queries, U64Counter);
generate_coord_metrics_gets!(coord_writes, U64Counter);
generate_coord_metrics_gets!(sql_data_in, U64Counter);
generate_coord_metrics_gets!(sql_write_row, U64Counter);
generate_coord_metrics_gets!(sql_points_data_in, U64Counter);
generate_coord_metrics_gets!(write_lines_prepare, U64Average);
generate_coord_metrics_gets!(write_batch_prepare, U64Average);
generate_coord_metrics_gets!(write_replica_duration, U64Average);

impl CoordServiceMetrics {
    pub fn new(register: &MetricsRegister) -> Self {
        let coord_data_in = register.metric("coord_data_in", "tenant data in");
        let coord_data_out = register.metric("coord_data_out", "tenant data out");
        let coord_writes = register.metric("coord_writes", "");
        let coord_queries = register.metric("coord_queries", "");

        let sql_data_in = register.metric("sql_data_in", "Traffic written through sql");
        let sql_write_row = register.metric("sql_write_row", "sql write row");
        let sql_points_data_in = register.metric("sql_points_data_in", "sql points data in");

        let write_lines_prepare = register.metric("write_lines_prepare", "write lines prepare");
        let write_batch_prepare = register.metric("write_batch_prepare", "write batch prepare");
        let write_replica_duration =
            register.metric("write_replica_duration", "write replica duration");

        Self {
            coord_data_in,
            coord_data_out,
            coord_writes,
            coord_queries,

            sql_data_in,
            sql_write_row,
            sql_points_data_in,
            write_lines_prepare,
            write_batch_prepare,
            write_replica_duration,
        }
    }

    pub fn tenant_db_labels<'a>(tenant: &'a str, db: &'a str) -> impl Into<Labels> + 'a {
        [("tenant", tenant), ("database", db)]
    }
}

impl CoordService {
    pub async fn new(
        runtime: Arc<Runtime>,
        kv_inst: Option<EngineRef>,
        meta: MetaRef,
        config: Config,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Arc<Self> {
        let raft_manager = Arc::new(RaftNodesManager::new(
            config.clone(),
            meta.clone(),
            kv_inst.clone(),
            metrics_register.clone(),
        ));
        RaftNodesManager::start_all_raft_node(runtime.clone(), raft_manager.clone())
            .await
            .unwrap();

        tokio::spawn(MultiRaft::raft_nodes_manager(
            raft_manager.multi_raft(),
            config.cluster.trigger_snapshot_interval,
        ));

        let coord = Arc::new(Self {
            runtime,
            kv_inst,
            memory_pool,
            raft_manager,
            meta: meta.clone(),
            config: config.clone(),
            node_id: config.global.node_id,
            metrics: Arc::new(CoordServiceMetrics::new(metrics_register.as_ref())),
            writer_count: Arc::new(AtomicUsize::new(0)),
        });

        tokio::spawn(CoordService::db_ttl_service(coord.clone()));

        if config.global.pre_create_bucket {
            tokio::spawn(CoordService::pre_create_bucket_service(coord.clone()));
        }

        if config.global.store_metrics {
            tokio::spawn(CoordService::metrics_service(
                coord.clone(),
                metrics_register,
            ));
        }

        coord
    }

    async fn db_ttl_service(coord: Arc<CoordService>) {
        loop {
            let dur = tokio::time::Duration::from_secs(60);
            tokio::time::sleep(dur).await;

            let expired = coord.meta.expired_bucket().await;
            for info in expired.iter() {
                let result = coord.delete_expired_bucket(info).await;

                info!("delete expired bucket :{:?}, {:?}", info, result);
            }
        }
    }

    async fn pre_create_bucket_service(coord: Arc<CoordService>) {
        loop {
            let interval = 5 * 60;
            let dur = tokio::time::Duration::from_secs(interval);
            tokio::time::sleep(dur).await;

            let now = now_timestamp_nanos() + (interval * 1_000_000_000) as i64;
            let per_create = coord.meta.pre_create_bucket(now).await;

            for item in per_create {
                if let Some(tenant_meta) = coord.meta.tenant_meta(&item.tenant).await {
                    if let Ok(bucket) = tenant_meta.create_bucket(&item.database, item.ts).await {
                        for replica_set in bucket.shard_group {
                            let command = AdminCommand {
                                tenant: item.tenant.clone(),
                                command: Some(BuildRaftGroup(BuildRaftGroupRequest {
                                    db_name: item.database.clone(),
                                    replica_id: replica_set.id,
                                })),
                            };

                            let result = coord
                                .admin_command_on_node(replica_set.leader_node_id, command)
                                .await;
                            info!(
                                "pre-create bucekt info: {:?}, replica-set: {:?}, result: {:?}",
                                item, replica_set, result
                            );
                        }
                    }
                }
            }
        }
    }

    async fn metrics_service(
        coord: Arc<CoordService>,
        root_metrics_register: Arc<MetricsRegister>,
    ) {
        let start = tokio::time::Instant::now() + Duration::from_secs(10);
        let interval = Duration::from_secs(10);
        let mut intv = tokio::time::interval_at(start, interval);
        loop {
            intv.tick().await;
            let mut lines_buffer = Vec::new();
            let mut reporter = LPReporter::new(&mut lines_buffer);
            root_metrics_register.report(&mut reporter);

            let lines = lines_buffer.iter().map(|l| l.to_line()).collect::<Vec<_>>();
            if let Err(e) = coord
                .write_lines(DEFAULT_CATALOG, USAGE_SCHEMA, Precision::NS, lines, None)
                .await
            {
                error!("write metrics to {DEFAULT_CATALOG} fail. {e}")
            }
        }
    }

    async fn delete_expired_bucket(&self, info: &ExpiredBucketInfo) -> CoordinatorResult<()> {
        for repl_set in info.bucket.shard_group.iter() {
            if repl_set.leader_node_id == self.node_id {
                self.raft_manager()
                    .destory_replica_group(&info.tenant, &info.database, repl_set.id)
                    .await?;
            } else {
                info!("Not the leader node for group: {} ignore...", repl_set.id);
            }
        }

        let meta = self.tenant_meta(&info.tenant).await.ok_or_else(|| {
            CoordinatorError::TenantNotFound {
                name: info.tenant.clone(),
            }
        })?;

        meta.delete_bucket(&info.database, info.bucket.id)
            .await
            .context(MetaSnafu)?;

        Ok(())
    }

    async fn prune_shards(
        &self,
        tenant: &str,
        database: &str,
        time_ranges: &TimeRanges,
    ) -> Result<Vec<ReplicationSet>, CoordinatorError> {
        let meta = self
            .meta_manager()
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            })?;
        let buckets = meta
            .mapping_bucket(database, time_ranges.min_ts(), time_ranges.max_ts())
            .context(MetaSnafu)?;
        let shards = buckets.into_iter().flat_map(|b| b.shard_group).collect();

        Ok(shards)
    }

    fn build_query_checker(&self, tenant: &str) -> CheckFuture {
        let tenant = tenant.to_string();
        let meta = self.meta.clone();

        let checker = async move {
            meta.limiter(&tenant)
                .await
                .context(MetaSnafu)?
                .check_coord_queries()
                .await
                .context(MetaSnafu)
        };

        Box::pin(checker)
    }

    async fn vnode_checksum_on_node(
        &self,
        tenant: &str,
        node_id: NodeId,
        vnode_id: VnodeId,
    ) -> CoordinatorResult<RecordBatch> {
        let request = AdminCommand {
            tenant: tenant.to_string(),
            command: Some(admin_command::Command::FetchChecksum(
                FetchChecksumRequest { vnode_id },
            )),
        };

        let data = self.admin_command_on_node(node_id, request).await?;
        match record_batch_decode(&data) {
            Ok(r) => Ok(r),
            Err(e) => Err(ArrowSnafu.into_error(e)),
        }
    }

    async fn push_points_to_requests<'a>(
        &'a self,
        tenant: &'a str,
        db: &'a str,
        precision: Precision,
        info: ReplicationSet,
        points: Arc<Vec<u8>>,
        span_ctx: Option<&'a SpanContext>,
    ) -> CoordinatorResult<Vec<impl Future<Output = CoordinatorResult<()>> + Sized + 'a>> {
        {
            let _span = Span::from_context("limit check", span_ctx);

            let limiter = self.meta.limiter(tenant).await.context(MetaSnafu)?;
            let write_size = points.len();

            limiter.check_coord_writes().await.context(MetaSnafu)?;
            limiter
                .check_coord_data_in(write_size)
                .await
                .context(MetaSnafu)?;

            self.metrics.coord_writes(tenant, db).inc_one();
            self.metrics
                .coord_data_in(tenant, db)
                .inc(write_size as u64);
        }
        if info.vnodes.is_empty() {
            return Err(CommonSnafu {
                msg: "no available vnode in replication set".to_string(),
            }
            .build());
        }

        let mut requests: Vec<Pin<Box<dyn Future<Output = Result<(), CoordinatorError>> + Send>>> =
            Vec::new();
        let request = WriteDataRequest {
            precision: precision as u32,
            data: Arc::unwrap_or_clone(points),
        };
        let request = RaftWriteCommand {
            replica_id: info.id,
            db_name: db.to_string(),
            tenant: tenant.to_string(),

            command: Some(raft_write_command::Command::WriteData(request)),
        };

        let request = self.write_replica_by_raft(info.clone(), request, span_ctx);
        requests.push(Box::pin(request));

        Ok(requests)
    }

    async fn admin_command_on_leader(
        &self,
        replica: ReplicationSet,
        request: AdminCommand,
    ) -> CoordinatorResult<()> {
        let tenant = request.tenant.clone();
        let caller = TskvAdminRequest {
            request,
            meta: self.meta.clone(),
            timeout: Duration::from_secs(3600),
            enable_gzip: self.config.service.grpc_enable_gzip,
        };
        let executor = TskvLeaderExecutor {
            meta: self.meta.clone(),
        };
        executor.do_request(&tenant, &replica, &caller).await?;
        Ok(())
    }

    pub async fn admin_command_on_node(
        &self,
        node_id: u64,
        request: AdminCommand,
    ) -> CoordinatorResult<Vec<u8>> {
        let caller = TskvAdminRequest {
            request,
            meta: self.meta.clone(),
            timeout: Duration::from_secs(3600),
            enable_gzip: self.config.service.grpc_enable_gzip,
        };

        caller.do_request(node_id).await
    }

    async fn check_remove_vnode_and_promote(
        &self,
        tenant: &str,
        db_name: &str,
        replica_id: ReplicationSetId,
        vnode_id: VnodeId,
    ) -> CoordinatorResult<()> {
        let meta = self.meta.clone();
        let replica_set = crate::get_replica_by_meta(meta, tenant, db_name, replica_id).await?;
        if replica_set.leader_vnode_id == vnode_id {
            let mut tmp_vnodes = replica_set.vnodes.clone();
            tmp_vnodes.retain(|x| x.id != vnode_id);
            if tmp_vnodes.is_empty() {
                return Err(CoordinatorError::ReplicaCannotRemove { replica_id });
            }

            let new_leader = tmp_vnodes[0].id;
            let cmd_type = ReplicationCmdType::PromoteLeader(replica_id, new_leader);
            self.replication_manager(tenant, cmd_type).await?;
        }

        Ok(())
    }
}

//***************************** Coordinator Interface ***************************************** */
#[async_trait::async_trait]
impl Coordinator for CoordService {
    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn meta_manager(&self) -> MetaRef {
        self.meta.clone()
    }

    fn store_engine(&self) -> Option<EngineRef> {
        self.kv_inst.clone()
    }

    fn raft_manager(&self) -> Arc<RaftNodesManager> {
        self.raft_manager.clone()
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        self.meta.tenant_meta(tenant).await
    }

    fn tskv_raft_writer(&self, request: RaftWriteCommand) -> TskvRaftWriter {
        TskvRaftWriter::new(
            self.meta.clone(),
            self.node_id,
            self.config.query.write_timeout,
            self.config.service.grpc_enable_gzip,
            self.config.deployment.memory * 1024 * 1024 * 1024,
            self.memory_pool.clone(),
            self.raft_manager.clone(),
            request,
            self.writer_count.clone(),
        )
    }

    async fn table_vnodes(
        &self,
        table: &ResolvedTable,
        predicate: ResolvedPredicateRef,
    ) -> CoordinatorResult<Vec<ReplicationSet>> {
        // 1. 根据传入的过滤条件获取表的分片信息（包括副本）
        let mut replica_sets = self
            .prune_shards(
                table.tenant(),
                table.database(),
                predicate.time_ranges().as_ref(),
            )
            .await?;

        // 2. 选择最优的副本
        for replica_set in replica_sets.iter_mut() {
            replica_set.vnodes.sort_by_key(|vnode| {
                // The smaller the score, the easier it is to be selected
                if vnode.id == replica_set.leader_vnode_id {
                    0
                } else {
                    match vnode.status {
                        VnodeStatus::Running => 1,
                        VnodeStatus::Copying => 2,
                        VnodeStatus::Broken => i32::MAX,
                    }
                }
            });

            replica_set
                .vnodes
                .retain(|e| e.status != VnodeStatus::Broken);

            replica_set.vnodes.truncate(2);
        }

        Ok(replica_sets)
    }

    async fn write_replica_by_raft(
        &self,
        replica: ReplicationSet,
        request: RaftWriteCommand,
        _span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let tenant = request.tenant.clone();
        let writer = self.tskv_raft_writer(request);
        let executor = TskvLeaderExecutor {
            meta: self.meta.clone(),
        };

        executor.do_request(&tenant, &replica, &writer).await?;

        Ok(())
    }

    async fn write_lines<'a>(
        &self,
        tenant: &str,
        db: &str,
        precision: Precision,
        lines: Vec<Line<'a>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize> {
        let pre_write_start = std::time::Instant::now();
        let mut write_bytes: usize = 0;
        let meta_client = self.meta.tenant_meta(tenant).await.ok_or_else(|| {
            CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            }
        })?;
        let mut map_lines: HashMap<ReplicationSetId, VnodeLines> = HashMap::new();
        let db_schema = meta_client
            .get_db_schema(db)
            .context(MetaSnafu)?
            .ok_or_else(|| MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
            .context(MetaSnafu)?;
        if db_schema.is_hidden() {
            return Err(CoordinatorError::Meta {
                source: MetaError::DatabaseNotFound {
                    database: db.to_string(),
                },
            });
        }

        let db_precision = db_schema.config.precision();
        for line in lines {
            let ts =
                timestamp_convert(precision, *db_precision, line.timestamp).ok_or_else(|| {
                    CommonSnafu {
                        msg: "timestamp overflow".to_string(),
                    }
                    .build()
                })?;
            let info = meta_client
                .locate_replication_set_for_write(db, line.hash_id, ts)
                .await
                .context(MetaSnafu)?;
            let lines_entry = map_lines.entry(info.id).or_insert(VnodeLines::new(info));
            lines_entry.add_line(line)
        }

        let mut requests = Vec::new();
        for lines in map_lines.into_values() {
            let points = Arc::new(line_to_point(&lines.lines, db).map_err(|_| {
                CommonSnafu {
                    msg: "line to point error".to_string(),
                }
                .build()
            })?);
            write_bytes += points.len();
            requests.extend(
                self.push_points_to_requests(tenant, db, precision, lines.info, points, span_ctx)
                    .await?,
            );
        }

        self.metrics
            .write_lines_prepare(tenant, db)
            .add(pre_write_start.elapsed().as_millis() as u64);

        let now = tokio::time::Instant::now();
        for res in futures::future::join_all(requests).await {
            debug!(
                "Parallel write points on vnode over, start at: {:?}, elapsed: {} millis, result: {:?}",
                now,
                now.elapsed().as_millis(),
                res
            );
            res?
        }
        self.metrics
            .write_replica_duration(tenant, db)
            .add(now.elapsed().as_millis() as u64);

        Ok(write_bytes)
    }

    async fn write_record_batch<'a>(
        &self,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
        db_precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize> {
        let pre_write_start = std::time::Instant::now();

        let mut write_bytes: usize = 0;
        let mut precision = Precision::NS;
        let tenant = table_schema.tenant.as_str();
        let db = table_schema.db.as_str();
        let meta_client = self.meta.tenant_meta(tenant).await.ok_or_else(|| {
            CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            }
        })?;

        let mut repl_idx: HashMap<ReplicationSet, Vec<u32>> = HashMap::new();
        let schema = record_batch.schema().fields.clone();
        let table_name = table_schema.name.as_str();
        let columns = record_batch.columns();
        for idx in 0..record_batch.num_rows() {
            let mut hasher = BkdrHasher::new();
            hasher.hash_with(table_name.as_bytes());
            let mut ts = i64::MAX;
            let mut has_ts = false;
            let mut has_fileds = false;
            for (column, schema) in columns.iter().zip(schema.iter()) {
                let name = schema.name().as_str();
                let tskv_schema_column = table_schema.column(name).ok_or_else(|| {
                    CommonSnafu {
                        msg: format!("column {} not found in table {}", name, table_name),
                    }
                    .build()
                })?;
                if name == TIME_FIELD_NAME {
                    let precsion_and_value =
                        get_precision_and_value_from_arrow_column(column, idx)?;
                    precision = precsion_and_value.0;
                    ts = timestamp_convert(precision, db_precision, precsion_and_value.1)
                        .ok_or_else(|| {
                            CommonSnafu {
                                msg: "timestamp overflow".to_string(),
                            }
                            .build()
                        })?;
                    has_ts = true;
                }
                if matches!(tskv_schema_column.column_type, ColumnType::Tag) {
                    let value = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            CommonSnafu {
                                msg: format!("column {} is not StringArray", name),
                            }
                            .build()
                        })?
                        .value(idx);
                    hasher.hash_with(name.as_bytes());
                    hasher.hash_with(value.as_bytes());
                }

                if let ColumnType::Field(_) = tskv_schema_column.column_type {
                    if !column.is_null(idx) {
                        has_fileds = true;
                    }
                }
            }

            if !has_ts {
                return Err(CommonSnafu {
                    msg: format!(
                        "column {} not found in table {}",
                        TIME_FIELD_NAME, table_name
                    ),
                }
                .build());
            }

            if !has_fileds {
                return Err(FieldsIsEmptySnafu.build());
            }

            let hash = hasher.number();
            let info = meta_client
                .locate_replication_set_for_write(db, hash, ts)
                .await
                .context(MetaSnafu)?;
            repl_idx.entry(info).or_default().push(idx as u32);
        }

        let mut requests = Vec::new();
        for (repl, idxs) in repl_idx {
            let indices = UInt32Array::from(idxs);
            let columns = record_batch
                .columns()
                .iter()
                .map(|column| {
                    take(column, &indices, None).map_err(|e| {
                        CommonSnafu {
                            msg: format!("take column error: {}", e),
                        }
                        .build()
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = record_batch.schema();
            let points = Arc::new(
                arrow_array_to_points(columns, schema, table_schema.clone(), indices.len())
                    .map_err(|e| {
                        CommonSnafu {
                            msg: format!("arrow array to points error: {}", e),
                        }
                        .build()
                    })?,
            );
            write_bytes += points.len();
            requests.extend(
                self.push_points_to_requests(tenant, db, precision, repl, points, span_ctx)
                    .await?,
            );
        }
        self.metrics
            .write_lines_prepare(tenant, db)
            .add(pre_write_start.elapsed().as_millis() as u64);

        let now = tokio::time::Instant::now();
        for res in futures::future::join_all(requests).await {
            debug!(
                "Parallel write points on vnode over, start at: {:?}, elapsed: {} millis, result: {:?}",
                now,
                now.elapsed().as_millis(),
                res
            );
            res?
        }
        self.metrics
            .write_replica_duration(tenant, db)
            .add(now.elapsed().as_millis() as u64);

        Ok(write_bytes)
    }

    fn table_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let checker = self.build_query_checker(&option.table_schema.tenant);

        let opener = TemporaryTableScanOpener::new(
            self.config.query.clone(),
            self.kv_inst.clone(),
            self.runtime.clone(),
            self.meta.clone(),
            span_ctx,
            self.config.service.grpc_enable_gzip,
        );

        Ok(Box::pin(CheckedCoordinatorRecordBatchStream::new(
            option,
            opener,
            self.meta.clone(),
            Box::pin(checker),
            &self.metrics,
        )))
    }

    fn tag_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let checker = self.build_query_checker(&option.table_schema.tenant);

        let opener = TemporaryTagScanOpener::new(
            self.config.query.clone(),
            self.kv_inst.clone(),
            self.meta.clone(),
            span_ctx,
            self.config.service.grpc_enable_gzip,
        );

        Ok(Box::pin(CheckedCoordinatorRecordBatchStream::new(
            option,
            opener,
            self.meta.clone(),
            Box::pin(checker),
            &self.metrics,
        )))
    }

    async fn delete_from_table(
        &self,
        table: &ResolvedTable,
        predicate: &ResolvedPredicate,
    ) -> CoordinatorResult<()> {
        let replicas = self
            .prune_shards(
                table.tenant(),
                table.database(),
                predicate.time_ranges().as_ref(),
            )
            .await?;

        let now = tokio::time::Instant::now();
        let mut requests = vec![];
        let predicate_bytes = bincode::serialize(predicate).context(BincodeSerdeSnafu)?;
        for replica in replicas.iter() {
            let request = DeleteFromTableRequest {
                tenant: table.tenant().to_string(),
                database: table.database().to_string(),
                table: table.table().to_string(),
                predicate: predicate_bytes.clone(),
                vnode_id: 0,
            };
            let command = RaftWriteCommand {
                replica_id: replica.id,
                tenant: table.tenant().to_string(),
                db_name: table.database().to_string(),
                command: Some(raft_write_command::Command::DeleteFromTable(request)),
            };

            let request = self.write_replica_by_raft(replica.clone(), command, None);
            requests.push(request);
        }

        for result in futures::future::join_all(requests).await {
            debug!("exec delete from {table} WHERE {predicate:?}, now:{now:?}, elapsed:{}ms, result:{result:?}", now.elapsed().as_millis());
            result?
        }

        Ok(())
    }

    async fn replication_manager(
        &self,
        tenant: &str,
        cmd_type: ReplicationCmdType,
    ) -> CoordinatorResult<()> {
        let (request, replica) = match cmd_type {
            ReplicationCmdType::AddRaftFollower(replica_id, node_id) => {
                let replica = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
                if replica.replica_set.by_node_id(node_id).is_some() {
                    return Err(CommonSnafu {
                        msg: format!("A Replication Already in {}", node_id),
                    }
                    .build());
                }
                (
                    AdminCommand {
                        tenant: tenant.to_string(),
                        command: Some(AddRaftFollower(AddRaftFollowerRequest {
                            db_name: replica.db_name,
                            replica_id: replica.replica_set.id,
                            follower_nid: node_id,
                        })),
                    },
                    replica.replica_set,
                )
            }

            ReplicationCmdType::RemoveRaftNode(vnode_id) => {
                let all_info = get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
                let replica_id = all_info.repl_set_id;
                self.check_remove_vnode_and_promote(
                    tenant,
                    &all_info.db_name,
                    replica_id,
                    vnode_id,
                )
                .await?;

                let replica = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
                (
                    AdminCommand {
                        tenant: tenant.to_string(),
                        command: Some(RemoveRaftNode(RemoveRaftNodeRequest {
                            vnode_id,
                            replica_id,
                            db_name: all_info.db_name,
                        })),
                    },
                    replica.replica_set,
                )
            }

            ReplicationCmdType::DestoryRaftGroup(replica_id) => {
                let replica = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
                (
                    AdminCommand {
                        tenant: tenant.to_string(),
                        command: Some(DestoryRaftGroup(DestoryRaftGroupRequest {
                            replica_id,
                            db_name: replica.db_name.clone(),
                        })),
                    },
                    replica.replica_set,
                )
            }

            ReplicationCmdType::PromoteLeader(replica_id, new_leader) => {
                let replica = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
                (
                    AdminCommand {
                        tenant: tenant.to_string(),
                        command: Some(PromoteLeader(PromoteLeaderRequest {
                            replica_id,
                            new_leader_id: new_leader,
                            db_name: replica.db_name.clone(),
                        })),
                    },
                    replica.replica_set,
                )
            }
        };

        self.admin_command_on_leader(replica, request).await
    }

    async fn compact_vnodes(&self, tenant: &str, vnode_ids: Vec<VnodeId>) -> CoordinatorResult<()> {
        // Group vnode ids by node id.
        let mut node_vnode_ids_map: HashMap<u64, Vec<u32>> = HashMap::new();
        for vnode_id in vnode_ids.iter() {
            let vnode = get_vnode_all_info(self.meta.clone(), tenant, *vnode_id).await?;
            node_vnode_ids_map
                .entry(vnode.node_id)
                .or_default()
                .push(*vnode_id);
        }
        let nodes = self.meta.data_nodes().await;

        // Send grouped vnode ids to nodes.
        let mut req_futures = vec![];
        for node in nodes {
            if let Some(vnode_ids) = node_vnode_ids_map.remove(&node.id) {
                let cmd = AdminCommand {
                    tenant: tenant.to_string(),
                    command: Some(CompactVnode(CompactVnodeRequest { vnode_ids })),
                };
                req_futures.push(self.admin_command_on_node(node.id, cmd));
            }
        }

        for res in futures::future::join_all(req_futures).await {
            res?;
        }

        return Ok(());
    }

    async fn replica_checksum(
        &self,
        tenant: &str,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<Vec<RecordBatch>> {
        let replica = get_replica_all_info(self.meta.clone(), tenant, replica_id)
            .await?
            .replica_set;

        // Group vnode ids by node id.
        let mut node_vnode_ids_map: HashMap<u64, Vec<u32>> = HashMap::new();
        for vnode in replica.vnodes {
            node_vnode_ids_map
                .entry(vnode.node_id)
                .or_default()
                .push(vnode.id);
        }

        let nodes = self.meta.data_nodes().await;

        // Send grouped vnode ids to nodes.
        let mut req_futures = vec![];
        for node in nodes {
            if let Some(vnode_ids) = node_vnode_ids_map.remove(&node.id) {
                for vnode_id in vnode_ids {
                    req_futures.push(self.vnode_checksum_on_node(tenant, node.id, vnode_id));
                }
            }
        }
        let record_batches = futures::future::try_join_all(req_futures).await?;

        Ok(record_batches)
    }

    fn metrics(&self) -> &Arc<CoordServiceMetrics> {
        &self.metrics
    }

    async fn update_tags_value(
        &self,
        table_schema: TskvTableSchemaRef,
        mut new_tags: Vec<UpdateSetValue>,
        record_batches: Vec<RecordBatch>,
    ) -> CoordinatorResult<()> {
        let tenant = &table_schema.tenant;
        let db = &table_schema.db;
        let table_name = &table_schema.name;

        let tenant_meta = self.meta.tenant_meta(tenant).await.ok_or_else(|| {
            CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            }
        })?;

        let mut series_keys = vec![];
        for new_tag in new_tags.iter_mut() {
            let key = mem::take(&mut new_tag.key);
            let tag_name = unsafe { String::from_utf8_unchecked(key) };

            let id = table_schema
                .column(&tag_name)
                .context(ColumnNotFoundSnafu { name: tag_name })?
                .id;
            new_tag.key = format!("{id}").into_bytes();
        }

        for record_batch in record_batches {
            let num_rows = record_batch.num_rows();
            let schema = record_batch.schema().fields().clone();
            let columns = record_batch.columns();

            // struct SeriesKey
            for idx in 0..num_rows {
                let mut tags = vec![];
                for (column, schema) in columns.iter().zip(schema.iter()) {
                    let name = schema.name().as_str();
                    let tskv_schema_column = table_schema.column(name).ok_or_else(|| {
                        CommonSnafu {
                            msg: format!("column {} not found in table {}", name, table_name),
                        }
                        .build()
                    })?;

                    if matches!(tskv_schema_column.column_type, ColumnType::Tag) {
                        let value = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                CommonSnafu {
                                    msg: format!("column {} is not string", name),
                                }
                                .build()
                            })?
                            .value(idx);

                        // match_series can`t have null tag
                        if value.is_empty() {
                            if let Some(null) = column.nulls() {
                                if null.is_null(idx) {
                                    continue;
                                }
                            }
                        }
                        tags.push(Tag::new_with_column_id(
                            tskv_schema_column.id,
                            value.as_bytes().to_vec(),
                        ));
                    }
                }

                series_keys.push(
                    SeriesKey {
                        tags,
                        table: table_name.clone(),
                    }
                    .encode(),
                );
            }
        }

        // find all shard/ReplicationSet/node_id
        // send only one request to each kv node
        let time_ranges = TimeRanges::new(vec![TimeRange::all()]);
        let shards = self.prune_shards(tenant, db, &time_ranges).await?;

        let update_tags_request = UpdateTagsRequest {
            db: db.to_string(),
            new_tags: new_tags.clone(),
            matched_series: series_keys.to_vec(),
            dry_run: true,
        };

        let mut requests = vec![];
        for replica in shards.iter() {
            let command = RaftWriteCommand {
                replica_id: replica.id,
                tenant: tenant.to_string(),
                db_name: db.to_string(),
                command: Some(raft_write_command::Command::UpdateTags(
                    update_tags_request.clone(),
                )),
            };

            let request = self.write_replica_by_raft(replica.clone(), command, None);
            requests.push(request);
        }

        for result in futures::future::join_all(requests).await {
            result?
        }

        let new_tags_vec: Vec<(Vec<u8>, Option<Vec<u8>>)> = new_tags
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect();

        let resourceinfo = ResourceInfo::new(
            (*tenant_meta.tenant().id(), db.to_string()),
            tenant.to_string() + "-" + db + "-" + table_name + "-" + "UpdateTagsValue",
            ResourceOperator::UpdateTagValue(
                tenant.to_string(),
                db.to_string(),
                new_tags_vec,
                series_keys,
                shards,
            ),
            &None,
            self.node_id,
        );
        ResourceManager::add_resource_task(Arc::new(self.clone()), resourceinfo).await?;

        Ok(())
    }

    fn get_config(&self) -> Config {
        self.config.clone()
    }

    fn get_writer_count(&self) -> Arc<AtomicUsize> {
        self.writer_count.clone()
    }
}

struct VnodeLines<'a> {
    pub lines: Vec<Line<'a>>,
    pub info: ReplicationSet,
}

impl<'a> VnodeLines<'a> {
    pub fn new(info: ReplicationSet) -> Self {
        Self {
            lines: vec![],
            info,
        }
    }

    pub fn add_line(&mut self, line: Line<'a>) {
        self.lines.push(line);
    }
}

fn get_precision_and_value_from_arrow_column(
    column: &ArrayRef,
    idx: usize,
) -> CoordinatorResult<(Precision, i64)> {
    match column.data_type() {
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => Err(CommonSnafu {
                msg: "time field not support second".to_string(),
            }
            .build()),
            TimeUnit::Millisecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        CommonSnafu {
                            msg: "time field data type miss match: millisecond".to_string(),
                        }
                        .build()
                    })?
                    .value(idx);
                Ok((Precision::MS, value))
            }
            TimeUnit::Microsecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        CommonSnafu {
                            msg: "time field data type miss match: microsecond".to_string(),
                        }
                        .build()
                    })?
                    .value(idx);
                Ok((Precision::US, value))
            }
            TimeUnit::Nanosecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        CommonSnafu {
                            msg: "time field data type miss match: nanosecond".to_string(),
                        }
                        .build()
                    })?
                    .value(idx);
                Ok((Precision::NS, value))
            }
        },
        DataType::Int64 => {
            let value = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    CommonSnafu {
                        msg: "time field data type miss match: int64".to_string(),
                    }
                    .build()
                })?
                .value(idx);
            Ok((Precision::NS, value))
        }
        _ => Err(CommonSnafu {
            msg: "time field data type miss match".to_string(),
        }
        .build()),
    }
}
