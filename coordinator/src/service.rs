#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{mem, vec};

use config::Config;
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
use metrics::count::U64Counter;
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;
use models::meta_data::{
    ExpiredBucketInfo, MetaModifyType, NodeId, ReplicationSet, ReplicationSetId, VnodeId,
    VnodeStatus,
};
use models::object_reference::ResolvedTable;
use models::oid::Identifier;
use models::predicate::domain::{ResolvedPredicate, ResolvedPredicateRef, TimeRange, TimeRanges};
use models::schema::{
    timestamp_convert, ColumnType, Precision, ResourceInfo, ResourceOperator, ResourceStatus,
    TskvTableSchemaRef, DEFAULT_CATALOG, TIME_FIELD, USAGE_SCHEMA,
};
use models::utils::now_timestamp_nanos;
use models::{record_batch_decode, SeriesKey, Tag};
use protocol_parser::lines_convert::{
    arrow_array_to_points, line_to_batches, mutable_batches_to_point,
};
use protocol_parser::Line;
use protos::kv_service::admin_command::Command::*;
use protos::kv_service::*;
use replication::multi_raft::MultiRaft;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use trace::{debug, error, info, SpanContext, SpanExt, SpanRecorder};
use tskv::{EngineRef, TskvError};
use utils::BkdrHasher;

use crate::errors::*;
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

    runtime: Arc<Runtime>,
    kv_inst: Option<EngineRef>,
    memory_pool: MemoryPoolRef,
    metrics: Arc<CoordServiceMetrics>,
    raft_manager: Arc<RaftNodesManager>,
    async_task_joinhandle: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    failed_task_joinhandle: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
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
}

macro_rules! generate_coord_metrics_gets {
    ($IDENT: ident) => {
        impl CoordServiceMetrics {
            pub fn $IDENT(&self, tenant: &str, db: &str) -> U64Counter {
                self.$IDENT.recorder(Self::tenant_db_labels(tenant, db))
            }
        }
    };
}
generate_coord_metrics_gets!(coord_data_in);
generate_coord_metrics_gets!(coord_data_out);
generate_coord_metrics_gets!(coord_queries);
generate_coord_metrics_gets!(coord_writes);
generate_coord_metrics_gets!(sql_data_in);
generate_coord_metrics_gets!(sql_write_row);
generate_coord_metrics_gets!(sql_points_data_in);

impl CoordServiceMetrics {
    pub fn new(register: &MetricsRegister) -> Self {
        let coord_data_in = register.metric("coord_data_in", "tenant data in");
        let coord_data_out = register.metric("coord_data_out", "tenant data out");
        let coord_writes = register.metric("coord_writes", "");
        let coord_queries = register.metric("coord_queries", "");

        let sql_data_in = register.metric("sql_data_in", "Traffic written through sql");
        let sql_write_row = register.metric("sql_write_row", "sql write row");
        let sql_points_data_in = register.metric("sql_points_data_in", "sql points data in");

        Self {
            coord_data_in,
            coord_data_out,
            coord_writes,
            coord_queries,

            sql_data_in,
            sql_write_row,
            sql_points_data_in,
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
        ));
        raft_manager.start_all_raft_node().await.unwrap();

        tokio::spawn(MultiRaft::trigger_snapshot_purge_logs(
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
            async_task_joinhandle: Arc::new(Mutex::new(HashMap::new())),
            failed_task_joinhandle: Arc::new(Mutex::new(HashMap::new())),
            node_id: config.global.node_id,
            metrics: Arc::new(CoordServiceMetrics::new(metrics_register.as_ref())),
        });

        let meta_task_receiver = coord
            .meta_manager()
            .take_resourceinfo_rx()
            .expect("meta resource channel only has one consumer");
        tokio::spawn(CoordService::recv_meta_modify(
            coord.clone(),
            meta_task_receiver,
        ));
        tokio::spawn(CoordService::db_ttl_service(coord.clone()));

        if config.global.store_metrics {
            tokio::spawn(CoordService::metrics_service(
                coord.clone(),
                metrics_register,
            ));
        }

        coord
    }

    async fn recv_meta_modify(coord: Arc<CoordService>, mut receiver: Receiver<MetaModifyType>) {
        while let Some(modify_data) = receiver.recv().await {
            // if error, max retry count 10
            let _ = Retry::spawn(
                ExponentialBackoff::from_millis(10).map(jitter).take(10),
                || async {
                    let res =
                        CoordService::handle_meta_modify(coord.clone(), modify_data.clone()).await;
                    if let Err(e) = &res {
                        error!("handle meta modify error: {}, retry later", e.to_string());
                    }
                    res
                },
            )
            .await;
        }
    }

    async fn handle_meta_modify(
        coord: Arc<CoordService>,
        modify_data: MetaModifyType,
    ) -> CoordinatorResult<()> {
        match modify_data {
            MetaModifyType::ResourceInfo(resourceinfo) => {
                if !resourceinfo.get_is_new_add() {
                    return Ok(()); // ignore the old task
                }

                // if current node get the lock, handle meta modify
                let (id, lock) = coord
                    .meta_manager()
                    .read_resourceinfos_mark()
                    .await
                    .map_err(|meta_err| CoordinatorError::Meta { source: meta_err })?;
                if id == coord.node_id() && lock {
                    match *resourceinfo.get_status() {
                        ResourceStatus::Schedule => {
                            if let Ok(mut joinhandle_map) = coord.async_task_joinhandle.lock() {
                                if let Some(handle) = joinhandle_map.get(resourceinfo.get_name()) {
                                    handle.abort(); // same resource name, abort the old task
                                }
                                joinhandle_map.insert(
                                    resourceinfo.get_name().to_string(),
                                    tokio::spawn(CoordService::exec_async_task(
                                        coord.clone(),
                                        *resourceinfo,
                                    )),
                                );
                            }
                        }
                        ResourceStatus::Failed => {
                            if let Ok(mut joinhandle_map) = coord.failed_task_joinhandle.lock() {
                                if joinhandle_map.contains_key(resourceinfo.get_name()) {
                                    return Ok(()); // ignore repetition failed task
                                }
                                let coord = coord.clone();
                                joinhandle_map.insert(
                                    resourceinfo.get_name().to_string(),
                                    tokio::spawn(ResourceManager::retry_failed_task(
                                        coord,
                                        *resourceinfo,
                                    )),
                                );
                            }
                        }
                        ResourceStatus::Cancel => {
                            if let Ok(mut joinhandle_map) = coord.async_task_joinhandle.lock() {
                                if let Some(handle) = joinhandle_map.get(resourceinfo.get_name()) {
                                    handle.abort(); // abort task
                                }
                                joinhandle_map.remove(resourceinfo.get_name()); // remove task
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            }
            MetaModifyType::NodeMetrics(node_metrics) => {
                // if lock node dead, grap lock again
                let (id, lock) = coord
                    .meta_manager()
                    .read_resourceinfos_mark()
                    .await
                    .map_err(|meta_err| CoordinatorError::Meta { source: meta_err })?;
                if node_metrics.id == id && lock {
                    // unlock the dead node
                    if let Err(e) = coord
                        .meta_manager()
                        .write_resourceinfos_mark(node_metrics.id, false)
                        .await
                    {
                        match e {
                            MetaError::ResourceInfosMarkIsLock { .. } => {
                                return Ok(());
                            }
                            _ => {
                                return Err(CoordinatorError::Meta { source: e });
                            }
                        }
                    }

                    // grap lock again
                    if let Err(e) = coord
                        .meta_manager()
                        .write_resourceinfos_mark(coord.node_id(), true)
                        .await
                    {
                        match e {
                            MetaError::ResourceInfosMarkIsLock { .. } => {
                                return Ok(());
                            }
                            _ => {
                                return Err(CoordinatorError::Meta { source: e });
                            }
                        }
                    }
                }

                // if current node get the lock, get the dead node task
                let (id, lock) = coord
                    .meta_manager()
                    .read_resourceinfos_mark()
                    .await
                    .map_err(|meta_err| CoordinatorError::Meta { source: meta_err })?;
                if coord.node_id() == id && lock {
                    let mut resourceinfos = coord
                        .meta_manager()
                        .read_resourceinfos()
                        .await
                        .map_err(|meta_err| CoordinatorError::Meta { source: meta_err })?;
                    // find the dead node task
                    resourceinfos.retain(|info| *info.get_execute_node_id() == node_metrics.id);
                    for mut resourceinfo in resourceinfos {
                        let coord = coord.clone();
                        resourceinfo.set_execute_node_id(coord.node_id());
                        resourceinfo.set_is_new_add(false);
                        coord
                            .meta_manager()
                            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
                            .await?;
                        match *resourceinfo.get_status() {
                            ResourceStatus::Schedule => {
                                if let Ok(mut joinhandle_map) = coord.async_task_joinhandle.lock() {
                                    if joinhandle_map.contains_key(resourceinfo.get_name()) {
                                        return Ok(()); // ignore the dead node task
                                    }

                                    joinhandle_map.insert(
                                        resourceinfo.get_name().to_string(),
                                        tokio::spawn(CoordService::exec_async_task(
                                            coord.clone(),
                                            resourceinfo,
                                        )),
                                    );
                                }
                            }
                            ResourceStatus::Executing => {
                                let _ =
                                    ResourceManager::add_resource_task(coord, resourceinfo).await;
                            }
                            ResourceStatus::Failed => {
                                if let Ok(mut joinhandle_map) = coord.failed_task_joinhandle.lock()
                                {
                                    if joinhandle_map.contains_key(resourceinfo.get_name()) {
                                        return Ok(()); // ignore repetition failed task
                                    }
                                    let coord = coord.clone();
                                    joinhandle_map.insert(
                                        resourceinfo.get_name().to_string(),
                                        tokio::spawn(ResourceManager::retry_failed_task(
                                            coord,
                                            resourceinfo,
                                        )),
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok(())
            }
        }
    }

    async fn exec_async_task(coord: Arc<CoordService>, mut resourceinfo: ResourceInfo) {
        let future_interval = resourceinfo.get_time() - now_timestamp_nanos();
        let future_time = Instant::now() + Duration::from_nanos(future_interval as u64);
        tokio::time::sleep_until(future_time).await;
        resourceinfo.set_status(ResourceStatus::Executing);
        resourceinfo.set_is_new_add(false);
        if let Err(meta_err) = coord
            .meta_manager()
            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
            .await
        {
            error!("failed to execute the async task: {}", meta_err.to_string());
        }
        // execute, if failed, retry later
        let _ = ResourceManager::do_operator(coord.clone(), resourceinfo.clone()).await;
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

    async fn metrics_service(
        coord: Arc<CoordService>,
        root_metrics_register: Arc<MetricsRegister>,
    ) {
        let start = tokio::time::Instant::now() + Duration::from_secs(10);
        let interval = Duration::from_secs(10);
        let mut intv = tokio::time::interval_at(start, interval);
        loop {
            intv.tick().await;
            let mut points_buffer = Vec::new();
            let mut reporter = LPReporter::new(&mut points_buffer);
            root_metrics_register.report(&mut reporter);

            for lines in points_buffer {
                if let Err(e) = coord
                    .write_lines(
                        DEFAULT_CATALOG,
                        USAGE_SCHEMA,
                        Precision::NS,
                        lines.iter().map(|l| l.to_line()).collect::<Vec<_>>(),
                        None,
                    )
                    .await
                {
                    error!("write metrics to {DEFAULT_CATALOG} fail. {e}")
                }
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

        let meta =
            self.tenant_meta(&info.tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: info.tenant.clone(),
                })?;

        meta.delete_bucket(&info.database, info.bucket.id).await?;

        Ok(())
    }

    async fn prune_shards(
        &self,
        tenant: &str,
        database: &str,
        time_ranges: &TimeRanges,
    ) -> Result<Vec<ReplicationSet>, CoordinatorError> {
        let meta = self.meta_manager().tenant_meta(tenant).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            },
        )?;
        let buckets = meta.mapping_bucket(database, time_ranges.min_ts(), time_ranges.max_ts())?;
        let shards = buckets.into_iter().flat_map(|b| b.shard_group).collect();

        Ok(shards)
    }

    fn build_query_checker(&self, tenant: &str) -> CheckFuture {
        let tenant = tenant.to_string();
        let meta = self.meta.clone();

        let checker = async move {
            meta.limiter(&tenant)
                .await?
                .check_coord_queries()
                .await
                .map_err(CoordinatorError::from)
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
            Err(e) => Err(CoordinatorError::ArrowError { source: e }),
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
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("limit check"));

            let limiter = self.meta.limiter(tenant).await?;
            let write_size = points.len();

            limiter.check_coord_writes().await?;
            limiter.check_coord_data_in(write_size).await?;

            self.metrics.coord_writes(tenant, db).inc_one();
            self.metrics
                .coord_data_in(tenant, db)
                .inc(write_size as u64);
        }
        if info.vnodes.is_empty() {
            return Err(CoordinatorError::CommonError {
                msg: "no available vnode in replication set".to_string(),
            });
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

    async fn admin_command_on_node(
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
        TskvRaftWriter {
            request,
            meta: self.meta.clone(),
            node_id: self.node_id,
            memory_pool: self.memory_pool.clone(),
            raft_manager: self.raft_manager.clone(),
            timeout: self.config.query.write_timeout,
            enable_gzip: self.config.service.grpc_enable_gzip,
            total_memory: self.config.deployment.memory * 1024 * 1024 * 1024,
        }
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
        let mut write_bytes: usize = 0;
        let meta_client =
            self.meta
                .tenant_meta(tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
                })?;
        let mut map_lines: HashMap<ReplicationSetId, VnodeLines> = HashMap::new();
        let db_schema =
            meta_client
                .get_db_schema(db)?
                .ok_or_else(|| MetaError::DatabaseNotFound {
                    database: db.to_string(),
                })?;
        if db_schema.options().get_db_is_hidden() {
            return Err(crate::errors::CoordinatorError::Meta {
                source: MetaError::DatabaseNotFound {
                    database: db.to_string(),
                },
            });
        }

        let db_precision = db_schema.config.precision_or_default();
        for line in lines {
            let ts = timestamp_convert(precision, *db_precision, line.timestamp).ok_or(
                CoordinatorError::CommonError {
                    msg: "timestamp overflow".to_string(),
                },
            )?;
            let info = meta_client
                .locate_replication_set_for_write(db, line.hash_id, ts)
                .await?;
            let lines_entry = map_lines.entry(info.id).or_insert(VnodeLines::new(info));
            lines_entry.add_line(line)
        }

        let mut requests = Vec::new();
        for lines in map_lines.into_values() {
            let batches =
                line_to_batches(&lines.lines).map_err(|e| CoordinatorError::CommonError {
                    msg: format!("line to batch error: {}", e),
                })?;
            let points = Arc::new(mutable_batches_to_point(db, batches));
            write_bytes += points.len();
            requests.extend(
                self.push_points_to_requests(tenant, db, precision, lines.info, points, span_ctx)
                    .await?,
            );
        }
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
        Ok(write_bytes)
    }

    async fn write_record_batch<'a>(
        &self,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
        db_precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize> {
        let mut write_bytes: usize = 0;
        let mut precision = Precision::NS;
        let tenant = table_schema.tenant.as_str();
        let db = table_schema.db.as_str();
        let meta_client =
            self.meta
                .tenant_meta(tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
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
                let tskv_schema_column =
                    table_schema
                        .column(name)
                        .ok_or(CoordinatorError::CommonError {
                            msg: format!("column {} not found in table {}", name, table_name),
                        })?;
                if name == TIME_FIELD {
                    let precsion_and_value =
                        get_precision_and_value_from_arrow_column(column, idx)?;
                    precision = precsion_and_value.0;
                    ts = timestamp_convert(precision, db_precision, precsion_and_value.1).ok_or(
                        CoordinatorError::CommonError {
                            msg: "timestamp overflow".to_string(),
                        },
                    )?;
                    has_ts = true;
                }
                if matches!(tskv_schema_column.column_type, ColumnType::Tag) {
                    let value = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or(CoordinatorError::CommonError {
                            msg: format!("column {} is not StringArray", name),
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
                return Err(CoordinatorError::CommonError {
                    msg: format!("column {} not found in table {}", TIME_FIELD, table_name),
                });
            }

            if !has_fileds {
                return Err(CoordinatorError::TskvError {
                    source: TskvError::FieldsIsEmpty,
                });
            }

            let hash = hasher.number();
            let info = meta_client
                .locate_replication_set_for_write(db, hash, ts)
                .await?;
            repl_idx.entry(info).or_default().push(idx as u32);
        }

        let mut requests = Vec::new();
        for (repl, idxs) in repl_idx {
            let indices = UInt32Array::from(idxs);
            let columns = record_batch
                .columns()
                .iter()
                .map(|column| {
                    take(column, &indices, None).map_err(|e| CoordinatorError::CommonError {
                        msg: format!("take column error: {}", e),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = record_batch.schema();
            let points = Arc::new(
                arrow_array_to_points(columns, schema, table_schema.clone(), indices.len())
                    .map_err(|e| CoordinatorError::CommonError {
                        msg: format!("arrow array to points error: {}", e),
                    })?,
            );
            write_bytes += points.len();
            requests.extend(
                self.push_points_to_requests(tenant, db, precision, repl, points, span_ctx)
                    .await?,
            );
        }
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
        let predicate_bytes = bincode::serialize(predicate)?;
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
                    return Err(CoordinatorError::CommonError {
                        msg: format!("A Replication Already in {}", node_id),
                    });
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

        let tenant_meta =
            self.meta
                .tenant_meta(tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
                })?;

        let mut series_keys = vec![];
        for new_tag in new_tags.iter_mut() {
            let key = mem::take(&mut new_tag.key);
            let tag_name = unsafe { String::from_utf8_unchecked(key) };

            let id = table_schema
                .column(&tag_name)
                .ok_or(TskvError::ColumnNotFound { column: tag_name })?
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
                    let tskv_schema_column =
                        table_schema
                            .column(name)
                            .ok_or(CoordinatorError::CommonError {
                                msg: format!("column {} not found in table {}", name, table_name),
                            })?;

                    if matches!(tskv_schema_column.column_type, ColumnType::Tag) {
                        let value = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or(CoordinatorError::CommonError {
                                msg: format!("column {} is not string", name),
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
            TimeUnit::Second => Err(CoordinatorError::CommonError {
                msg: "time field not support second".to_string(),
            }),
            TimeUnit::Millisecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or(CoordinatorError::CommonError {
                        msg: "time field data type miss match: millisecond".to_string(),
                    })?
                    .value(idx);
                Ok((Precision::MS, value))
            }
            TimeUnit::Microsecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or(CoordinatorError::CommonError {
                        msg: "time field data type miss match: microsecond".to_string(),
                    })?
                    .value(idx);
                Ok((Precision::US, value))
            }
            TimeUnit::Nanosecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or(CoordinatorError::CommonError {
                        msg: "time field data type miss match: nanosecond".to_string(),
                    })?
                    .value(idx);
                Ok((Precision::NS, value))
            }
        },
        DataType::Int64 => {
            let value = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(CoordinatorError::CommonError {
                    msg: "time field data type miss match: int64".to_string(),
                })?
                .value(idx);
            Ok((Precision::NS, value))
        }
        _ => Err(CoordinatorError::CommonError {
            msg: "time field data type miss match".to_string(),
        }),
    }
}
