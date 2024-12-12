#![allow(dead_code, unused_variables)]

use std::fmt::Debug;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::todo;

use config::tskv::Config;
use datafusion::arrow::record_batch::RecordBatch;
use meta::model::meta_admin::AdminMeta;
use meta::model::meta_tenant::TenantMeta;
use meta::model::{MetaClientRef, MetaRef};
use models::meta_data::{ReplicationSet, ReplicationSetId, VnodeId, VnodeInfo, VnodeStatus};
use models::object_reference::ResolvedTable;
use models::predicate::domain::{ResolvedPredicate, ResolvedPredicateRef};
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use protocol_parser::Line;
use protos::kv_service::{RaftWriteCommand, UpdateSetValue};
use trace::SpanContext;
use tskv::reader::QueryOption;
use utils::precision::Precision;

use crate::errors::CoordinatorResult;
use crate::raft::manager::RaftNodesManager;
use crate::raft::writer::TskvRaftWriter;
use crate::service::CoordServiceMetrics;
use crate::{Coordinator, ReplicationCmdType, SendableCoordinatorRecordBatchStream};

pub const WITH_NONEMPTY_DATABASE_FOR_TEST: &str = "with_nonempty_database";

#[derive(Debug, Default)]
pub struct MockCoordinator {}

#[async_trait::async_trait]
impl Coordinator for MockCoordinator {
    fn node_id(&self) -> u64 {
        0
    }

    fn meta_manager(&self) -> MetaRef {
        Arc::new(AdminMeta::mock())
    }

    fn raft_manager(&self) -> Arc<RaftNodesManager> {
        todo!()
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        Some(Arc::new(TenantMeta::mock()))
    }

    async fn compact_vnodes(&self, tenant: &str, vnode_ids: Vec<VnodeId>) -> CoordinatorResult<()> {
        todo!()
    }

    fn tskv_raft_writer(&self, request: RaftWriteCommand) -> TskvRaftWriter {
        todo!()
    }

    async fn write_replica_by_raft(
        &self,
        replica: ReplicationSet,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        todo!()
    }

    async fn table_vnodes(
        &self,
        table: &ResolvedTable,
        _predicate: ResolvedPredicateRef,
    ) -> CoordinatorResult<Vec<ReplicationSet>> {
        if table.database() == WITH_NONEMPTY_DATABASE_FOR_TEST {
            return Ok(vec![
                ReplicationSet::new(
                    0,
                    0,
                    0,
                    vec![VnodeInfo {
                        id: 0,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
                ReplicationSet::new(
                    1,
                    0,
                    1,
                    vec![VnodeInfo {
                        id: 1,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
                ReplicationSet::new(
                    2,
                    0,
                    2,
                    vec![VnodeInfo {
                        id: 2,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
                ReplicationSet::new(
                    3,
                    0,
                    3,
                    vec![VnodeInfo {
                        id: 3,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
                ReplicationSet::new(
                    4,
                    0,
                    4,
                    vec![VnodeInfo {
                        id: 4,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
                ReplicationSet::new(
                    5,
                    0,
                    5,
                    vec![VnodeInfo {
                        id: 5,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
                ReplicationSet::new(
                    6,
                    0,
                    6,
                    vec![VnodeInfo {
                        id: 6,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
                ReplicationSet::new(
                    7,
                    0,
                    7,
                    vec![VnodeInfo {
                        id: 7,
                        node_id: 0,
                        status: VnodeStatus::Running,
                    }],
                ),
            ]);
        }
        Ok(vec![])
    }

    async fn write_lines<'a>(
        &self,
        tenant: &str,
        db: &str,
        precision: Precision,
        line: Vec<Line<'a>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize> {
        todo!()
    }

    async fn write_record_batch<'a>(
        &self,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
        db_precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize> {
        todo!()
    }

    fn table_scan(
        &self,
        option: QueryOption,
        _span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        // TODO
        todo!()
    }

    fn tag_scan(
        &self,
        option: QueryOption,
        _span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        todo!("tag_scan")
    }

    async fn delete_from_table(
        &self,
        table: &ResolvedTable,
        predicate: &ResolvedPredicate,
    ) -> CoordinatorResult<()> {
        todo!("delete_from_table")
    }

    async fn replication_manager(
        &self,
        tenant: &str,
        cmd_type: ReplicationCmdType,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn replica_checksum(
        &self,
        tenant: &str,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<Vec<RecordBatch>> {
        Ok(vec![])
    }

    fn metrics(&self) -> &Arc<CoordServiceMetrics> {
        todo!()
    }

    async fn update_tags_value(
        &self,
        table_schema: TskvTableSchemaRef,
        new_tags: Vec<UpdateSetValue>,
        record_batches: Vec<RecordBatch>,
    ) -> CoordinatorResult<()> {
        todo!()
    }

    fn get_config(&self) -> Config {
        Config::default()
    }
    fn get_writer_count(&self) -> Arc<AtomicUsize> {
        todo!()
    }
}
