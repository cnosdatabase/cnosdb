#![recursion_limit = "256"]

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use config::tskv::Config;
use datafusion::arrow::record_batch::RecordBatch;
use errors::CoordinatorError;
use futures::Stream;
use meta::model::{MetaClientRef, MetaRef};
use models::meta_data::{
    NodeId, ReplicaAllInfo, ReplicationSet, ReplicationSetId, VnodeAllInfo, VnodeId,
};
use models::object_reference::ResolvedTable;
use models::predicate::domain::{ResolvedPredicate, ResolvedPredicateRef};
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use protocol_parser::Line;
use protos::kv_service::{RaftWriteCommand, UpdateSetValue};
use raft::manager::RaftNodesManager;
use raft::writer::TskvRaftWriter;
use snafu::ResultExt;
use trace::SpanContext;
use tskv::reader::QueryOption;
use utils::precision::Precision;

use crate::errors::{CoordinatorResult, MetaSnafu};
use crate::service::CoordServiceMetrics;

pub mod errors;
pub mod metrics;
pub mod raft;
pub mod reader;
pub mod resource_manager;
pub mod service;
pub mod service_mock;
pub mod tskv_executor;

pub type SendableCoordinatorRecordBatchStream =
    Pin<Box<dyn Stream<Item = CoordinatorResult<RecordBatch>> + Send>>;

#[derive(Debug, Clone)]
pub enum ReplicationCmdType {
    /// replica set id, dst nod id
    AddRaftFollower(u32, u64),
    /// vnode id. just remove the follower, if remove leader temporarily unavailable
    RemoveRaftNode(u32),
    /// replica set id
    DestoryRaftGroup(u32),
    /// replica set id, new leader vnode id
    PromoteLeader(u32, u32),
}

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync {
    fn node_id(&self) -> u64;
    fn meta_manager(&self) -> MetaRef;
    fn raft_manager(&self) -> Arc<RaftNodesManager>;
    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;

    fn tskv_raft_writer(&self, request: RaftWriteCommand) -> TskvRaftWriter;

    /// get all vnodes of a table to quering
    async fn table_vnodes(
        &self,
        table: &ResolvedTable,
        predicate: ResolvedPredicateRef,
    ) -> CoordinatorResult<Vec<ReplicationSet>>;

    async fn write_replica_by_raft(
        &self,
        replica: ReplicationSet,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()>;

    async fn write_lines<'a>(
        &self,
        tenant: &str,
        db: &str,
        precision: Precision,
        lines: Vec<Line<'a>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize>;

    async fn write_record_batch<'a>(
        &self,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
        db_precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize>;

    fn table_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream>;

    fn tag_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream>;

    async fn delete_from_table(
        &self,
        table: &ResolvedTable,
        predicate: &ResolvedPredicate,
    ) -> CoordinatorResult<()>;

    async fn compact_vnodes(&self, tenant: &str, vnode_ids: Vec<VnodeId>) -> CoordinatorResult<()>;

    /// A manager to manage vnode.
    async fn replication_manager(
        &self,
        tenant: &str,
        cmd_type: ReplicationCmdType,
    ) -> CoordinatorResult<()>;

    /// A summarizer to summarize vnode info.
    async fn replica_checksum(
        &self,
        tenant: &str,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<Vec<RecordBatch>>;

    fn metrics(&self) -> &Arc<CoordServiceMetrics>;

    async fn update_tags_value(
        &self,
        table_schema: TskvTableSchemaRef,
        new_tags: Vec<UpdateSetValue>,
        record_batches: Vec<RecordBatch>,
    ) -> CoordinatorResult<()>;

    fn get_config(&self) -> Config;
    fn get_writer_count(&self) -> Arc<AtomicUsize>;
}

#[async_trait::async_trait]
pub trait TskvLeaderCaller: Send + Sync {
    async fn call(&self, replica: &ReplicationSet, node_id: NodeId) -> CoordinatorResult<Vec<u8>>;
}

pub async fn get_vnode_all_info(
    meta: MetaRef,
    tenant: &str,
    vnode_id: u32,
) -> CoordinatorResult<VnodeAllInfo> {
    match meta.tenant_meta(tenant).await {
        Some(meta_client) => match meta_client.get_vnode_all_info(vnode_id) {
            Some(all_info) => Ok(all_info),
            None => Err(CoordinatorError::VnodeNotFound { id: vnode_id }),
        },

        None => Err(CoordinatorError::TenantNotFound {
            name: tenant.to_string(),
        }),
    }
}

pub async fn get_replica_all_info(
    meta: MetaRef,
    tenant: &str,
    replica_id: ReplicationSetId,
) -> CoordinatorResult<ReplicaAllInfo> {
    let replica = meta
        .tenant_meta(tenant)
        .await
        .ok_or_else(|| CoordinatorError::TenantNotFound {
            name: tenant.to_owned(),
        })?
        .get_replica_all_info(replica_id)
        .ok_or(CoordinatorError::ReplicationSetNotFound { id: replica_id })?;

    Ok(replica)
}

pub async fn get_replica_by_meta(
    meta: MetaRef,
    tenant: &str,
    db_name: &str,
    replica_id: ReplicationSetId,
) -> CoordinatorResult<ReplicationSet> {
    let replica = meta
        .tenant_meta(tenant)
        .await
        .ok_or_else(|| CoordinatorError::TenantNotFound {
            name: tenant.to_owned(),
        })?
        .get_replication_set_by_meta(db_name, replica_id)
        .await
        .context(MetaSnafu)?
        .ok_or(CoordinatorError::ReplicationSetNotFound { id: replica_id })?;

    Ok(replica)
}

pub async fn update_replication_set(
    meta: MetaRef,
    tenant: &str,
    db_name: &str,
    bucket_id: u32,
    replica_id: u32,
    del_info: &[models::meta_data::VnodeInfo],
    add_info: &[models::meta_data::VnodeInfo],
) -> CoordinatorResult<()> {
    meta.tenant_meta(tenant)
        .await
        .ok_or_else(|| CoordinatorError::TenantNotFound {
            name: tenant.to_owned(),
        })?
        .update_replication_set(db_name, bucket_id, replica_id, del_info, add_info)
        .await
        .context(MetaSnafu)?;

    Ok(())
}
