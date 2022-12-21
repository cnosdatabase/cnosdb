use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use snafu::ResultExt;

use meta::error::MetaError;
use spi::query::execution;
use spi::query::execution::MetadataSnafu;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::DescribeDatabase;
use std::sync::Arc;

pub struct DescribeDatabaseTask {
    stmt: DescribeDatabase,
}

impl DescribeDatabaseTask {
    pub fn new(stmt: DescribeDatabase) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DescribeDatabaseTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        describe_database(self.stmt.database_name.as_str(), query_state_machine)
    }
}

fn describe_database(
    database_name: &str,
    machine: QueryStateMachineRef,
) -> Result<Output, ExecutionError> {
    let tenant = machine.session.tenant();
    let client = machine
        .meta
        .tenant_manager()
        .tenant_meta(tenant)
        .ok_or(MetaError::TenantNotFound {
            tenant: tenant.to_string(),
        })
        .context(MetadataSnafu)?;
    let db_cfg = client
        .get_db_schema(database_name)
        .context(execution::MetadataSnafu)?
        .ok_or(MetaError::DatabaseNotFound {
            database: database_name.to_string(),
        })
        .context(MetadataSnafu)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("TTL", DataType::Utf8, false),
        Field::new("SHARD", DataType::Utf8, false),
        Field::new("VNODE_DURATION", DataType::Utf8, false),
        Field::new("REPLICA", DataType::Utf8, false),
        Field::new("PRECISION", DataType::Utf8, false),
    ]));

    let ttl = db_cfg.config.ttl_or_default().to_string();
    let shard = db_cfg.config.shard_num_or_default().to_string();
    let vnode_duration = db_cfg.config.vnode_duration_or_default().to_string();
    let replica = db_cfg.config.replica_or_default().to_string();
    let precision = db_cfg.config.precision_or_default().to_string();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![ttl.as_str()])),
            Arc::new(StringArray::from(vec![shard.as_str()])),
            Arc::new(StringArray::from(vec![vnode_duration.as_str()])),
            Arc::new(StringArray::from(vec![replica.as_str()])),
            Arc::new(StringArray::from(vec![precision.as_str()])),
        ],
    )
    .map_err(|e| ExecutionError::External {
        source: DataFusionError::ArrowError(e),
    })?;

    let batches = vec![batch];

    Ok(Output::StreamData(schema, batches))
}
