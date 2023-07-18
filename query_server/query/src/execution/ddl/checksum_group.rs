use async_trait::async_trait;
use coordinator::VnodeSummarizerCmdType;
use datafusion::arrow::datatypes::SchemaRef;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ChecksumGroup;
use spi::query::recordbatch::RecordBatchStreamWrapper;
use spi::Result;

use super::DDLDefinitionTask;

pub struct ChecksumGroupTask {
    schema: SchemaRef,
    stmt: ChecksumGroup,
}

impl ChecksumGroupTask {
    #[inline(always)]
    pub fn new(stmt: ChecksumGroup, schema: SchemaRef) -> Self {
        Self { schema, stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ChecksumGroupTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let replication_set_id = self.stmt.replication_set_id;
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        let cmd_type = VnodeSummarizerCmdType::Checksum(replication_set_id);
        let checksums = coord.vnode_summarizer(tenant, cmd_type).await?;
        let stream = RecordBatchStreamWrapper::new(self.schema.clone(), checksums);
        Ok(Output::StreamData(Box::pin(stream)))
    }
}
