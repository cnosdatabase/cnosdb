use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{project_schema, ExecutionPlan},
};
use models::{
    predicate::domain::{Predicate, PredicateRef},
    schema::TableSchema,
};
use spi::catalog::MetadataError;
use tskv::engine::EngineRef;

use crate::{
    data_source::tskv_sink::TskvRecordBatchSinkProvider,
    extension::physical::plan_node::table_writer::TableWriterExec, tskv_exec::TskvExec,
};

pub struct ClusterTable {
    engine: EngineRef,
    schema: TableSchema,
}

impl ClusterTable {
    pub(crate) async fn create_physical_plan(
        &self,
        projection: &Option<Vec<usize>>,
        predicate: PredicateRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let proj_schema = self.project_schema(projection)?;

        Ok(Arc::new(TskvExec::new(
            self.schema.clone(),
            proj_schema,
            predicate,
            self.engine.clone(),
        )))
    }

    pub fn new(engine: EngineRef, schema: TableSchema) -> Self {
        ClusterTable { engine, schema }
    }

    pub async fn write(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let record_batch_sink_privider = Arc::new(TskvRecordBatchSinkProvider::new(
            self.engine.clone(),
            self.schema.clone(),
        ));

        Ok(Arc::new(TableWriterExec::new(
            input,
            self.schema.clone(),
            record_batch_sink_privider,
        )))
    }

    pub fn table_schema(&self) -> &TableSchema {
        &self.schema
    }

    // Check and return the projected schema
    fn project_schema(&self, projection: &Option<Vec<usize>>) -> Result<SchemaRef> {
        valid_project(&self.schema, projection)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        project_schema(&self.schema.to_arrow_schema(), projection.as_ref())
    }
}

#[async_trait]
impl TableProvider for ClusterTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(
            Predicate::default()
                .set_limit(limit)
                .push_down_filter(filters, &self.schema),
        );

        return self.create_physical_plan(projection, filter).await;
    }
    fn supports_filter_pushdown(&self, _: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

/// Check the validity of the projection
///
/// 1. If the projection contains the time column, it must contain the field column, otherwise an error will be reported
pub fn valid_project(
    schema: &TableSchema,
    projection: &Option<Vec<usize>>,
) -> std::result::Result<(), MetadataError> {
    let mut field_count = 0;
    let mut contains_time_column = false;

    if let Some(e) = projection.as_ref() {
        e.iter().cloned().for_each(|idx| {
            if let Some(c) = schema.column_by_index(idx) {
                if c.column_type.is_time() {
                    contains_time_column = true;
                }
                if c.column_type.is_field() {
                    field_count += 1;
                }
            };
        });
    }

    if contains_time_column && field_count == 0 {
        return Err(MetadataError::InvalidSchema {
            error_msg:
                "If the projection contains the time column, it must contain the field column"
                    .to_string(),
        });
    }

    Ok(())
}
