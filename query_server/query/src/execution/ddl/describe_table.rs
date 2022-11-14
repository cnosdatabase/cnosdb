use crate::execution::ddl::DDLDefinitionTask;
use crate::metadata::stream_from_batches;
use async_trait::async_trait;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableReference;
use models::schema::TableSchema;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution::ExternalSnafu;
use spi::query::execution::MetadataSnafu;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::DescribeTable;
use std::sync::Arc;

pub struct DescribeTableTask {
    stmt: DescribeTable,
}

impl DescribeTableTask {
    pub fn new(stmt: DescribeTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DescribeTableTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        describe_table(
            self.stmt.table_name.as_str(),
            query_state_machine.catalog.clone(),
        )
    }
}

fn describe_table(table_name: &str, catalog: MetaDataRef) -> Result<Output, ExecutionError> {
    let table_name = TableReference::from(table_name);
    let table_schema = catalog.table(table_name).context(MetadataSnafu)?;

    match table_schema {
        TableSchema::TsKvTableSchema(tskv_schema) => {
            let mut name = StringBuilder::new();
            let mut data_type = StringBuilder::new();
            let mut column_type = StringBuilder::new();
            let mut encoding = StringBuilder::new();

            tskv_schema.columns().iter().for_each(|column| {
                name.append_value(column.name.as_str());
                data_type.append_value(column.column_type.to_sql_type_str());
                column_type.append_value(column.column_type.as_column_type_str());
                encoding.append_value(column.encoding.as_str());
            });
            let schema = Arc::new(Schema::new(vec![
                Field::new("COLUMN_NAME", DataType::Utf8, false),
                Field::new("DATA_TYPE", DataType::Utf8, false),
                Field::new("COLUMN_TYPE", DataType::Utf8, false),
                Field::new("COMPRESSION_CODEC", DataType::Utf8, false),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(name.finish()),
                    Arc::new(data_type.finish()),
                    Arc::new(column_type.finish()),
                    Arc::new(encoding.finish()),
                ],
            )
            .map_err(datafusion::error::DataFusionError::ArrowError)
            .context(ExternalSnafu)?;
            let batches = vec![Arc::new(batch)];
            Ok(Output::StreamData(stream_from_batches(batches)))
        }
        TableSchema::ExternalTableSchema(external_schema) => {
            let mut name = StringBuilder::new();
            let mut data_type = StringBuilder::new();
            external_schema.schema.fields.iter().for_each(|field| {
                name.append_value(field.name());
                data_type.append_value(field.data_type().to_string());
            });
            let schema = Arc::new(Schema::new(vec![
                Field::new("COLUMN_NAME", DataType::Utf8, false),
                Field::new("DATA_TYPE", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(name.finish()), Arc::new(data_type.finish())],
            )
            .map_err(datafusion::error::DataFusionError::ArrowError)
            .context(ExternalSnafu)?;
            let batches = vec![Arc::new(batch)];
            Ok(Output::StreamData(stream_from_batches(batches)))
        }
    }
}
