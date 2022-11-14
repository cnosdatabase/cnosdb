use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::RecordBatchStream,
};
use futures::Stream;
use models::codec::Encoding;
use models::{
    predicate::domain::PredicateRef,
    schema::{ColumnType, TableColumn, TskvTableSchema, TIME_FIELD},
};

use tskv::{
    engine::EngineRef,
    iterator::{QueryOption, RowIterator, TableScanMetrics},
};

use tskv::Error;

#[allow(dead_code)]
pub struct TableScanStream {
    proj_schema: SchemaRef,
    batch_size: usize,
    store_engine: EngineRef,

    iterator: RowIterator,

    metrics: TableScanMetrics,
}

impl TableScanStream {
    pub fn new(
        table_schema: TskvTableSchema,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        batch_size: usize,
        store_engine: EngineRef,
        metrics: TableScanMetrics,
    ) -> Result<Self, Error> {
        let mut proj_fileds = Vec::with_capacity(proj_schema.fields().len());
        for item in proj_schema.fields().iter() {
            let field_name = item.name();
            if field_name == TIME_FIELD {
                let encoding = match table_schema.column(TIME_FIELD) {
                    None => Encoding::Default,
                    Some(v) => v.encoding,
                };
                proj_fileds.push(TableColumn::new(
                    0,
                    TIME_FIELD.to_string(),
                    ColumnType::Time,
                    encoding,
                ));
                continue;
            }

            if let Some(v) = table_schema.column(field_name) {
                proj_fileds.push(v.clone());
            } else {
                return Err(Error::NotFoundField {
                    reason: field_name.clone(),
                });
            }
        }

        let proj_table_schema =
            TskvTableSchema::new(table_schema.db.clone(), table_schema.name, proj_fileds);

        let filter = filter
            .filter()
            .translate_column(|c| proj_table_schema.column(&c.name).cloned());

        // 提取过滤条件
        let time_filter = filter.translate_column(|e| match e.column_type {
            ColumnType::Time => Some(e.name.clone()),
            _ => None,
        });
        let tags_filter = filter.translate_column(|e| match e.column_type {
            ColumnType::Tag => Some(e.name.clone()),
            _ => None,
        });
        let fields_filter = filter.translate_column(|e| match e.column_type {
            ColumnType::Field(_) => Some(e.name.clone()),
            _ => None,
        });
        let option = QueryOption {
            table_schema: proj_table_schema,
            datafusion_schema: proj_schema.clone(),
            time_filter,
            tags_filter,
            fields_filter,
        };

        let iterator = match RowIterator::new(
            metrics.tskv_metrics(),
            store_engine.clone(),
            option,
            batch_size,
        ) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };

        Ok(Self {
            proj_schema,
            batch_size,
            store_engine,
            iterator,
            metrics,
        })
    }
}

impl Stream for TableScanStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let timer = this.metrics.elapsed_compute().timer();

        let result = match this.iterator.next() {
            Some(data) => match data {
                Ok(batch) => std::task::Poll::Ready(Some(Ok(batch))),
                Err(err) => {
                    std::task::Poll::Ready(Some(Err(ArrowError::CastError(err.to_string()))))
                }
            },
            None => {
                this.metrics.done();
                std::task::Poll::Ready(None)
            }
        };

        timer.done();
        this.metrics.record_poll(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // todo   (self.data.len(), Some(self.data.len()))
        (0, Some(0))
    }
}

impl RecordBatchStream for TableScanStream {
    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }
}
