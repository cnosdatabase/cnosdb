use std::{
    any::Any,
    fmt::{Display, Formatter},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{
        array::ArrayBuilder, datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch,
    },
    arrow::{array::ArrayRef, error::Result as ArrowResult},
    error::Result,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::Stream;
use models::{
    arrow_array::{build_arrow_array_builders, WriteArrow},
    predicate::domain::{ColumnDomains, PredicateRef},
    schema::{ColumnType, TskvTableSchemaRef},
    SeriesKey, TagValue,
};

use trace::debug;
use tskv::{engine::EngineRef, index::IndexError};

#[derive(Debug, Clone)]
pub struct TagScanExec {
    // connection
    // db: CustomDataSource,
    table_schema: TskvTableSchemaRef,
    proj_schema: SchemaRef,
    predicate: PredicateRef,
    engine: EngineRef,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl TagScanExec {
    pub(crate) fn new(
        table_schema: TskvTableSchemaRef,
        proj_schema: SchemaRef,
        predicate: PredicateRef,
        engine: EngineRef,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();

        Self {
            table_schema,
            proj_schema,
            predicate,
            engine,
            metrics,
        }
    }

    pub fn predicate(&self) -> PredicateRef {
        self.predicate.clone()
    }
}

impl ExecutionPlan for TagScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TagScanExec {
            table_schema: self.table_schema.clone(),
            proj_schema: self.proj_schema.clone(),
            engine: self.engine.clone(),
            metrics: self.metrics.clone(),
            predicate: self.predicate.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start TagScanExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let batch_size = context.session_config().batch_size();

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let tags_filter = self
            .predicate()
            .filter()
            .translate_column(|c| self.table_schema.column(&c.name).cloned())
            .translate_column(|e| match e.column_type {
                ColumnType::Tag => Some(e.name.clone()),
                _ => None,
            });

        do_tag_scan(
            self.table_schema.clone(),
            self.schema(),
            tags_filter,
            self.engine.clone(),
            metrics,
            batch_size,
        )
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let filter = self.predicate();
                let fields: Vec<_> = self
                    .proj_schema
                    .fields()
                    .iter()
                    .map(|x| x.name().to_owned())
                    .collect::<Vec<String>>();
                write!(
                    f,
                    "TagScan: {}, projection=[{}]",
                    PredicateDisplay(&filter),
                    fields.join(","),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // TODO
        Statistics::default()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// A wrapper to customize PredicateRef display
#[derive(Debug)]
struct PredicateDisplay<'a>(&'a PredicateRef);

impl<'a> Display for PredicateDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let filter = self.0;
        write!(
            f,
            "limit={:?}, predicate={:?}",
            filter.limit(),
            filter.filter(),
        )
    }
}

fn do_tag_scan(
    table_schema: TskvTableSchemaRef,
    proj_schema: SchemaRef,
    tags_filter: ColumnDomains<String>,
    store_engine: EngineRef,
    metrics: BaselineMetrics,
    _batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    debug!(
        "Start do_tag_scan: proj_schema {}, tags_filter {:?}",
        proj_schema, tags_filter
    );

    let timer = metrics.elapsed_compute().timer();
    let db = &table_schema.db;
    let series_keys = store_engine
        .get_series_id_by_filter(db, &table_schema.name, &tags_filter)
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
        .iter()
        .map(|sid| store_engine.get_series_key(db, *sid))
        .collect::<std::result::Result<Vec<_>, IndexError>>()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

    debug!("Scan series key count: {}", series_keys.len());

    let mut builder = TagRecordBatchStreamBuilder::try_new(proj_schema, series_keys.len())?;

    series_keys
        .into_iter()
        .flatten()
        .for_each(|k| builder.append(k));

    let reader = builder.build()?;

    timer.done();

    Ok(Box::pin(reader))
}

struct TagRecordBatchStream {
    schema: SchemaRef,
    columns: Option<Vec<ArrayRef>>,
}

impl Stream for TagRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.columns
            .take()
            .map(|e| {
                let batch = RecordBatch::try_new(self.schema.clone(), e);
                Poll::Ready(Some(batch))
            })
            .unwrap_or_else(|| Poll::Ready(None))
    }
}

impl RecordBatchStream for TagRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct TagRecordBatchStreamBuilder {
    schema: SchemaRef,
    tag_key_array: Vec<String>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    tag_values_containers: Vec<Vec<Option<TagValue>>>,
}

impl TagRecordBatchStreamBuilder {
    pub fn try_new(schema: SchemaRef, size_hint: usize) -> ArrowResult<Self> {
        let builders = build_arrow_array_builders(schema.clone(), size_hint)?;
        let tag_values_containers: Vec<Vec<Option<TagValue>>> =
            vec![Vec::with_capacity(size_hint); schema.fields().len()];

        let tag_key_array = schema.fields().iter().map(|e| e.name()).cloned().collect();

        Ok(Self {
            schema,
            tag_key_array,
            builders,
            tag_values_containers,
        })
    }

    pub fn append(&mut self, series_key: SeriesKey) {
        self.tag_key_array
            .iter()
            .zip(&mut self.tag_values_containers)
            .for_each(|(tag_key, vals_container)| {
                // TODO improve, to return Option
                let tag_val = series_key.tag_val(tag_key);

                vals_container.push(tag_val);
            })
    }

    pub fn build(mut self) -> ArrowResult<TagRecordBatchStream> {
        trace::trace!("tag_values_containers: {:?}", &self.tag_values_containers);

        self.tag_values_containers
            .into_iter()
            .zip(&mut self.builders)
            .try_for_each(|(c, builder)| c.write(builder))?;

        let columns = self.builders.iter_mut().map(|e| e.finish()).collect();

        trace::trace!("columns: {:?}", columns);

        Ok(TagRecordBatchStream {
            schema: self.schema,
            columns: Some(columns),
        })
    }
}
