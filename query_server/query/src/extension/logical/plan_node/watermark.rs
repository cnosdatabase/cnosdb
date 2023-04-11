use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::prelude::Expr;
use models::schema::Watermark;

use crate::extension::{EVENT_TIME_COLUMN, WATERMARK_DELAY_MS};

#[derive(Clone)]
pub struct WatermarkNode {
    pub watermark: Watermark,
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}

impl WatermarkNode {
    /// Create a new WatermarkNode
    pub fn try_new(watermark: Watermark, input: Arc<LogicalPlan>) -> Result<Self, DataFusionError> {
        let schema = input.schema();
        // find event time column
        let idx = schema.index_of_column_by_name(None, &watermark.column)?;
        let mut metadata = input.schema().metadata().clone();
        // It will be used when the aggregate node is transferred to a physical node
        let _ = metadata.insert(EVENT_TIME_COLUMN.into(), idx.to_string());
        let _ = metadata.insert(
            WATERMARK_DELAY_MS.into(),
            watermark.delay.as_millis().to_string(),
        );

        let schema = Arc::new(DFSchema::new_with_metadata(
            schema.fields().clone(),
            metadata,
        )?);

        Ok(Self {
            watermark,
            input,
            schema,
        })
    }
}

impl Debug for WatermarkNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for WatermarkNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Watermark: event_time={}, delay={}ms",
            self.watermark.column,
            self.watermark.delay.as_millis(),
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");

        Arc::new(self.clone())
    }

    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        HashSet::default()
    }
}
