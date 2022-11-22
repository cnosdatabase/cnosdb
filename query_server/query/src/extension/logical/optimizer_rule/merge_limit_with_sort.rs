use std::sync::Arc;

use datafusion::{
    logical_expr::{
        Limit, LogicalPlan, {Extension, Sort},
    },
    optimizer::{OptimizerConfig, OptimizerRule},
};

use super::super::plan_node::topk::TopKPlanNode;

use datafusion::error::Result;

pub struct MergeLimitWithSortRule {}

impl OptimizerRule for MergeLimitWithSortRule {
    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // Note: this code simply looks for the pattern of a Limit followed by a
        // Sort and replaces it by a TopK node. It does not handle many
        // edge cases (e.g multiple sort columns, sort ASC / DESC), etc.
        if let LogicalPlan::Limit(Limit {
            skip,
            fetch: Some(fetch),
            input,
        }) = plan
        {
            if let LogicalPlan::Sort(Sort {
                ref expr,
                ref input,
                ..
            }) = **input
            {
                // If k is too large, no topk optimization is performed
                if skip + fetch <= 255 {
                    // we found a sort with a single sort expr, replace with a a TopK
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(TopKPlanNode::new(
                            expr.clone(),
                            Arc::new(self.optimize(input.as_ref(), optimizer_config)?),
                            Some(*skip),
                            *fetch,
                        )),
                    }));
                }
            }
        }

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        datafusion::optimizer::utils::optimize_children(self, plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "merge_limit_with_sort"
    }
}
