use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::AggregateUDF;
use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use spi::query::function::*;
use spi::{QueryError, Result};

pub struct DFSessionContextFuncAdapter<'a> {
    ctx: &'a mut SessionContext,
}

impl<'a> DFSessionContextFuncAdapter<'a> {
    pub fn new(ctx: &'a mut SessionContext) -> Self {
        Self { ctx }
    }
}

impl<'a> FunctionMetadataManager for DFSessionContextFuncAdapter<'a> {
    fn register_udf(&mut self, udf: ScalarUDF) -> Result<()> {
        if self.ctx.udf(udf.name.as_str()).is_err() {
            return Err(QueryError::FunctionExists { name: udf.name });
        }

        self.ctx.register_udf(udf);
        Ok(())
    }

    fn register_udaf(&mut self, udaf: AggregateUDF) -> Result<()> {
        if self.ctx.udaf(udaf.name.as_str()).is_err() {
            return Err(QueryError::FunctionExists { name: udaf.name });
        }

        self.ctx.register_udaf(udaf);
        Ok(())
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.ctx
            .udf(name)
            .map_err(|e| QueryError::Datafusion { source: e })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.ctx
            .udaf(name)
            .map_err(|e| QueryError::Datafusion { source: e })
    }

    fn udfs(&self) -> Vec<Arc<ScalarUDF>> {
        self.ctx
            .state()
            .scalar_functions
            .values()
            .cloned()
            .collect()
    }
}
