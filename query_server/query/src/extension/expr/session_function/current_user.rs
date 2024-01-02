use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::scalar::ScalarValue;
use spi::service::protocol::Context;
use spi::Result;

pub fn register_session_udf(df_session_ctx: &SessionContext, context: &Context) {
    let username = context.user().desc().name().to_owned();
    let current_user = move |_args: &[ArrayRef]| -> Result<ArrayRef, DataFusionError> {
        let array = ScalarValue::Utf8(Some(username.clone())).to_array();
        Ok(Arc::new(array))
    };
    let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Utf8)));
    let udf = ScalarUDF::new(
        "current_user",
        &Signature::any(0, Volatility::Immutable),
        &return_type_fn,
        &make_scalar_function(current_user),
    );
    df_session_ctx.register_udf(udf);
}
