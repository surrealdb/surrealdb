use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;

use crate::err::Error;
use crate::expr::FlowResult;
use crate::val::Value;

/// Attempts to convert a logical plan to an execution plan.
///
/// If the conversion is not possible, the original plan and the error are returned.
#[allow(clippy::result_large_err)]
pub(crate) fn logical_plan_to_execution_plan(
	plan: crate::expr::LogicalPlan,
) -> Result<Vec<Arc<dyn ExecutionPlan>>, (crate::expr::LogicalPlan, Error)> {
	Err((plan, Error::unreachable("Logical plan to execution plan conversion not implemented")))
}

#[derive(Debug, Clone)]
pub(crate) struct ValueBatch {
	pub(crate) values: Vec<Value>,
}

pub type ValueBatchStream = Pin<Box<dyn Stream<Item = FlowResult<ValueBatch>> + Send + Sync>>;

/// A trait for execution plans that can be executed and produce a stream of value batches.
pub(crate) trait ExecutionPlan: Debug + Send + Sync {
	/// Executes the execution plan and returns a stream of value batches.
	fn execute(&self) -> Result<ValueBatchStream, Error>;
}
