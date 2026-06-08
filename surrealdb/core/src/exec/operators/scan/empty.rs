//! EmptyScan operator — produces zero rows.
//!
//! Selected when the planner can statically prove a SELECT cannot match any
//! rows — for example a contradictory range (`a > 10 AND a < 5`), an empty
//! `IN []`, or a WHERE clause folded to `false`. Returning an empty stream
//! is strictly cheaper than reaching storage and filtering.

use std::sync::Arc;

use futures::stream;

use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatchStream, monitor_stream,
};

/// Operator that produces no rows.
///
/// Used when the planner can statically prove a SELECT cannot match any
/// rows (contradictory range, empty `IN []`, etc.).
#[derive(Debug, Clone)]
pub struct EmptyScan {
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl EmptyScan {
	pub(crate) fn new() -> Self {
		Self {
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl Default for EmptyScan {
	fn default() -> Self {
		Self::new()
	}
}

impl ExecOperator for EmptyScan {
	fn name(&self) -> &'static str {
		"EmptyScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		Vec::new()
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Bounded(0)
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		Ok(monitor_stream(Box::pin(stream::empty()), "EmptyScan", &self.metrics))
	}
}
