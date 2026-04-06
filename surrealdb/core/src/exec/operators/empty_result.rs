//! EmptyResult operator — provably empty result set.
//!
//! Used when the planner can statically determine that a query will produce
//! zero rows (e.g., `WHERE id IN []`). Analogous to DataFusion's `EmptyExec`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;

use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	ValueBatchStream, monitor_stream,
};

/// Operator that always produces an empty stream.
///
/// Created by the planner when a condition is provably unsatisfiable
/// (e.g., `id IN []`), avoiding any storage I/O or pipeline overhead.
#[derive(Debug, Clone)]
pub struct EmptyResult {
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl EmptyResult {
	pub(crate) fn new() -> Self {
		Self {
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for EmptyResult {
	fn name(&self) -> &'static str {
		"EmptyResult"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> crate::exec::CardinalityHint {
		crate::exec::CardinalityHint::AtMostOne
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		Ok(monitor_stream(Box::pin(stream::empty()), "EmptyResult", &self.metrics))
	}
}
