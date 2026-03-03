//! Control flow operators - RETURN, THROW, BREAK, CONTINUE.
//!
//! These operators signal control flow changes to parent operators (blocks, loops).
//! They don't produce value streams in the normal sense - instead they return
//! control flow signals via `FlowResult`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream,
	buffer_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Control flow operator - handles RETURN, THROW, BREAK, CONTINUE.
///
/// This operator signals control flow changes to parent operators.
/// - RETURN: Evaluates inner plan, returns `ControlFlow::Return(value)`
/// - THROW: Evaluates inner plan, returns `ControlFlow::Err(Error::Thrown(...))`
/// - BREAK: Returns `ControlFlow::Break` immediately
/// - CONTINUE: Returns `ControlFlow::Continue` immediately
#[derive(Debug)]
pub struct ReturnPlan {
	pub inner: Arc<dyn ExecOperator>,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ReturnPlan {
	pub(crate) fn new(inner: Arc<dyn ExecOperator>) -> Self {
		Self {
			inner,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for ReturnPlan {
	fn name(&self) -> &'static str {
		"Return"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![]
	}

	fn required_context(&self) -> ContextLevel {
		self.inner.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.inner.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.inner.cardinality_hint()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let inner = self.inner.clone();
		let ctx = ctx.clone();

		// Check if inner plan is scalar (like `RETURN 1 + 2`) vs query (like `RETURN SELECT
		// ...`) Query results should stay wrapped in array; scalar results can be
		// unwrapped
		let inner_is_scalar = inner.is_scalar();

		// Return a stream that executes the inner plan and produces the control flow signal
		Ok(Box::pin(futures::stream::once(async move {
			// Execute inner plan and collect values
			let mut stream = match inner.execute(&ctx) {
				Ok(s) => buffer_stream(
					s,
					inner.access_mode(),
					inner.cardinality_hint(),
					ctx.ctx().config().limits.operator_buffer_size,
				),
				Err(ctrl) => return Err(ctrl),
			};

			let mut values = Vec::new();
			while let Some(batch_result) = stream.next().await {
				match batch_result {
					Ok(batch) => values.extend(batch.values),
					Err(ControlFlow::Return(v)) => {
						values.push(v);
						break;
					}
					Err(e) => return Err(e),
				}
			}

			// Get the result value
			// For scalar expressions (like `RETURN 1 + 2`), unwrap single values
			// For query expressions (like `RETURN SELECT ...`), keep array wrapping
			let value = if inner_is_scalar {
				// Scalar: unwrap single value, use NONE for empty
				if values.len() == 1 {
					values.into_iter().next().expect("values verified non-empty")
				} else if values.is_empty() {
					Value::None
				} else {
					Value::Array(crate::val::Array(values))
				}
			} else {
				// Query: always wrap in array (matches SELECT behavior)
				Value::Array(crate::val::Array(values))
			};

			Err(ControlFlow::Return(value))
		})))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.inner]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}
}
