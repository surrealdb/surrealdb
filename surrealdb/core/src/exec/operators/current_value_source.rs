//! CurrentValueSource operator - yields the execution context's current value.
//!
//! This is a leaf operator in the DAG that reads the `current_value` from the
//! `ExecutionContext` and yields it as a single-element batch. It serves as the
//! explicit input binding for correlated sub-execution (e.g., graph lookups).

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream, monitor_stream,
};
use crate::val::Value;

/// Leaf operator that yields the execution context's `current_value`.
///
/// Used as the source of graph/reference lookup operator chains. When
/// `LookupPart` evaluates a graph traversal for a specific `RecordId`,
/// it sets `current_value` on the `ExecutionContext` and executes the
/// operator chain rooted at this node.
///
/// In `EXPLAIN` output, this appears as:
/// ```text
/// CurrentValueSource [ctx: Db]
/// ```
#[derive(Debug, Clone)]
pub struct CurrentValueSource {
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl CurrentValueSource {
	pub(crate) fn new() -> Self {
		Self {
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for CurrentValueSource {
	fn name(&self) -> &'static str {
		"CurrentValueSource"
	}

	fn required_context(&self) -> ContextLevel {
		// The current_value is set at any context level, but graph lookups
		// that consume this will need database context. Keep this minimal.
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let value = ctx.current_value().cloned().unwrap_or(Value::None);

		Ok(monitor_stream(
			Box::pin(stream::once(async move {
				Ok(ValueBatch {
					values: vec![value],
				})
			})),
			"CurrentValueSource",
			&self.metrics,
		))
	}
}
