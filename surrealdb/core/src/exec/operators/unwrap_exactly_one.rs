//! UnwrapExactlyOne operator - implements SELECT ... FROM ONLY semantics.
//!
//! This operator enforces that exactly one result is produced:
//! - 0 results → returns NONE (if `none_on_empty`) or `Error::SingleOnlyOutput`
//! - 1 result → returns that value (unwrapped from array)
//! - >1 results → returns `Error::SingleOnlyOutput`

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Unwraps exactly one result from the input stream.
///
/// This operator implements the `ONLY` keyword in `SELECT ... FROM ONLY ...`.
/// It collects all values from the input stream and:
/// - Returns NONE if no results and `none_on_empty` is true (table scans)
/// - Returns `Error::SingleOnlyOutput` if no results and `none_on_empty` is false (array sources)
/// - Returns the single value (unwrapped) if exactly one result
/// - Returns `Error::SingleOnlyOutput` if more than one result
#[derive(Debug, Clone)]
pub struct UnwrapExactlyOne {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// If true, return NONE when input produces 0 results (used for table scans).
	/// If false, return an error when input produces 0 results (used for array sources).
	pub(crate) none_on_empty: bool,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl UnwrapExactlyOne {
	pub(crate) fn new(input: Arc<dyn ExecOperator>, none_on_empty: bool) -> Self {
		Self {
			input,
			none_on_empty,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for UnwrapExactlyOne {
	fn name(&self) -> &'static str {
		"UnwrapExactlyOne"
	}

	fn required_context(&self) -> ContextLevel {
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn is_scalar(&self) -> bool {
		// The result is unwrapped (not wrapped in an array)
		true
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let none_on_empty = self.none_on_empty;

		let unwrap_stream = async_stream::try_stream! {
			futures::pin_mut!(input_stream);

			// Collect all values from the input stream
			let mut collected: Vec<Value> = Vec::new();

			while let Some(batch_result) = input_stream.next().await {
				let batch = batch_result?;
				collected.extend(batch.values);

				// Early exit if we already have more than one value
				if collected.len() > 1 {
					Err(ControlFlow::Err(anyhow::anyhow!(Error::SingleOnlyOutput)))?;
				}
			}

			// Stream completed - check result count
			// More than 1 was handled above with early exit
			if collected.is_empty() {
				if none_on_empty {
					// Table scan with no results → return NONE
					yield ValueBatch {
						values: vec![Value::None],
					};
				} else {
					// Array source with no elements → error
					Err(ControlFlow::Err(anyhow::anyhow!(Error::SingleOnlyOutput)))?;
				}
			} else {
				let result = collected.pop().expect("collected has exactly one element");

				// Emit the single result
				yield ValueBatch {
					values: vec![result],
				};
			}
		};

		Ok(monitor_stream(Box::pin(unwrap_stream), "UnwrapExactlyOne", &self.metrics))
	}
}
