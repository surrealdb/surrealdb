//! UnwrapExactlyOne operator - implements SELECT ... FROM ONLY semantics.
//!
//! This operator enforces that no more than one result is produced:
//! - 0 results → returns NONE
//! - 1 result → returns that value (unwrapped from array)
//! - >1 results → returns `ExecError::SingleOnlyOutput`

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;

use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, Error as ExecError, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, OutputShape, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Unwraps the one result of a `SELECT ... FROM ONLY ...`.
///
/// It collects all values from the input stream and:
/// - Returns NONE if there are no results
/// - Returns the single value (unwrapped) if exactly one result
/// - Returns `ExecError::SingleOnlyOutput` if more than one result
///
/// No rows is `NONE` whatever the source. What `ONLY` asks is that the
/// statement not answer with several rows, and none is not several — a
/// missing record, a predicate nothing satisfies and an empty source all
/// answer alike.
#[derive(Debug, Clone)]
pub struct UnwrapExactlyOne {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl UnwrapExactlyOne {
	pub(crate) fn new(input: Arc<dyn ExecOperator>) -> Self {
		Self {
			input,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
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

	fn output_shape(&self) -> OutputShape {
		// The result is unwrapped (not wrapped in an array)
		OutputShape::Scalar
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// Collect all values from the input stream
			let mut collected: Vec<Value> = Vec::new();

			while let Some(batch_result) = input_stream.next().await {
				let batch = batch_result?;
				collected.extend(batch.into_values());

				// Early exit if we already have more than one value
				if collected.len() > 1 {
					Err(ControlFlow::Err(anyhow::anyhow!(ExecError::SingleOnlyOutput)))?;
				}
			}

			// Stream completed - check result count
			// More than 1 was handled above with early exit
			if collected.is_empty() {
				// No rows: the statement answers NONE.
				yielder.emit(ValueBatch::new(vec![Value::None])).await;
			} else {
				let result = collected.pop().expect("collected has exactly one element");

				// Emit the single result
				yielder.emit(ValueBatch::new(vec![result])).await;
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "UnwrapExactlyOne", &self.metrics))
	}
}
