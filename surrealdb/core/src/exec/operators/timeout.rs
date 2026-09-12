//! Timeout operator for query time limits.
//!
//! Wraps an input operator stream and enforces a maximum execution
//! duration. If the timeout expires before the input completes, a
//! `QueryTimedout` error is returned.

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;

use crate::err::EngineError;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::Duration;

/// Applies a timeout to the execution of its input operator.
///
/// If the timeout expires before the input stream completes, an error is returned.
/// This is typically applied as the outermost operator for a query.
#[derive(Debug, Clone)]
pub struct Timeout {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The timeout duration. If None, no timeout is applied.
	pub(crate) timeout: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Timeout {
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		timeout: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			input,
			timeout,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for Timeout {
	fn name(&self) -> &'static str {
		"Timeout"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		if let Some(timeout) = &self.timeout {
			vec![("duration".to_string(), timeout.to_sql())]
		} else {
			vec![]
		}
	}

	fn required_context(&self) -> ContextLevel {
		// Combine timeout expression context with child operator context
		let timeout_ctx =
			self.timeout.as_ref().map(|e| e.required_context()).unwrap_or(ContextLevel::Root);
		timeout_ctx.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// The duration is an expression this operator evaluates, so what it
		// does is this operator's too: a caller deciding how to run the plan —
		// whether the rows of a batch may be run concurrently, whether the
		// producer may be spawned — reads this, and a mode narrower than the
		// work would let a write be reordered.
		match &self.timeout {
			Some(timeout) => self.input.access_mode().combine(timeout.access_mode()),
			None => self.input.access_mode(),
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		if let Some(timeout) = &self.timeout {
			vec![("timeout", timeout)]
		} else {
			vec![]
		}
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// If no timeout is specified, just pass through the input stream
		let Some(timeout_expr) = &self.timeout else {
			let input_stream = buffer_stream(
				self.input.execute(ctx)?,
				self.input.access_mode(),
				self.input.cardinality_hint(),
				ctx.root().ctx.config.exec.operator_buffer_size,
			);
			return Ok(monitor_stream(input_stream, "Timeout", &self.metrics));
		};

		// Evaluate the timeout expression to get the duration
		let timeout_expr = Arc::clone(timeout_expr);
		let input = Arc::clone(&self.input);
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			use crate::exec::EvalContext;

			// Evaluate the timeout expression (no row context needed)
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let timeout_value = timeout_expr.evaluate(eval_ctx).await?;

			// The clause is cast, not coerced, so a duration written down as
			// text names one. `NONE` names no bound at all and the statement
			// runs unbounded; anything that is not a duration is reported as
			// the cast error it is.
			let duration: Option<Duration> =
				timeout_value.cast_to::<Option<Duration>>().map_err(anyhow::Error::new)?;

			// The clause bounds the work the input does, so the input is
			// started only once the clause has been read: a statement whose
			// clause names no duration fails before anything beneath this
			// operator runs.
			let mut input_stream = buffer_stream(
				input.execute(&ctx)?,
				input.access_mode(),
				input.cardinality_hint(),
				ctx.root().ctx.config.exec.operator_buffer_size,
			);

			let Some(duration) = duration else {
				while let Some(batch) = input_stream.next().await {
					yielder.emit(batch?).await;
				}
				return Ok(());
			};

			// Convert our Duration to std::time::Duration for tokio
			let std_duration: std::time::Duration = duration.0;

			// Create a timeout future
			let timeout_instant = common::time::Instant::now() + std_duration;

			loop {
				// Check if we've exceeded the timeout
				let remaining =
					timeout_instant.saturating_duration_since(common::time::Instant::now());
				if remaining.is_zero() {
					Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryTimedout(duration.0))))?;
				}

				// Wait for next batch with timeout
				let batch_result = common::time::timeout(remaining, input_stream.next()).await;

				match batch_result {
					Ok(Some(batch)) => {
						yielder.emit(batch?).await;
					}
					Ok(None) => {
						// Stream completed normally
						break;
					}
					Err(_) => {
						// Timeout expired
						Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryTimedout(
							duration.0
						))))?;
					}
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "Timeout", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{ValuesOperator, physical_expr, root_ctx, val};

	/// The duration is an expression this operator evaluates, so a write in it
	/// is a write of this operator's. A caller reading a narrower mode than the
	/// work would be free to reorder it.
	#[tokio::test]
	async fn a_writing_duration_makes_the_operator_read_write() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(vec![val("1").await]);
		assert_eq!(input.access_mode(), AccessMode::ReadOnly);

		let plain = Timeout::new(Arc::clone(&input), Some(physical_expr("1s", &ctx).await));
		assert_eq!(plain.access_mode(), AccessMode::ReadOnly);

		let writing = Timeout::new(
			Arc::clone(&input),
			Some(physical_expr("eval::surql('RETURN 1s')", &ctx).await),
		);
		assert_eq!(writing.access_mode(), AccessMode::ReadWrite);

		let none = Timeout::new(input, None);
		assert_eq!(none.access_mode(), AccessMode::ReadOnly);
	}
}
