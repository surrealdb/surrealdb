//! Timeout operator for query time limits.
//!
//! Wraps an input operator stream and enforces a maximum execution
//! duration. If the timeout expires before the input completes, a
//! `QueryTimedout` error is returned.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
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

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
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
		// Timeout is transparent to access mode
		self.input.access_mode()
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
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);

		// If no timeout is specified, just pass through the input stream
		let Some(timeout_expr) = &self.timeout else {
			return Ok(monitor_stream(input_stream, "Timeout", &self.metrics));
		};

		// Evaluate the timeout expression to get the duration
		let timeout_expr = timeout_expr.clone();
		let ctx = ctx.clone();

		let timeout_stream = async_stream::try_stream! {
			use crate::exec::EvalContext;

			// Evaluate the timeout expression (no row context needed)
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let timeout_value = timeout_expr.evaluate(eval_ctx).await?;

			// Convert to duration
			let duration: Duration = timeout_value
				.coerce_to::<Duration>()
				.context("Invalid timeout value")?;

			// Convert our Duration to std::time::Duration for tokio
			let std_duration: std::time::Duration = duration.0;

			// Create a timeout future
			let timeout_instant = tokio::time::Instant::now() + std_duration;

			futures::pin_mut!(input_stream);

			loop {
				// Check if we've exceeded the timeout
				let remaining = timeout_instant.saturating_duration_since(tokio::time::Instant::now());
				if remaining.is_zero() {
					Err(ControlFlow::Err(anyhow::anyhow!(Error::QueryTimedout(duration))))?;
				}

				// Wait for next batch with timeout
				let batch_result = tokio::time::timeout(
					remaining,
					input_stream.next()
				).await;

				match batch_result {
					Ok(Some(batch)) => {
						yield batch?;
					}
					Ok(None) => {
						// Stream completed normally
						break;
					}
					Err(_) => {
						// Timeout expired
						Err(ControlFlow::Err(anyhow::anyhow!(Error::QueryTimedout(duration))))?;
					}
				}
			}
		};

		Ok(monitor_stream(Box::pin(timeout_stream), "Timeout", &self.metrics))
	}
}
