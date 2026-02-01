use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	AccessMode, ContextLevel, ExecutionContext, FlowResult, OperatorPlan, PhysicalExpr,
	ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::val::Duration;

/// Applies a timeout to the execution of its input operator.
///
/// If the timeout expires before the input stream completes, an error is returned.
/// This is typically applied as the outermost operator for a query.
#[derive(Debug, Clone)]
pub struct Timeout {
	pub(crate) input: Arc<dyn OperatorPlan>,
	/// The timeout duration. If None, no timeout is applied.
	pub(crate) timeout: Option<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl OperatorPlan for Timeout {
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
		// Timeout inherits its input's context requirements
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// Timeout is transparent to access mode
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;

		// If no timeout is specified, just pass through the input stream
		let Some(timeout_expr) = &self.timeout else {
			return Ok(input_stream);
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
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Invalid timeout value: {}", e)))?;

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

		Ok(Box::pin(timeout_stream))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_timeout_name() {
		use crate::exec::operators::scan::Scan;

		let scan = Arc::new(Scan {
			source: "test".to_string(),
			fields: None,
			condition: None,
		});

		let timeout = Timeout {
			input: scan,
			timeout: None,
		};

		assert_eq!(timeout.name(), "Timeout");
	}
}
