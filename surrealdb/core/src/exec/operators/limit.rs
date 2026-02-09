//! Limit operator - applies LIMIT and OFFSET (START) to a stream.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	PhysicalExpr, ValueBatch, ValueBatchStream, instrument_stream,
};
use crate::expr::ControlFlow;

/// Applies LIMIT and OFFSET (START) to an input stream.
///
/// Evaluates the limit and offset expressions at execution start, then
/// applies them to the input stream by skipping and limiting values.
#[derive(Debug, Clone)]
pub struct Limit {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Maximum number of values to return (LIMIT clause)
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	/// Number of values to skip (START/OFFSET clause)
	pub(crate) offset: Option<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl ExecOperator for Limit {
	fn name(&self) -> &'static str {
		"Limit"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = Vec::new();
		if let Some(limit) = &self.limit {
			attrs.push(("limit".to_string(), limit.to_sql()));
		}
		if let Some(offset) = &self.offset {
			attrs.push(("offset".to_string(), offset.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Inherit child requirements
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's mode with limit/offset expressions
		let mut mode = self.input.access_mode();
		if let Some(limit) = &self.limit {
			mode = mode.combine(limit.access_mode());
		}
		if let Some(offset) = &self.offset {
			mode = mode.combine(offset.access_mode());
		}
		mode
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	#[instrument(name = "Limit::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;

		let limit_expr = self.limit.clone();
		let offset_expr = self.offset.clone();
		let ctx = ctx.clone();

		let limited = async_stream::try_stream! {
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);

			// Evaluate limit expression
			let limit_value = match &limit_expr {
				Some(expr) => {
					let value = expr.evaluate(eval_ctx.clone()).await.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to evaluate LIMIT: {}", e))
					})?;
					Some(coerce_to_usize(&value).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("LIMIT must be a non-negative integer: {}", e))
					})?)
				}
				None => None,
			};

			// If limit is 0, produce no output
			if limit_value == Some(0) {
				return;
			}

			// Evaluate offset expression
			let offset = match &offset_expr {
				Some(expr) => {
					let value = expr.evaluate(eval_ctx).await.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to evaluate START: {}", e))
					})?;
					coerce_to_usize(&value).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("START must be a non-negative integer: {}", e))
					})?
				}
				None => 0,
			};

			let mut skipped = 0usize;
			let mut emitted = 0usize;

			futures::pin_mut!(input_stream);

			while let Some(batch_result) = input_stream.next().await {
				let mut batch = batch_result?;

				// Apply offset - skip values until we've skipped enough
				if skipped < offset {
					let to_skip = (offset - skipped).min(batch.values.len());
					skipped += to_skip;
					if to_skip >= batch.values.len() {
						// Entire batch is within the offset window, skip it
						continue;
					}
					// Remove the prefix in-place (single memmove, no allocation)
					batch.values.drain(..to_skip);
				}

				// Apply limit - only take as many as we need
				if let Some(limit) = limit_value {
					let remaining = limit.saturating_sub(emitted);
					if batch.values.len() > remaining {
						batch.values.truncate(remaining);
					}
				}

				emitted += batch.values.len();

				// Only emit non-empty batches
				if !batch.values.is_empty() {
					yield batch;
				}

				// Stop if we've hit the limit
				if let Some(limit) = limit_value
					&& emitted >= limit
				{
					break;
				}
			}
		};

		Ok(instrument_stream(Box::pin(limited), "Limit"))
	}
}

/// Coerce a Value to usize for limit/offset
fn coerce_to_usize(value: &crate::val::Value) -> Result<usize, String> {
	use crate::val::Value;

	match value {
		Value::Number(n) => {
			// Try to get as int first
			let i = (*n).to_int();
			if i >= 0 {
				Ok(i as usize)
			} else {
				Err(format!("expected non-negative integer, got {}", i))
			}
		}
		Value::None | Value::Null => Ok(0),
		_ => Err(format!("expected integer, got {:?}", value)),
	}
}
