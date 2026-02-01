//! Limit operator - applies LIMIT and OFFSET (START) to a stream.

use std::sync::Arc;

use async_trait::async_trait;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecutionContext, FlowResult, OperatorPlan,
	PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::ControlFlow;

/// Applies LIMIT and OFFSET (START) to an input stream.
///
/// Evaluates the limit and offset expressions at execution start, then
/// applies them to the input stream by skipping and limiting values.
#[derive(Debug, Clone)]
pub struct Limit {
	pub(crate) input: Arc<dyn OperatorPlan>,
	/// Maximum number of values to return (LIMIT clause)
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	/// Number of values to skip (START/OFFSET clause)
	pub(crate) offset: Option<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl OperatorPlan for Limit {
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

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Evaluate limit and offset expressions upfront
		let eval_ctx = EvalContext::from_exec_ctx(ctx);

		let limit_value = match &self.limit {
			Some(expr) => {
				let value =
					futures::executor::block_on(expr.evaluate(eval_ctx.clone())).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to evaluate LIMIT: {}", e))
					})?;
				Some(coerce_to_usize(&value).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("LIMIT must be a non-negative integer: {}", e))
				})?)
			}
			None => None,
		};

		let offset_value = match &self.offset {
			Some(expr) => {
				let value = futures::executor::block_on(expr.evaluate(eval_ctx)).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("Failed to evaluate START: {}", e))
				})?;
				coerce_to_usize(&value).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("START must be a non-negative integer: {}", e))
				})?
			}
			None => 0,
		};

		// If limit is 0, return empty stream immediately
		if limit_value == Some(0) {
			return Ok(Box::pin(futures::stream::empty()));
		}

		let input_stream = self.input.execute(ctx)?;

		// Create a stream that applies offset and limit
		let limited = LimitStream::new(input_stream, offset_value, limit_value);

		Ok(Box::pin(limited))
	}
}

/// Coerce a Value to usize for limit/offset
fn coerce_to_usize(value: &crate::val::Value) -> Result<usize, String> {
	use crate::val::Value;

	match value {
		Value::Number(n) => {
			// Try to get as int first
			let i = n.clone().to_int();
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

/// A stream wrapper that applies offset and limit to batches
struct LimitStream {
	inner: ValueBatchStream,
	offset: usize,
	limit: Option<usize>,
	/// How many values we've skipped so far
	skipped: usize,
	/// How many values we've emitted so far
	emitted: usize,
	/// Whether we're done (limit reached or inner stream exhausted)
	done: bool,
}

impl LimitStream {
	fn new(inner: ValueBatchStream, offset: usize, limit: Option<usize>) -> Self {
		Self {
			inner,
			offset,
			limit,
			skipped: 0,
			emitted: 0,
			done: false,
		}
	}
}

impl futures::Stream for LimitStream {
	type Item = Result<ValueBatch, ControlFlow>;

	fn poll_next(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		use std::task::Poll;

		if self.done {
			return Poll::Ready(None);
		}

		// Check if we've hit the limit
		if let Some(limit) = self.limit {
			if self.emitted >= limit {
				self.done = true;
				return Poll::Ready(None);
			}
		}

		// Poll the inner stream
		let inner = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) };
		match inner.poll_next(cx) {
			Poll::Ready(None) => {
				self.done = true;
				Poll::Ready(None)
			}
			Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
			Poll::Ready(Some(Ok(batch))) => {
				let mut values = batch.values;

				// Apply offset - skip values until we've skipped enough
				if self.skipped < self.offset {
					let to_skip = (self.offset - self.skipped).min(values.len());
					values = values.into_iter().skip(to_skip).collect();
					self.skipped += to_skip;
				}

				// Apply limit - only take as many as we need
				if let Some(limit) = self.limit {
					let remaining = limit.saturating_sub(self.emitted);
					if values.len() > remaining {
						values.truncate(remaining);
					}
				}

				self.emitted += values.len();

				// Check if we've hit the limit
				if let Some(limit) = self.limit {
					if self.emitted >= limit {
						self.done = true;
					}
				}

				// Only emit non-empty batches
				if values.is_empty() {
					// Need to continue polling
					cx.waker().wake_by_ref();
					Poll::Pending
				} else {
					Poll::Ready(Some(Ok(ValueBatch {
						values,
					})))
				}
			}
			Poll::Pending => Poll::Pending,
		}
	}
}
