//! Limit operator - applies LIMIT and OFFSET (START) to a stream.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

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
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Limit {
	/// Create a new Limit operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		limit: Option<Arc<dyn PhysicalExpr>>,
		offset: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			input,
			limit,
			offset,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Coerce a Value to usize for limit/offset.
	fn coerce_to_usize(value: &Value) -> Result<usize, String> {
		match value {
			Value::Number(n) => {
				let i = n.to_int();
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
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Limit {
	fn name(&self) -> &'static str {
		"Limit"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = Vec::with_capacity(2);
		if let Some(limit) = &self.limit {
			attrs.push(("limit".to_string(), limit.to_sql()));
		}
		if let Some(offset) = &self.offset {
			attrs.push(("offset".to_string(), offset.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Combine limit/offset expression contexts with child operator context
		let exprs_ctx = [self.limit.as_ref(), self.offset.as_ref()]
			.into_iter()
			.flatten()
			.map(|e| e.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		exprs_ctx.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's mode with limit/offset expressions
		[self.limit.as_ref(), self.offset.as_ref()]
			.into_iter()
			.flatten()
			.map(|e| e.access_mode())
			.fold(self.input.access_mode(), AccessMode::combine)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut exprs = Vec::with_capacity(2);
		if let Some(limit) = &self.limit {
			exprs.push(("limit", limit));
		}
		if let Some(offset) = &self.offset {
			exprs.push(("offset", offset));
		}
		exprs
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	#[instrument(name = "Limit::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);

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
					Some(Limit::coerce_to_usize(&value).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("LIMIT must be a non-negative integer: {}", e))
					})?)
				}
				None => None,
			};

			// Evaluate offset expression
			let offset = match &offset_expr {
				Some(expr) => {
					let value = expr.evaluate(eval_ctx).await.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to evaluate START: {}", e))
					})?;
					Limit::coerce_to_usize(&value).map_err(|e| {
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

				// Apply limit - truncate is a no-op when len <= remaining
				if let Some(limit) = limit_value {
					batch.values.truncate(limit.saturating_sub(emitted));
				}

				emitted += batch.values.len();

				// Only emit non-empty batches
				if !batch.values.is_empty() {
					yield batch;
				}

				// Stop once the limit is exhausted (also handles limit == 0)
				if limit_value.is_some_and(|l| emitted >= l) {
					break;
				}
			}
		};

		Ok(monitor_stream(Box::pin(limited), "Limit", &self.metrics))
	}
}
