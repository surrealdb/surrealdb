//! Limit operator - applies LIMIT and OFFSET (START) to a stream.

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use tracing::instrument;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::{Number, Value};

/// Applies LIMIT and OFFSET (START) to an input stream.
///
/// Evaluates the limit and offset expressions at execution start, before its
/// input is opened, then applies them to the input stream by skipping and
/// limiting values. A limit of zero runs the input with its storage scans
/// reading no record (see [`crate::exec::zero_limit`]) and emits nothing.
#[derive(Debug, Clone)]
pub struct Limit {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Maximum number of values to return (LIMIT clause)
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	/// Number of values to skip (START/OFFSET clause)
	pub(crate) offset: Option<Arc<dyn PhysicalExpr>>,
	/// The id the storage scans of `input` are bound to.
	zero_limit: u64,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Limit {
	/// Create a new Limit operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		limit: Option<Arc<dyn PhysicalExpr>>,
		offset: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		let zero_limit = crate::exec::zero_limit::next_id();
		if limit.is_some() {
			crate::exec::zero_limit::bind_scans(&input, zero_limit);
		}
		Self {
			input,
			limit,
			offset,
			zero_limit,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

/// Which paging clause a count was written for. Only the error it raises
/// depends on this; the contract below is the same for both.
#[derive(Clone, Copy, Debug)]
pub(crate) enum PagingClause {
	Limit,
	Start,
}

impl PagingClause {
	fn rejects(self, value: String) -> anyhow::Error {
		match self {
			PagingClause::Limit => anyhow::Error::new(crate::exec::Error::InvalidLimit {
				value,
			}),
			PagingClause::Start => anyhow::Error::new(crate::exec::Error::InvalidStart {
				value,
			}),
		}
	}
}

/// The row count a paging clause names.
///
/// A count is written as a non-negative integer that fits in a `u32`, and
/// every other value is rejected rather than read as one: a float or decimal
/// is not truncated, `NONE` and `NULL` are not read as zero, and a count too
/// large to fit is an error rather than a wrap — so the answer does not depend
/// on the width of a pointer on the machine running the query.
pub(crate) fn paging_count(value: &Value, clause: PagingClause) -> Result<u32, anyhow::Error> {
	match value {
		Value::Number(Number::Int(count)) if *count >= 0 => {
			u32::try_from(*count).map_err(|_| clause.rejects(count.to_string()))
		}
		other => Err(clause.rejects(other.clone().into_raw_string())),
	}
}
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
		let input = Arc::clone(&self.input);
		let access_mode = self.access_mode().combine(self.input.access_mode());
		let zero_limit = self.zero_limit;
		let limit_expr = self.limit.clone();
		let offset_expr = self.offset.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);

			// Evaluate limit expression
			let limit_value = match &limit_expr {
				Some(expr) => {
					let value = expr.evaluate(eval_ctx.clone()).await?;
					Some(paging_count(&value, PagingClause::Limit).map_err(ControlFlow::Err)?
						as usize)
				}
				None => None,
			};

			// Evaluate offset expression
			let offset = match &offset_expr {
				Some(expr) => {
					let value = expr.evaluate(eval_ctx).await?;
					paging_count(&value, PagingClause::Start).map_err(ControlFlow::Err)? as usize
				}
				None => 0,
			};

			// Opened once the clauses are known, so a zero limit reaches the
			// input's scans before any of them reads.
			let input_ctx = if limit_value == Some(0) {
				ctx.with_zero_limit(zero_limit)
			} else {
				ctx.clone()
			};
			let mut input_stream = buffer_stream(
				input.execute(&input_ctx)?,
				access_mode,
				input.cardinality_hint(),
				ctx.root().ctx.config.exec.operator_buffer_size,
			);
			// Nothing is emitted, but the input is drained: what its scans
			// check before reading, and the rows of a source that is not a
			// read of storage, are owed as for any other limit.
			if limit_value == Some(0) {
				while let Some(batch) = input_stream.next().await {
					batch?;
				}
				return Ok(());
			}

			let mut skipped = 0usize;
			let mut emitted = 0usize;

			while let Some(batch_result) = input_stream.next().await {
				let mut batch = batch_result?;

				// Apply offset - skip values until we've skipped enough
				if skipped < offset {
					let to_skip = (offset - skipped).min(batch.len());
					skipped += to_skip;
					if to_skip >= batch.len() {
						// Entire batch is within the offset window, skip it
						continue;
					}
					// Remove the prefix in-place (single memmove, no allocation)
					batch.values_mut().drain(..to_skip);
				}

				// Apply limit - truncate is a no-op when len <= remaining
				if let Some(limit) = limit_value {
					batch.values_mut().truncate(limit.saturating_sub(emitted));
				}

				emitted += batch.len();

				// Only emit non-empty batches
				if !batch.is_empty() {
					yielder.emit(batch).await;
				}

				// Stop once the limit is exhausted (also handles limit == 0)
				if limit_value.is_some_and(|l| emitted >= l) {
					break;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "Limit", &self.metrics))
	}
}
