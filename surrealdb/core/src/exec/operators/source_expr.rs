//! SourceExpr operator - evaluates an expression and yields values for iteration in FROM clause.
//!
//! This operator is specifically designed for FROM clause sources, handling:
//! - None/Null: yields no rows (empty stream)
//! - Arrays: yields each element as a separate row (RecordIds are resolved to documents)
//! - RecordId: resolved to its full document
//! - Other values: yields the value as a single row

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream,
	monitor_stream,
};
use crate::val::Value;

/// SourceExpr operator - evaluates an expression and yields values for iteration.
///
/// Unlike ExprPlan (which returns a single value), SourceExpr handles
/// FROM clause semantics:
/// - None/Null → empty stream (no rows)
/// - Array → yield each element (RecordIds resolved when `needs_record_fetch`)
/// - RecordId → resolved to its full document (when `needs_record_fetch`)
/// - Other → yield single value
#[derive(Debug, Clone)]
pub struct SourceExpr {
	/// The expression to evaluate
	pub expr: Arc<dyn PhysicalExpr>,
	/// Whether this source may produce RecordIds that need resolution.
	/// When false, Database context is not required and RecordIds are
	/// yielded as-is (pure value sources like `[1,2,3]`).
	needs_record_fetch: bool,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SourceExpr {
	pub(crate) fn new(expr: Arc<dyn PhysicalExpr>, needs_record_fetch: bool) -> Self {
		Self {
			expr,
			needs_record_fetch,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for SourceExpr {
	fn name(&self) -> &'static str {
		"SourceExpr"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("expr".to_string(), self.expr.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		if self.needs_record_fetch {
			self.expr.required_context().max(ContextLevel::Database)
		} else {
			self.expr.required_context()
		}
	}

	fn access_mode(&self) -> AccessMode {
		// Delegate to the wrapped expression
		self.expr.access_mode()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("expr", &self.expr)]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let expr = self.expr.clone();
		let ctx = ctx.clone();
		let needs_fetch = self.needs_record_fetch;

		let stream = async_stream::try_stream! {
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let value = expr.evaluate(eval_ctx).await?;

			match value {
				// Arrays yield their elements, filtering out NONE/NULL
				// entries to match the old compute path's behaviour.
				// Yielded in chunks so downstream Limit can short-circuit
				// without fetching the entire array.
				Value::Array(arr) => {
					let values: Vec<Value> = arr
						.into_iter()
						.filter(|v| !matches!(v, Value::None | Value::Null))
						.collect();

					if needs_fetch {
						const BATCH_SIZE: usize = 64;
						for chunk in values.chunks(BATCH_SIZE) {
							let mut batch = chunk.to_vec();
							super::fetch::batch_fetch_in_place(&ctx, &mut batch).await?;
							batch.retain(|v| !matches!(v, Value::None));
							if !batch.is_empty() {
								yield ValueBatch { values: batch };
							}
						}
					} else if !values.is_empty() {
						yield ValueBatch { values };
					}
				}
				// NONE and NULL yield no rows (empty source), matching
				// the behaviour of the old compute path.
				Value::None | Value::Null => {}
				// Single RecordId: resolve to full document when needed.
				Value::RecordId(ref rid) if needs_fetch => {
					let fetched = super::fetch::fetch_record(&ctx, rid).await?;
					if !matches!(fetched, Value::None) {
						yield ValueBatch { values: vec![fetched] };
					}
				}
				// Everything else yields a single row
				other => {
					yield ValueBatch { values: vec![other] };
				}
			}
		};

		Ok(monitor_stream(Box::pin(stream), "SourceExpr", &self.metrics))
	}

	fn is_scalar(&self) -> bool {
		// SourceExpr is not scalar - it can yield multiple values
		false
	}
}

impl ToSql for SourceExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
	}
}
