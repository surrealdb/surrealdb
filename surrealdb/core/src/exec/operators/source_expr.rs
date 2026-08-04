//! SourceExpr operator - evaluates an expression and yields values for iteration in FROM clause.
//!
//! This operator is specifically designed for FROM clause sources, handling:
//! - None/Null: yields no rows (empty stream)
//! - Arrays: yields each element as a separate row, resolving RecordIds to full documents
//! - RecordId: fetches the full document and yields it
//! - Other values: yields the value as a single row
//!
//! RecordId resolution ensures downstream pipeline operators (Filter, Sort, etc.)
//! can access document fields, matching the behaviour of RecordIdScan and TableScan.

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, OutputShape, ValueBatch,
	ValueBatchStream, monitor_stream,
};
use crate::val::Value;

/// SourceExpr operator - evaluates an expression and yields values for iteration.
///
/// Unlike ExprPlan (which returns a single value), SourceExpr handles
/// FROM clause semantics:
/// - None/Null → empty stream (no rows)
/// - Array → yield each element
/// - Other → yield single value
#[derive(Debug, Clone)]
pub struct SourceExpr {
	/// The expression to evaluate
	pub expr: Arc<dyn PhysicalExpr>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SourceExpr {
	pub(crate) fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			expr,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for SourceExpr {
	fn name(&self) -> &'static str {
		"SourceExpr"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("expr".to_string(), self.expr.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// The expression may yield RecordId values that need resolving to full
		// documents, which requires database-level context for transaction access.
		self.expr.required_context().max(ContextLevel::Database)
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
		let expr = Arc::clone(&self.expr);
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// In a correlated subquery, ScalarSubquery binds the outer document
			// as `$this`. Use it as current_value so that bare field paths in
			// the FROM expression resolve against the outer row (e.g.
			// `FROM data.files` where `data` is a record link on the outer row).
			let this_value = ctx.value("this").cloned();
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let eval_ctx = match this_value {
				Some(ref v) => eval_ctx.with_value(v),
				None => eval_ctx,
			};
			let value = expr.evaluate(eval_ctx).await?;

			match value {
				Value::Array(arr) => {
					let mut values: Vec<Value> = arr
						.into_iter()
						.filter(|v| !matches!(v, Value::None | Value::Null))
						.collect();
					if !values.is_empty() {
						// Resolve RecordId values to full documents so that
						// downstream operators (Filter, Sort, etc.) can access
						// document fields — matching RecordIdScan/TableScan.
						super::fetch::batch_fetch_in_place(&ctx, &mut values).await?;
						values.retain(|v| !matches!(v, Value::None | Value::Null));
						if !values.is_empty() {
							yielder
								.emit(ValueBatch {
									values,
								})
								.await;
						}
					}
				}
				Value::None | Value::Null => {}
				Value::RecordId(ref rid) => {
					let fetched = super::fetch::fetch_record(&ctx, rid).await?;
					if !matches!(fetched, Value::None) {
						yielder
							.emit(ValueBatch {
								values: vec![fetched],
							})
							.await;
					}
				}
				other => {
					yielder
						.emit(ValueBatch {
							values: vec![other],
						})
						.await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "SourceExpr", &self.metrics))
	}

	fn output_shape(&self) -> OutputShape {
		// SourceExpr is not scalar - it can yield multiple values
		OutputShape::Rows
	}
}

impl ToSql for SourceExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
	}
}
