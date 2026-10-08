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

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use common::future::stream::{self, Yielder};
#[cfg(test)]
use surrealdb_types::{SqlFormat, ToSql};

#[cfg(test)]
use crate::exec::context::{ContextLevel, ExecutionContext};
#[cfg(test)]
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
#[cfg(test)]
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, OutputShape, ValueBatch,
	ValueBatchStream, monitor_stream,
};
#[cfg(test)]
use crate::val::Value;

#[cfg(test)]
/// SourceExpr operator - evaluates an expression and yields values for iteration.
///
/// An expression in a FROM clause names rows to iterate, not one value, so
/// SourceExpr gives it FROM clause semantics:
/// - None/Null → empty stream (no rows)
/// - Array → yield each element
/// - Other → yield single value
///
/// An array written in `FROM` is a list of sources, as the per-row engine
/// reads it: an element that evaluates to an array contributes its elements,
/// one level deep, so `FROM [a->b, c]` reads the records the traversal reaches
/// as `FROM a->b, c` does, while an array held in a parameter or returned by a
/// call is one source whose elements are its rows as they are.
#[derive(Debug, Clone)]
pub struct SourceExpr {
	/// The expression to evaluate
	pub expr: Arc<dyn PhysicalExpr>,
	/// Whether `expr` is an array written in `FROM`, whose elements that are
	/// arrays contribute their elements.
	written: bool,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl SourceExpr {
	#[cfg(test)]
	pub(crate) fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			expr,
			written: false,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// The source of an array written in `FROM`.
	pub(crate) fn written_array(expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			written: true,
			..Self::new(expr)
		}
	}
}
#[cfg(test)]
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
		let written = self.written;
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// In a correlated subquery, ScalarSubquery binds the outer document
			// as `$this`. Use it as both the current value and the document
			// root, so that bare field paths in the FROM expression resolve
			// against the outer row (e.g. `FROM data.files` where `data` is a
			// record link on the outer row) and so do the reads inside a
			// nested expression — a method's argument, a dynamic index — which
			// are evaluated against the root rather than against the position
			// the path has reached.
			let this_value = ctx.value("this").cloned();
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let eval_ctx = match this_value {
				Some(ref v) => eval_ctx.with_value_and_doc(v),
				None => eval_ctx,
			};
			let value = super::evaluate_from_source(&*expr, eval_ctx).await?;

			match value {
				Value::Array(arr) => {
					let mut values: Vec<Value> = Vec::with_capacity(arr.len());
					for value in arr {
						match value {
							Value::Array(inner) if written => values.extend(inner),
							other => values.push(other),
						}
					}
					values.retain(|v| !matches!(v, Value::None | Value::Null));
					if !values.is_empty() {
						// Resolve RecordId values to full documents so that
						// downstream operators (Filter, Sort, etc.) can access
						// document fields — matching RecordIdScan/TableScan.
						super::fetch::batch_fetch_in_place(&ctx, &mut values).await?;
						values.retain(|v| !matches!(v, Value::None | Value::Null));
						if !values.is_empty() {
							yielder.emit(ValueBatch::new(values)).await;
						}
					}
				}
				Value::None | Value::Null => {}
				Value::RecordId(ref rid) => {
					let fetched = super::fetch::fetch_record(&ctx, rid).await?;
					if !matches!(fetched, Value::None) {
						yielder.emit(ValueBatch::new(vec![fetched])).await;
					}
				}
				other => {
					yielder.emit(ValueBatch::new(vec![other])).await;
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

#[cfg(test)]
impl ToSql for SourceExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
	}
}
