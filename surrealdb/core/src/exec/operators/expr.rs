//! Expr operator - evaluates a scalar expression and returns a single value.
//!
//! This operator wraps a PhysicalExpr and executes it in scalar context,
//! returning the result as a single-element ValueBatch.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};

/// Expr operator - evaluates a scalar expression.
///
/// This operator wraps a PhysicalExpr and evaluates it in scalar context
/// (without row context). Used for top-level expressions like `1 + 1;` or `$param;`.
#[derive(Debug, Clone)]
pub struct ExprPlan {
	/// The expression to evaluate
	pub expr: Arc<dyn PhysicalExpr>,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ExprPlan {
	pub(crate) fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			expr,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for ExprPlan {
	fn name(&self) -> &'static str {
		"Expr"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("expr".to_string(), self.expr.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		self.expr.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.expr.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
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

		Ok(Box::pin(stream::once(async move {
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let value = expr.evaluate(eval_ctx).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

impl ToSql for ExprPlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
	}
}
