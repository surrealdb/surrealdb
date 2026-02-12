//! IfElse operator with deferred planning.
//!
//! The IfElsePlan operator evaluates IF/ELSE IF/ELSE conditional branches,
//! using deferred planning like SequencePlan. Each condition is evaluated
//! sequentially, and the first truthy branch's body is executed.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::evaluate_expr;
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream,
};
use crate::expr::Expr;
use crate::val::Value;

/// IfElse operator with deferred planning.
///
/// Stores the original condition-body pairs and optional else body.
/// Plans and executes each condition at runtime, executing the first
/// truthy branch's body.
///
/// Example:
/// ```surql
/// IF $x > 10 {
///     "large"
/// } ELSE IF $x > 5 {
///     "medium"
/// } ELSE {
///     "small"
/// }
/// ```
#[derive(Debug)]
pub struct IfElsePlan {
	/// Condition-body pairs: Vec<(condition_expr, body_expr)>
	pub branches: Vec<(Expr, Expr)>,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// Optional else body
	pub else_body: Option<Expr>,
}

impl IfElsePlan {
	pub(crate) fn new(branches: Vec<(Expr, Expr)>, else_body: Option<Expr>) -> Self {
		Self {
			branches,
			else_body,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for IfElsePlan {
	fn name(&self) -> &'static str {
		"IfElse"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("branches".to_string(), self.branches.len().to_string())];
		if self.else_body.is_some() {
			attrs.push(("has_else".to_string(), "true".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Conservative: require database context since we don't know
		// what the inner expressions need without analyzing them
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		// Check if any branch requires write access
		let branches_read_only =
			self.branches.iter().all(|(cond, body)| cond.read_only() && body.read_only());
		let else_read_only = self.else_body.as_ref().map(|e| e.read_only()).unwrap_or(true);

		if branches_read_only && else_read_only {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let branches = self.branches.clone();
		let else_body = self.else_body.clone();
		let ctx = ctx.clone();

		let stream = stream::once(async move { execute_ifelse(&branches, &else_body, &ctx).await });

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// With deferred planning, we don't have pre-built children
		vec![]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn is_scalar(&self) -> bool {
		// IF/ELSE expressions return a single value
		true
	}
}

/// Execute the IF/ELSE logic with deferred planning.
async fn execute_ifelse(
	branches: &[(Expr, Expr)],
	else_body: &Option<Expr>,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<ValueBatch> {
	// Evaluate each condition in order
	for (cond, body) in branches {
		let cond_value = evaluate_expr(cond, ctx).await?;

		if cond_value.is_truthy() {
			// Execute the body of the first truthy branch
			let result = evaluate_expr(body, ctx).await?;
			return Ok(ValueBatch {
				values: vec![result],
			});
		}
	}

	// No branch matched - check for else body
	if let Some(else_expr) = else_body {
		let result = evaluate_expr(else_expr, ctx).await?;
		Ok(ValueBatch {
			values: vec![result],
		})
	} else {
		// No else - return NONE
		Ok(ValueBatch {
			values: vec![Value::None],
		})
	}
}

impl ToSql for IfElsePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (i, (cond, body)) in self.branches.iter().enumerate() {
			if i == 0 {
				f.push_str("IF ");
			} else {
				f.push_str(" ELSE IF ");
			}
			cond.fmt_sql(f, fmt);
			f.push(' ');
			body.fmt_sql(f, fmt);
		}
		if let Some(ref else_body) = self.else_body {
			f.push_str(" ELSE ");
			else_body.fmt_sql(f, fmt);
		}
	}
}
