//! SourceExpr operator - evaluates an expression and yields values for iteration in FROM clause.
//!
//! This operator is specifically designed for FROM clause sources, handling:
//! - None/Null: yields no rows (empty stream)
//! - Arrays: yields each element as a separate row
//! - Other values: yields the value as a single row

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, FlowResult, OperatorPlan, ValueBatch, ValueBatchStream};
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
}

#[async_trait]
impl OperatorPlan for SourceExpr {
	fn name(&self) -> &'static str {
		"SourceExpr"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("expr".to_string(), self.expr.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// Source expressions can run at root level
		// (they only need parameters, not database access)
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		// Delegate to the wrapped expression
		self.expr.access_mode()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let expr = self.expr.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let value = expr.evaluate(eval_ctx).await
				.map_err(|e| crate::expr::ControlFlow::Err(anyhow::anyhow!(e.to_string())))?;

			match value {
				// None and Null yield no rows
				Value::None | Value::Null => {
					// Empty stream - don't yield anything
				}
				// Arrays yield their elements
				Value::Array(arr) => {
					if !arr.is_empty() {
						yield ValueBatch { values: arr.into() };
					}
				}
				// Everything else yields a single row
				other => {
					yield ValueBatch { values: vec![other] };
				}
			}
		};

		Ok(Box::pin(stream))
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
