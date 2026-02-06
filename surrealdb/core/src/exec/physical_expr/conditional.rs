use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes};
use crate::expr::FlowResult;
use crate::val::Value;

/// IF/THEN/ELSE expression - IF condition THEN value ELSE other END
#[derive(Debug, Clone)]
pub struct IfElseExpr {
	/// List of (condition, value) pairs for IF and ELSE IF branches
	pub(crate) branches: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
	/// Optional ELSE branch (final fallback)
	pub(crate) otherwise: Option<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for IfElseExpr {
	fn name(&self) -> &'static str {
		"IfElse"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		use crate::exec::ContextLevel;

		// Combine all branches' context requirements
		let branches_ctx = self
			.branches
			.iter()
			.flat_map(|(cond, val)| [cond.required_context(), val.required_context()])
			.max()
			.unwrap_or(ContextLevel::Root);

		let otherwise_ctx =
			self.otherwise.as_ref().map_or(ContextLevel::Root, |e| e.required_context());

		branches_ctx.max(otherwise_ctx)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// Evaluate each condition in order
		for (condition, value) in &self.branches {
			let cond_result = condition.evaluate(ctx.clone()).await?;
			// Check if condition is truthy
			if cond_result.is_truthy() {
				return value.evaluate(ctx).await;
			}
		}

		// No condition was true, evaluate the else branch if present
		if let Some(otherwise) = &self.otherwise {
			otherwise.evaluate(ctx).await
		} else {
			// No else branch, return NONE
			Ok(Value::None)
		}
	}

	fn references_current_value(&self) -> bool {
		// Check if any branch references current value
		self.branches
			.iter()
			.any(|(cond, val)| cond.references_current_value() || val.references_current_value())
			|| self.otherwise.as_ref().is_some_and(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine all branches' access modes
		let branches_mode = self
			.branches
			.iter()
			.flat_map(|(cond, val)| [cond.access_mode(), val.access_mode()])
			.combine_all();

		let otherwise_mode =
			self.otherwise.as_ref().map_or(AccessMode::ReadOnly, |e| e.access_mode());

		branches_mode.combine(otherwise_mode)
	}
}

impl ToSql for IfElseExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (i, (condition, value)) in self.branches.iter().enumerate() {
			if i == 0 {
				write_sql!(f, fmt, "IF {} THEN {}", condition, value);
			} else {
				write_sql!(f, fmt, " ELSE IF {} THEN {}", condition, value);
			}
		}
		if let Some(otherwise) = &self.otherwise {
			write_sql!(f, fmt, " ELSE {}", otherwise);
		}
		f.push_str(" END");
	}
}
