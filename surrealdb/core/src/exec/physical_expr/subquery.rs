use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ExecOperator};
use crate::val::{Array, Value};

/// Scalar subquery - (SELECT ... LIMIT 1)
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn ExecOperator>,
}

#[async_trait]
impl PhysicalExpr for ScalarSubquery {
	fn name(&self) -> &'static str {
		"ScalarSubquery"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Subqueries execute against the database
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> crate::expr::FlowResult<Value> {
		// Create a derived execution context with the parent value set.
		// This allows $parent and $this references in the subquery to access the outer document.
		// $this is needed for correlated subqueries like `(SELECT ... FROM $this.field)`.
		let subquery_ctx = if let Some(parent_value) = ctx.current_value {
			ctx.exec_ctx
				.with_param("parent", parent_value.clone())
				.with_param("this", parent_value.clone())
		} else {
			ctx.exec_ctx.clone()
		};

		// Execute the subquery plan with the derived context
		let mut stream = self.plan.execute(&subquery_ctx)?;

		// Collect all values from the stream
		let mut values = Vec::new();
		while let Some(batch_result) = stream.next().await {
			match batch_result {
				Ok(batch) => values.extend(batch.values),
				Err(ctrl) => return Err(ctrl),
			}
		}

		// Check if the plan is scalar (e.g., SELECT ... FROM ONLY)
		if self.plan.is_scalar() {
			// Scalar plans should return a single value directly (or NONE if empty)
			Ok(values.pop().unwrap_or(Value::None))
		} else {
			// Return collected values as array (matches legacy SELECT behavior)
			Ok(Value::Array(Array(values)))
		}
	}

	fn references_current_value(&self) -> bool {
		// For now, assume subqueries don't reference current value
		// TODO: Track if plan references outer scope for correlated subqueries
		false
	}

	fn access_mode(&self) -> AccessMode {
		// CRITICAL: Propagate the subquery's access mode!
		// This is why `SELECT *, (UPSERT person) FROM person` is ReadWrite
		self.plan.access_mode()
	}
}

impl ToSql for ScalarSubquery {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, _fmt, "TODO: Not implemented")
	}
}
