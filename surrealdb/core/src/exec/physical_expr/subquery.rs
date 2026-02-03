use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ExecOperator};
use crate::expr::ControlFlow;
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

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Create a derived execution context with the parent value set.
		// This allows $parent references in the subquery to access the outer document.
		let subquery_ctx = if let Some(parent_value) = ctx.current_value {
			ctx.exec_ctx.with_param("parent", parent_value.clone())
		} else {
			ctx.exec_ctx.clone()
		};

		// Execute the subquery plan with the derived context
		let mut stream = match self.plan.execute(&subquery_ctx) {
			Ok(s) => s,
			Err(ControlFlow::Err(e)) => return Err(e),
			Err(ControlFlow::Return(v)) => return Ok(v),
			Err(other) => {
				return Err(anyhow::anyhow!(
					"Unexpected control flow when executing subquery: {:?}",
					other
				));
			}
		};

		// Collect all values from the stream
		let mut values = Vec::new();
		while let Some(batch_result) = stream.next().await {
			match batch_result {
				Ok(batch) => values.extend(batch.values),
				Err(ControlFlow::Return(v)) => {
					// Return statement in subquery - use the returned value
					return Ok(v);
				}
				Err(ControlFlow::Err(e)) => return Err(e),
				Err(other) => {
					return Err(anyhow::anyhow!(
						"Unexpected control flow in subquery: {:?}",
						other
					));
				}
			}
		}

		// Return collected values as array (matches legacy SELECT behavior)
		Ok(Value::Array(Array(values)))
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
