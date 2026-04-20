use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ExecOperator};
use crate::expr::FlowResult;
use crate::val::{Array, Value};

/// Scalar subquery - (SELECT ... LIMIT 1)
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn ExecOperator>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ScalarSubquery {
	fn name(&self) -> &'static str {
		"ScalarSubquery"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Delegate to the subquery plan's context requirements
		self.plan.required_context()
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> crate::expr::FlowResult<Value> {
		// Create a derived execution context with the parent value set.
		// This allows $parent and $this references in the subquery to access the outer document.
		// $this is needed for correlated subqueries like `(SELECT ... FROM $this.field)`.
		//
		// Prefer `document_root` over `current_value` when both are set: nested evaluation
		// (e.g. view projections and graph traversals) may advance `current_value` along a
		// field chain while `document_root` still refers to the outer row that `$parent`
		// should denote (issue #7154).
		let outer_row = ctx.document_root.or(ctx.current_value);
		let subquery_ctx = if let Some(parent_value) = outer_row {
			ctx.exec_ctx
				.with_param("parent", parent_value.clone())
				.with_param("this", parent_value.clone())
		} else {
			ctx.exec_ctx.clone()
		};

		// Propagate the permission-skip flag so that inner plans executed
		// during permission predicate evaluation do not re-enter table
		// permission checks (which would recurse on cyclic record links).
		let subquery_ctx = if ctx.skip_fetch_perms {
			subquery_ctx.with_skip_fetch_perms(true)
		} else {
			subquery_ctx
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

	/// Parallel batch evaluation for subqueries.
	///
	/// Read-only subqueries can run in parallel across rows since each row's
	/// subquery execution is independent. Falls back to sequential for
	/// mutation subqueries (ReadWrite) to preserve side-effect ordering.
	async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		if values.len() < 2 || self.plan.access_mode() == AccessMode::ReadWrite {
			// Sequential for small batches or mutation subqueries.
			// Use `with_value_and_doc` so single-row batches still set
			// `document_root` for `$parent` / nested correlation (#7154).
			let mut results = Vec::with_capacity(values.len());
			for value in values {
				results.push(self.evaluate(ctx.with_value_and_doc(value)).await?);
			}
			return Ok(results);
		}
		// Bind both current value and document root so `$parent` / nested
		// subqueries match `with_value_and_doc` single-row evaluation (#7154).
		let futures: Vec<_> =
			values.iter().map(|value| self.evaluate(ctx.with_value_and_doc(value))).collect();
		futures::future::try_join_all(futures).await
	}

	fn access_mode(&self) -> AccessMode {
		// CRITICAL: Propagate the subquery's access mode!
		// This is why `SELECT *, (UPSERT person) FROM person` is ReadWrite
		self.plan.access_mode()
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		vec![("subquery", &self.plan)]
	}
}

impl ToSql for ScalarSubquery {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		// ExecOperator doesn't require ToSql, so we use the plan name
		// as a best-effort representation for display/EXPLAIN output.
		f.push('(');
		f.push_str(self.plan.name());
		f.push(')');
	}
}
