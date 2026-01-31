use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, OperatorPlan};
use crate::val::Value;

/// Scalar subquery - (SELECT ... LIMIT 1)
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn OperatorPlan>,
}

#[async_trait]
impl PhysicalExpr for ScalarSubquery {
	fn name(&self) -> &'static str {
		"ScalarSubquery"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// TODO: Implement scalar subquery evaluation
		// This requires bridging EvalContext (which has borrowed &Transaction)
		// with ExecutionContext (which needs Arc<Transaction>).
		// Options:
		// 1. Store Arc<Transaction> in EvalContext
		// 2. Add a method to create ExecutionContext from borrowed context
		// 3. Make ExecutionContext work with borrowed Transaction (but this conflicts with 'static
		//    stream requirement)

		Err(anyhow::anyhow!(
			"ScalarSubquery evaluation not yet fully implemented - need Arc<Transaction> in EvalContext"
		))
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "TODO: Not implemented")
	}
}
