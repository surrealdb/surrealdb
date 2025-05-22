use crate::sql::statement::Statement;
use anyhow::Result;

pub(crate) struct SqlToLogical {}

impl SqlToLogical {
	pub(crate) fn new() -> Self {
		Self {}
	}

	pub(crate) fn statement_to_logical(&self, stmt: Statement) -> Result<crate::expr::LogicalPlan> {
		Ok(stmt.into())
	}
}
