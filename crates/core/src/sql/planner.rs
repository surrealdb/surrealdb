use crate::sql::TopLevelExpr;
use anyhow::Result;

pub(crate) struct SqlToLogical {}

impl SqlToLogical {
	pub(crate) fn new() -> Self {
		Self {}
	}

	pub(crate) fn statement_to_logical(
		&self,
		stmt: TopLevelExpr,
	) -> Result<crate::expr::TopLevelExpr> {
		Ok(stmt.into())
	}
}
