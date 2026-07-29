use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{Block, CoverStmts, Expr, Param};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Expr,
	pub block: Block,
}

impl ToSql for ForeachStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "FOR {} IN {} {}", self.param, CoverStmts(&self.range), self.block)
	}
}
