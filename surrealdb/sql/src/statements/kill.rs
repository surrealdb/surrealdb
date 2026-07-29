use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Expr};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Expr,
}

impl ToSql for KillStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "KILL {}", CoverStmts(&self.id));
	}
}
