use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Start(pub Expr);

impl Start {
	/// Whether evaluating the start expression can be done on a read-only
	/// transaction.
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl ToSql for Start {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "START {}", CoverStmts(&self.0));
	}
}
