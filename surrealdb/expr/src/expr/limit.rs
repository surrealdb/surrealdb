use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Limit(pub Expr);

impl Limit {
	/// Whether evaluating the limit expression can be done on a read-only
	/// transaction.
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl ToSql for Limit {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "LIMIT {}", CoverStmts(&self.0))
	}
}
