use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Cond(pub(crate) Expr);

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "WHERE {}", self.0);
	}
}
