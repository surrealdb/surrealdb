use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Cond(pub(crate) Expr);

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::cond::Cond = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
