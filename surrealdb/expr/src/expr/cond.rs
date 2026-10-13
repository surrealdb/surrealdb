use surrealdb_types::{SqlFormat, ToSql};

use super::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Cond(pub Expr);

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::cond::Cond = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
