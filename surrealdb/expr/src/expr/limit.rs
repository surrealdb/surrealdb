use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Limit(pub Expr);

impl ToSql for Limit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let stmt: crate::sql::limit::Limit = self.clone().into();
		stmt.fmt_sql(f, sql_fmt);
	}
}
