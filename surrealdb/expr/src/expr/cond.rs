use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::Expr;
use crate::expr::CoverStmts;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Cond(pub Expr);

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "WHERE {}", CoverStmts(&self.0))
	}
}
