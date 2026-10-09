use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveFieldStatement {
	pub name: Expr,
	pub table_name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveFieldStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			table_name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveFieldStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE FIELD");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.table_name));
	}
}
