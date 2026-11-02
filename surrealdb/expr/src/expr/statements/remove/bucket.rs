use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{Base, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveBucketStatement {
	pub name: Expr,
	/// The level the bucket lives at; `DATABASE` when the statement names none.
	pub base: Base,
	pub if_exists: bool,
}

impl Default for RemoveBucketStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Db,
			if_exists: false,
		}
	}
}

impl ToSql for RemoveBucketStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE BUCKET");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));
		if self.base != Base::Db {
			write_sql!(f, sql_fmt, " ON {}", self.base);
		}
	}
}
