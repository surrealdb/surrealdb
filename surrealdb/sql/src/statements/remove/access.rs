use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{Base, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
}

impl Default for RemoveAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::default(),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveAccessStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE ACCESS");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {} ON {}", CoverStmts(&self.name), self.base);
	}
}
