use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveFieldStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
}

impl Default for RemoveFieldStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
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
		write_sql!(f, sql_fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.what));
	}
}

impl From<RemoveFieldStatement> for crate::expr::statements::RemoveFieldStatement {
	fn from(v: RemoveFieldStatement) -> Self {
		crate::expr::statements::RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			table_name: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveFieldStatement> for RemoveFieldStatement {
	fn from(v: crate::expr::statements::RemoveFieldStatement) -> Self {
		RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.table_name.into(),
		}
	}
}
