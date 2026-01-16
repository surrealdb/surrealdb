use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveTableStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveTableStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}

impl ToSql for RemoveTableStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE TABLE");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));
	}
}

impl From<RemoveTableStatement> for crate::expr::statements::RemoveTableStatement {
	fn from(v: RemoveTableStatement) -> Self {
		crate::expr::statements::RemoveTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveTableStatement> for RemoveTableStatement {
	fn from(v: crate::expr::statements::RemoveTableStatement) -> Self {
		RemoveTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
