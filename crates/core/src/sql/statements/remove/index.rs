use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveIndexStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
}

impl Default for RemoveIndexStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveIndexStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE INDEX");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {} ON {}", self.name, self.what);
	}
}

impl From<RemoveIndexStatement> for crate::expr::statements::RemoveIndexStatement {
	fn from(v: RemoveIndexStatement) -> Self {
		crate::expr::statements::RemoveIndexStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveIndexStatement> for RemoveIndexStatement {
	fn from(v: crate::expr::statements::RemoveIndexStatement) -> Self {
		RemoveIndexStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}
