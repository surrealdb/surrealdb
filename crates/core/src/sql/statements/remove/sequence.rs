use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveSequenceStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveSequenceStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveSequenceStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE SEQUENCE");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {}", self.name);
	}
}

impl From<RemoveSequenceStatement> for crate::expr::statements::remove::RemoveSequenceStatement {
	fn from(v: RemoveSequenceStatement) -> Self {
		crate::expr::statements::remove::RemoveSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveSequenceStatement> for RemoveSequenceStatement {
	fn from(v: crate::expr::statements::remove::RemoveSequenceStatement) -> Self {
		RemoveSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
