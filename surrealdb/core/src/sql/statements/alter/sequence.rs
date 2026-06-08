use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterSequenceStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub timeout: Option<Expr>,
}

impl Default for AlterSequenceStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			timeout: None,
		}
	}
}

impl ToSql for AlterSequenceStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER SEQUENCE");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", CoverStmts(&self.name));
		if let Some(timeout) = &self.timeout {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(timeout));
		}
	}
}

impl From<AlterSequenceStatement> for crate::expr::statements::alter::AlterSequenceStatement {
	fn from(v: AlterSequenceStatement) -> Self {
		crate::expr::statements::alter::AlterSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			timeout: v.timeout.map(From::from),
		}
	}
}
impl From<crate::expr::statements::alter::AlterSequenceStatement> for AlterSequenceStatement {
	fn from(v: crate::expr::statements::alter::AlterSequenceStatement) -> Self {
		AlterSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			timeout: v.timeout.map(From::from),
		}
	}
}
