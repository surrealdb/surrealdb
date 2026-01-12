use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Start(pub Expr);

impl ToSql for Start {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "START {}", self.0);
	}
}

impl From<Start> for crate::expr::Start {
	fn from(value: Start) -> Self {
		crate::expr::Start(value.0.into())
	}
}

impl From<crate::expr::Start> for Start {
	fn from(value: crate::expr::Start) -> Self {
		Start(value.0.into())
	}
}
