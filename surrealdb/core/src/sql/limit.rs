use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Limit(pub(crate) Expr);

impl ToSql for Limit {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "LIMIT {}", CoverStmts(&self.0))
	}
}

impl From<Limit> for crate::expr::Limit {
	fn from(value: Limit) -> Self {
		Self(value.0.into())
	}
}

impl From<crate::expr::Limit> for Limit {
	fn from(value: crate::expr::Limit) -> Self {
		Limit(value.0.into())
	}
}
