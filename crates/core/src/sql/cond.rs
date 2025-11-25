use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Cond(pub(crate) Expr);

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "WHERE {}", self.0)
	}
}

impl From<Cond> for crate::expr::Cond {
	fn from(v: Cond) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Cond> for Cond {
	fn from(v: crate::expr::Cond) -> Self {
		Self(v.0.into())
	}
}
