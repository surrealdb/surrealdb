>>>>> DELETE THIS

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Expr, Literal};
use crate::types::PublicDuration;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Timeout(pub(crate) Expr);

impl Default for Timeout {
	fn default() -> Self {
		Self(Expr::Literal(Literal::Duration(PublicDuration::default())))
	}
}

impl ToSql for Timeout {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "TIMEOUT {}", self.0)
	}
}

impl From<Timeout> for crate::expr::Timeout {
	fn from(v: Timeout) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Timeout> for Timeout {
	fn from(v: crate::expr::Timeout) -> Self {
		Self(v.0.into())
	}
}

impl From<std::time::Duration> for Timeout {
	fn from(v: std::time::Duration) -> Self {
		Self(Expr::Literal(Literal::Duration(PublicDuration::from(v))))
	}
}
