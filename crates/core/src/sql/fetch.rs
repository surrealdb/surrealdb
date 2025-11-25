use std::ops::Deref;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::Fmt;
use crate::sql::Expr;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Fetchs(pub(crate) Vec<Fetch>);

impl Deref for Fetchs {
	type Target = Vec<Fetch>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl ToSql for Fetchs {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "FETCH {}", Fmt::comma_separated(&self.0))
	}
}

impl From<Fetchs> for crate::expr::Fetchs {
	fn from(v: Fetchs) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}
impl From<crate::expr::Fetchs> for Fetchs {
	fn from(v: crate::expr::Fetchs) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Fetch(pub(crate) Expr);

impl ToSql for Fetch {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

impl From<Fetch> for crate::expr::Fetch {
	fn from(v: Fetch) -> Self {
		crate::expr::Fetch(v.0.into())
	}
}

impl From<crate::expr::Fetch> for Fetch {
	fn from(v: crate::expr::Fetch) -> Self {
		Fetch(v.0.into())
	}
}
