use crate::sql::block::Block;
use crate::sql::value::SqlValue;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Future";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Future")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Future(pub Block);

impl From<SqlValue> for Future {
	fn from(v: SqlValue) -> Self {
		Future(Block::from(v))
	}
}

impl From<Future> for crate::expr::Future {
	fn from(v: Future) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Future> for Future {
	fn from(v: crate::expr::Future) -> Self {
		Future(v.0.into())
	}
}

crate::sql::impl_display_from_sql!(Future);

impl crate::sql::DisplaySql for Future {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<future> {}", self.0)
	}
}
