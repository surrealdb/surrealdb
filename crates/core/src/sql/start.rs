use crate::sql::value::SqlValue;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Start(pub SqlValue);

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

crate::sql::impl_display_from_sql!(Start);

impl crate::sql::DisplaySql for Start {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "START {}", self.0)
	}
}
