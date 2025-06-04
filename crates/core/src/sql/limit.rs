use crate::sql::value::SqlValue;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Limit(pub SqlValue);

impl fmt::Display for Limit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIMIT {}", self.0)
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
