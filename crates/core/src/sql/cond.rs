use crate::sql::value::SqlValue;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Cond(pub SqlValue);

impl Deref for Cond {
	type Target = SqlValue;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
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
