use crate::sql::duration::Duration;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Expire(pub Duration);

impl Deref for Expire {
	type Target = Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Expire {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "EXPIRE {}", self.0)
	}
}

impl From<Expire> for crate::expr::Expire {
	fn from(v: Expire) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Expire> for Expire {
	fn from(v: crate::expr::Expire) -> Self {
		Self(v.0.into())
	}
}
