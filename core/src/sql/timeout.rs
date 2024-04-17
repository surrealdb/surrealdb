use crate::sql::duration::Duration;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Timeout(pub Duration);

impl Deref for Timeout {
	type Target = Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Timeout {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "TIMEOUT {}", self.0)
	}
}
