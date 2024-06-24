use crate::sql::datetime::Datetime;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Version(pub Datetime);

impl fmt::Display for Version {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}
