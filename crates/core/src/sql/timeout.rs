use crate::val::Duration;
use std::fmt;
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<Timeout> for crate::expr::Timeout {
	fn from(v: Timeout) -> Self {
		Self(v.0)
	}
}

impl From<crate::expr::Timeout> for Timeout {
	fn from(v: crate::expr::Timeout) -> Self {
		Self(v.0)
	}
}
