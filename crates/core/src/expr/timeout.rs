use std::fmt;
use std::ops::Deref;

use crate::val::Duration;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Timeout(pub Duration);

impl Timeout {
	pub fn as_std_duration(&self) -> &std::time::Duration {
		&self.0.0
	}
}

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
