use std::fmt;
use std::ops::Deref;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::val::Duration;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
