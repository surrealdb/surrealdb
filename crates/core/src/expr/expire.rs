use crate::expr::duration::Duration;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use tokio::time::Instant;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Expire(pub Duration);

impl Expire {
	pub fn expiring_time(&self) -> Instant {
		Instant::now() + self.0.0
	}
}

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
