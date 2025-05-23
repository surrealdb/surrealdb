use crate::sql::Duration;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
pub struct SleepStatement {
	pub(crate) duration: Duration,
}

impl fmt::Display for SleepStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SLEEP {}", self.duration)
	}
}

impl From<SleepStatement> for crate::expr::statements::SleepStatement {
	fn from(v: SleepStatement) -> Self {
		crate::expr::statements::SleepStatement {
			duration: v.duration.into(),
		}
	}
}

impl From<crate::expr::statements::SleepStatement> for SleepStatement {
	fn from(v: crate::expr::statements::SleepStatement) -> Self {
		SleepStatement {
			duration: v.duration.into(),
		}
	}
}
