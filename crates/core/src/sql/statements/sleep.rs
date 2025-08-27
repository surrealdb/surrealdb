use std::fmt;

use crate::val::Duration;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
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
			duration: v.duration,
		}
	}
}

impl From<crate::expr::statements::SleepStatement> for SleepStatement {
	fn from(v: crate::expr::statements::SleepStatement) -> Self {
		SleepStatement {
			duration: v.duration,
		}
	}
}
