use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::{fmt, time};

pub(crate) static SECONDS_PER_YEAR: u64 = 365 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
pub(crate) static SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
pub(crate) static SECONDS_PER_MINUTE: u64 = 60;
pub(crate) static NANOSECONDS_PER_MILLISECOND: u32 = 1000000;
pub(crate) static NANOSECONDS_PER_MICROSECOND: u32 = 1000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Ord)]
pub struct Duration(pub time::Duration);

impl Duration {
	pub const MAX: Duration = Duration(time::Duration::MAX);
}

impl From<time::Duration> for Duration {
	fn from(v: time::Duration) -> Self {
		Self(v)
	}
}

impl From<Duration> for time::Duration {
	fn from(s: Duration) -> Self {
		s.0
	}
}

impl From<Duration> for crate::val::Duration {
	fn from(v: Duration) -> Self {
		crate::val::Duration(v.0)
	}
}

impl From<crate::val::Duration> for Duration {
	fn from(v: crate::val::Duration) -> Self {
		Self(v.0)
	}
}

impl Deref for Duration {
	type Target = time::Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Duration {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		crate::val::Duration(self.0).fmt(f)
	}
}
