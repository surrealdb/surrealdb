use std::ops::Deref;

use serde::{Deserialize, Serialize};

pub(crate) static SECONDS_PER_YEAR: u64 = 365 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
pub(crate) static SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
pub(crate) static SECONDS_PER_MINUTE: u64 = 60;
pub(crate) static NANOSECONDS_PER_MILLISECOND: u32 = 1000000;
pub(crate) static NANOSECONDS_PER_MICROSECOND: u32 = 1000;

/// Represents a duration value in SurrealDB
///
/// A duration represents a span of time, typically used for time-based calculations and
/// comparisons. This type wraps the standard `std::time::Duration` type.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct Duration(pub(crate) std::time::Duration);

impl From<std::time::Duration> for Duration {
	fn from(v: std::time::Duration) -> Self {
		Self(v)
	}
}

impl From<Duration> for std::time::Duration {
	fn from(v: Duration) -> Self {
		v.0
	}
}

impl Deref for Duration {
	type Target = std::time::Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Duration {
	/// Create a duration from both seconds and nanoseconds components
	pub fn new(secs: u64, nanos: u32) -> Duration {
		std::time::Duration::new(secs, nanos).into()
	}

	/// Get the total number of nanoseconds
	pub fn nanos(&self) -> u128 {
		self.0.as_nanos()
	}
	/// Get the total number of microseconds
	pub fn micros(&self) -> u128 {
		self.0.as_micros()
	}
	/// Get the total number of milliseconds
	pub fn millis(&self) -> u128 {
		self.0.as_millis()
	}
	/// Get the total number of seconds
	pub fn secs(&self) -> u64 {
		self.0.as_secs()
	}
	/// Get the total number of minutes
	pub fn mins(&self) -> u64 {
		self.0.as_secs() / SECONDS_PER_MINUTE
	}
	/// Get the total number of hours
	pub fn hours(&self) -> u64 {
		self.0.as_secs() / SECONDS_PER_HOUR
	}
	/// Get the total number of dats
	pub fn days(&self) -> u64 {
		self.0.as_secs() / SECONDS_PER_DAY
	}
	/// Get the total number of months
	pub fn weeks(&self) -> u64 {
		self.0.as_secs() / SECONDS_PER_WEEK
	}
	/// Get the total number of years
	pub fn years(&self) -> u64 {
		self.0.as_secs() / SECONDS_PER_YEAR
	}
	/// Create a duration from nanoseconds
	pub fn from_nanos(nanos: u64) -> Duration {
		std::time::Duration::from_nanos(nanos).into()
	}
	/// Create a duration from microseconds
	pub fn from_micros(micros: u64) -> Duration {
		std::time::Duration::from_micros(micros).into()
	}
	/// Create a duration from milliseconds
	pub fn from_millis(millis: u64) -> Duration {
		std::time::Duration::from_millis(millis).into()
	}
	/// Create a duration from seconds
	pub fn from_secs(secs: u64) -> Duration {
		std::time::Duration::from_secs(secs).into()
	}
	/// Create a duration from minutes
	pub fn from_mins(mins: u64) -> Option<Duration> {
		mins.checked_mul(SECONDS_PER_MINUTE).map(std::time::Duration::from_secs).map(|x| x.into())
	}
	/// Create a duration from hours
	pub fn from_hours(hours: u64) -> Option<Duration> {
		hours.checked_mul(SECONDS_PER_HOUR).map(std::time::Duration::from_secs).map(|x| x.into())
	}
	/// Create a duration from days
	pub fn from_days(days: u64) -> Option<Duration> {
		days.checked_mul(SECONDS_PER_DAY).map(std::time::Duration::from_secs).map(|x| x.into())
	}
	/// Create a duration from weeks
	pub fn from_weeks(weeks: u64) -> Option<Duration> {
		weeks.checked_mul(SECONDS_PER_WEEK).map(std::time::Duration::from_secs).map(|x| x.into())
	}
}

impl Duration {
	pub(crate) fn fmt_internal(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		// Split up the duration
		let secs = self.0.as_secs();
		let nano = self.0.subsec_nanos();
		// Ensure no empty output
		if secs == 0 && nano == 0 {
			return write!(f, "0ns");
		}
		// Calculate the total years
		let year = secs / SECONDS_PER_YEAR;
		let secs = secs % SECONDS_PER_YEAR;
		// Calculate the total weeks
		let week = secs / SECONDS_PER_WEEK;
		let secs = secs % SECONDS_PER_WEEK;
		// Calculate the total days
		let days = secs / SECONDS_PER_DAY;
		let secs = secs % SECONDS_PER_DAY;
		// Calculate the total hours
		let hour = secs / SECONDS_PER_HOUR;
		let secs = secs % SECONDS_PER_HOUR;
		// Calculate the total minutes
		let mins = secs / SECONDS_PER_MINUTE;
		let secs = secs % SECONDS_PER_MINUTE;
		// Calculate the total milliseconds
		let msec = nano / NANOSECONDS_PER_MILLISECOND;
		let nano = nano % NANOSECONDS_PER_MILLISECOND;
		// Calculate the total microseconds
		let usec = nano / NANOSECONDS_PER_MICROSECOND;
		let nano = nano % NANOSECONDS_PER_MICROSECOND;
		// Write the different parts
		if year > 0 {
			write!(f, "{year}y")?;
		}
		if week > 0 {
			write!(f, "{week}w")?;
		}
		if days > 0 {
			write!(f, "{days}d")?;
		}
		if hour > 0 {
			write!(f, "{hour}h")?;
		}
		if mins > 0 {
			write!(f, "{mins}m")?;
		}
		if secs > 0 {
			write!(f, "{secs}s")?;
		}
		if msec > 0 {
			write!(f, "{msec}ms")?;
		}
		if usec > 0 {
			write!(f, "{usec}µs")?;
		}
		if nano > 0 {
			write!(f, "{nano}ns")?;
		}
		Ok(())
	}
}
