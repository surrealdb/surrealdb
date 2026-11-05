use std::fmt::Debug;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::sql::{SqlFormat, ToSql};

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
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Duration(pub(crate) std::time::Duration);

impl Duration {
	/// The maximum duration
	pub const MAX: Duration = Duration(std::time::Duration::MAX);
	/// The zero duration
	pub const ZERO: Duration = Duration(std::time::Duration::ZERO);

	/// Create a duration from both seconds and nanoseconds components
	pub fn new(secs: u64, nanos: u32) -> Duration {
		std::time::Duration::new(secs, nanos).into()
	}

	/// Create a duration from std::time::Duration
	pub fn from_std(d: std::time::Duration) -> Self {
		Self(d)
	}

	/// Convert into the inner std::time::Duration
	pub fn into_inner(self) -> std::time::Duration {
		self.0
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

	pub(crate) fn fmt_sql_internal(&self, f: &mut String) {
		fmt_duration_sql(self.0, f);
	}
}

/// Format a [`std::time::Duration`] using SurrealQL duration syntax (e.g. `1h30m`).
///
/// This is the exact rendering [`Duration`]'s [`ToSql`] implementation uses;
/// it is exposed so layers holding a raw [`std::time::Duration`] can print the
/// public duration grammar without wrapping.
pub fn fmt_duration_sql(duration: std::time::Duration, f: &mut String) {
	// Split up the duration
	let secs = duration.as_secs();
	let nano = duration.subsec_nanos();
	// Ensure no empty output
	if secs == 0 && nano == 0 {
		return f.push_str("0ns");
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
		f.push_str(&year.to_string());
		f.push('y');
	}
	if week > 0 {
		f.push_str(&week.to_string());
		f.push('w');
	}
	if days > 0 {
		f.push_str(&days.to_string());
		f.push('d');
	}
	if hour > 0 {
		f.push_str(&hour.to_string());
		f.push('h');
	}
	if mins > 0 {
		f.push_str(&mins.to_string());
		f.push('m');
	}
	if secs > 0 {
		f.push_str(&secs.to_string());
		f.push('s');
	}
	if msec > 0 {
		f.push_str(&msec.to_string());
		f.push_str("ms");
	}
	if usec > 0 {
		f.push_str(&usec.to_string());
		f.push_str("µs");
	}
	if nano > 0 {
		f.push_str(&nano.to_string());
		f.push_str("ns");
	}
}

impl FromStr for Duration {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		parse_common::duration(s).map(Self).map_err(|e| anyhow::Error::msg(e.message))
	}
}

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

impl std::fmt::Display for Duration {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		f.write_str(&self.to_sql())
	}
}

impl ToSql for Duration {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		self.fmt_sql_internal(f);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_duration_from_str() {
		// Test basic units
		assert_eq!(Duration::from_str("1s").unwrap(), Duration::from_secs(1));
		assert_eq!(Duration::from_str("1m").unwrap(), Duration::from_mins(1).unwrap());
		assert_eq!(Duration::from_str("1h").unwrap(), Duration::from_hours(1).unwrap());
		assert_eq!(Duration::from_str("1d").unwrap(), Duration::from_days(1).unwrap());
		assert_eq!(Duration::from_str("1w").unwrap(), Duration::from_weeks(1).unwrap());
		assert_eq!(Duration::from_str("1y").unwrap(), Duration::new(365 * 24 * 60 * 60, 0));

		// Test nanosecond units
		assert_eq!(Duration::from_str("1000ns").unwrap(), Duration::from_nanos(1000));
		assert_eq!(Duration::from_str("1000ms").unwrap(), Duration::from_millis(1000));

		// Test zero duration
		assert_eq!(Duration::from_str("0ns").unwrap(), Duration::new(0, 0));

		// Test combined units
		let combined = Duration::from_str("1h30m15s500ms").unwrap();
		let expected = Duration::from_hours(1).unwrap().0
			+ Duration::from_mins(30).unwrap().0
			+ Duration::from_secs(15).0
			+ Duration::from_millis(500).0;
		assert_eq!(combined.0, expected);

		// Test invalid input
		assert!(Duration::from_str("invalid").is_err());
		assert!(Duration::from_str("1x").is_err());
		assert!(Duration::from_str("").is_err());
	}

	#[test]
	fn test_duration_from_str_debug() {
		// Debug test for microseconds
		println!("Testing '1000us'");
		match Duration::from_str("1000us") {
			Ok(duration) => {
				println!("Successfully parsed: {:?}", duration);
				assert_eq!(duration, Duration::from_micros(1000));
			}
			Err(_) => {
				println!("Failed to parse '1000us'");
				panic!("Failed to parse microseconds");
			}
		}
	}
}
