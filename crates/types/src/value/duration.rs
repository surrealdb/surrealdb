use std::fmt::Debug;
use std::ops::Deref;
use std::str::FromStr;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::sql::ToSql;
use crate::write_sql;

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

	/// Get the inner std::time::Duration
	pub fn inner(&self) -> std::time::Duration {
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

impl FromStr for Duration {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let mut total_secs = 0u64;
		let mut total_nanos = 0u32;
		let mut remaining = s.trim();

		// Handle empty string
		if remaining.is_empty() {
			return Err(anyhow!("Invalid duration string: {s}, empty string"));
		}

		// Handle special case for zero duration
		if remaining == "0ns" || remaining == "0" {
			return Ok(Duration::new(0, 0));
		}

		while !remaining.is_empty() {
			// Find the end of the number part
			let mut end = 0;
			for (i, c) in remaining.char_indices() {
				if !c.is_ascii_digit() {
					end = i;
					break;
				}
				end = i + c.len_utf8();
			}

			if end == 0 {
				return Err(anyhow!("Invalid duration string: {s}, empty characters"));
			}

			let value_str = &remaining[..end];
			let value: u64 = value_str.parse().map_err(|err| {
				anyhow!("Invalid duration string: {s}, failed to parse value: {err}")
			})?;

			remaining = &remaining[end..];

			// Parse the unit - check longer units first to avoid partial matches
			let unit = if remaining.starts_with("ms") {
				remaining = &remaining[2..];
				"ms"
			} else if remaining.starts_with("µs") {
				remaining = &remaining[2..];
				"µs"
			} else if remaining.starts_with("us") {
				remaining = &remaining[2..];
				"us"
			} else if remaining.starts_with("ns") {
				remaining = &remaining[2..];
				"ns"
			} else if remaining.starts_with("y") {
				remaining = &remaining[1..];
				"y"
			} else if remaining.starts_with("w") {
				remaining = &remaining[1..];
				"w"
			} else if remaining.starts_with("d") {
				remaining = &remaining[1..];
				"d"
			} else if remaining.starts_with("h") {
				remaining = &remaining[1..];
				"h"
			} else if remaining.starts_with("m") {
				remaining = &remaining[1..];
				"m"
			} else if remaining.starts_with("s") {
				remaining = &remaining[1..];
				"s"
			} else {
				return Err(anyhow!(
					"Invalid duration string: {s}, unexpected remainder: {remaining}"
				));
			};

			// Convert to seconds and nanoseconds based on unit
			match unit {
				"y" => {
					total_secs = total_secs.saturating_add(value.saturating_mul(SECONDS_PER_YEAR));
				}
				"w" => {
					total_secs = total_secs.saturating_add(value.saturating_mul(SECONDS_PER_WEEK));
				}
				"d" => {
					total_secs = total_secs.saturating_add(value.saturating_mul(SECONDS_PER_DAY));
				}
				"h" => {
					total_secs = total_secs.saturating_add(value.saturating_mul(SECONDS_PER_HOUR));
				}
				"m" => {
					total_secs =
						total_secs.saturating_add(value.saturating_mul(SECONDS_PER_MINUTE));
				}
				"s" => {
					total_secs = total_secs.saturating_add(value);
				}
				"ms" => {
					let millis = value.saturating_mul(NANOSECONDS_PER_MILLISECOND as u64);
					let (secs, nanos) = (millis / 1_000_000_000, (millis % 1_000_000_000) as u32);
					total_secs = total_secs.saturating_add(secs);
					total_nanos = total_nanos.saturating_add(nanos);
				}
				"µs" | "us" => {
					let micros = value.saturating_mul(NANOSECONDS_PER_MICROSECOND as u64);
					let (secs, nanos) = (micros / 1_000_000_000, (micros % 1_000_000_000) as u32);
					total_secs = total_secs.saturating_add(secs);
					total_nanos = total_nanos.saturating_add(nanos);
				}
				"ns" => {
					let (secs, nanos) = (value / 1_000_000_000, (value % 1_000_000_000) as u32);
					total_secs = total_secs.saturating_add(secs);
					total_nanos = total_nanos.saturating_add(nanos);
				}
				unexpected => {
					return Err(anyhow!(
						"Invalid duration string: {s}, unexpected unit: {unexpected}"
					));
				}
			}
		}

		// Handle nanosecond overflow
		if total_nanos >= 1_000_000_000 {
			let additional_secs = total_nanos / 1_000_000_000;
			total_secs = total_secs.saturating_add(additional_secs as u64);
			total_nanos %= 1_000_000_000;
		}

		Ok(Duration::new(total_secs, total_nanos))
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
		self.fmt_internal(f)
	}
}

impl ToSql for Duration {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self)
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
		assert_eq!(Duration::from_str("0").unwrap(), Duration::new(0, 0));

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
