use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::statements::info::InfoStructure;
use crate::sql::strand::Strand;
use crate::sql::Value;
use crate::syn;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Sum;
use std::ops;
use std::ops::Deref;
use std::str::FromStr;
use std::time;

use super::value::{TryAdd, TrySub};

pub(crate) static SECONDS_PER_YEAR: u64 = 365 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
pub(crate) static SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
pub(crate) static SECONDS_PER_MINUTE: u64 = 60;
pub(crate) static NANOSECONDS_PER_MILLISECOND: u32 = 1000000;
pub(crate) static NANOSECONDS_PER_MICROSECOND: u32 = 1000;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Duration";

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Ord)]
#[serde(rename = "$surrealdb::private::sql::Duration")]
#[non_exhaustive]
pub struct Duration(pub time::Duration);

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

impl From<time::Duration> for Value {
	fn from(value: time::Duration) -> Self {
		Self::Duration(value.into())
	}
}

impl FromStr for Duration {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Duration {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Duration {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Duration {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match syn::duration(v) {
			Ok(v) => Ok(v),
			_ => Err(()),
		}
	}
}

impl Deref for Duration {
	type Target = time::Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Duration {
	/// Create a duration from both seconds and nanoseconds components
	pub fn new(secs: u64, nanos: u32) -> Duration {
		time::Duration::new(secs, nanos).into()
	}
	/// Convert the Duration to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
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
		time::Duration::from_nanos(nanos).into()
	}
	/// Create a duration from microseconds
	pub fn from_micros(micros: u64) -> Duration {
		time::Duration::from_micros(micros).into()
	}
	/// Create a duration from milliseconds
	pub fn from_millis(millis: u64) -> Duration {
		time::Duration::from_millis(millis).into()
	}
	/// Create a duration from seconds
	pub fn from_secs(secs: u64) -> Duration {
		time::Duration::from_secs(secs).into()
	}
	/// Create a duration from minutes
	pub fn from_mins(mins: u64) -> Duration {
		time::Duration::from_secs(mins * SECONDS_PER_MINUTE).into()
	}
	/// Create a duration from hours
	pub fn from_hours(hours: u64) -> Duration {
		time::Duration::from_secs(hours * SECONDS_PER_HOUR).into()
	}
	/// Create a duration from days
	pub fn from_days(days: u64) -> Duration {
		time::Duration::from_secs(days * SECONDS_PER_DAY).into()
	}
	/// Create a duration from weeks
	pub fn from_weeks(days: u64) -> Duration {
		time::Duration::from_secs(days * SECONDS_PER_WEEK).into()
	}
}

impl fmt::Display for Duration {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl ops::Add for Duration {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		match self.0.checked_add(other.0) {
			Some(v) => Duration::from(v),
			None => Duration::from(time::Duration::MAX),
		}
	}
}

impl TryAdd for Duration {
	type Output = Self;
	fn try_add(self, other: Self) -> Result<Self, Error> {
		self.0
			.checked_add(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map(Duration::from)
	}
}

impl<'a, 'b> ops::Add<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn add(self, other: &'b Duration) -> Duration {
		match self.0.checked_add(other.0) {
			Some(v) => Duration::from(v),
			None => Duration::from(time::Duration::MAX),
		}
	}
}

impl<'a, 'b> TryAdd<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn try_add(self, other: &'b Duration) -> Result<Duration, Error> {
		self.0
			.checked_add(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map(Duration::from)
	}
}

impl ops::Sub for Duration {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		match self.0.checked_sub(other.0) {
			Some(v) => Duration::from(v),
			None => Duration::default(),
		}
	}
}

impl TrySub for Duration {
	type Output = Self;
	fn try_sub(self, other: Self) -> Result<Self, Error> {
		self.0
			.checked_sub(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} - {other}")))
			.map(Duration::from)
	}
}

impl<'a, 'b> ops::Sub<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn sub(self, other: &'b Duration) -> Duration {
		match self.0.checked_sub(other.0) {
			Some(v) => Duration::from(v),
			None => Duration::default(),
		}
	}
}

impl<'a, 'b> TrySub<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn try_sub(self, other: &'b Duration) -> Result<Duration, Error> {
		self.0
			.checked_sub(other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} - {other}")))
			.map(Duration::from)
	}
}

impl ops::Add<Datetime> for Duration {
	type Output = Datetime;
	fn add(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.0) {
			Ok(d) => match other.0.checked_add_signed(d) {
				Some(v) => Datetime::from(v),
				None => Datetime::default(),
			},
			Err(_) => Datetime::default(),
		}
	}
}

impl TryAdd<Datetime> for Duration {
	type Output = Datetime;
	fn try_add(self, other: Datetime) -> Result<Datetime, Error> {
		match chrono::Duration::from_std(self.0) {
			Ok(d) => match other.0.checked_add_signed(d) {
				Some(v) => Ok(Datetime::from(v)),
				None => Err(Error::ArithmeticOverflow(format!("{self} + {other}"))),
			},
			Err(_) => Err(Error::ArithmeticOverflow(format!("{self} + {other}"))),
		}
	}
}

impl ops::Sub<Datetime> for Duration {
	type Output = Datetime;
	fn sub(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.0) {
			Ok(d) => match other.0.checked_sub_signed(d) {
				Some(v) => Datetime::from(v),
				None => Datetime::default(),
			},
			Err(_) => Datetime::default(),
		}
	}
}

impl TrySub<Datetime> for Duration {
	type Output = Datetime;
	fn try_sub(self, other: Datetime) -> Result<Datetime, Error> {
		match chrono::Duration::from_std(self.0) {
			Ok(d) => match other.0.checked_sub_signed(d) {
				Some(v) => Ok(Datetime::from(v)),
				None => Err(Error::ArithmeticOverflow(format!("{self} - {other}"))),
			},
			Err(_) => Err(Error::ArithmeticOverflow(format!("{self} - {other}"))),
		}
	}
}

impl Sum<Self> for Duration {
	fn sum<I>(iter: I) -> Duration
	where
		I: Iterator<Item = Self>,
	{
		iter.fold(Duration::default(), |a, b| a + b)
	}
}

impl<'a> Sum<&'a Self> for Duration {
	fn sum<I>(iter: I) -> Duration
	where
		I: Iterator<Item = &'a Self>,
	{
		iter.fold(Duration::default(), |a, b| &a + b)
	}
}

impl InfoStructure for Duration {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
