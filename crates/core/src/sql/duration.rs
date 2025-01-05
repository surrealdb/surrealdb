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

pub(crate) static SECONDS_PER_YEAR: i64 = 365 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_WEEK: i64 = 7 * SECONDS_PER_DAY;
pub(crate) static SECONDS_PER_DAY: i64 = 24 * SECONDS_PER_HOUR;
pub(crate) static SECONDS_PER_HOUR: i64 = 60 * SECONDS_PER_MINUTE;
pub(crate) static SECONDS_PER_MINUTE: i64 = 60;
pub(crate) static NANOSECONDS_PER_SECOND: u32 = 1_000 * NANOSECONDS_PER_MILLISECOND;
pub(crate) static NANOSECONDS_PER_MILLISECOND: u32 = 1_000 * NANOSECONDS_PER_MICROSECOND;
pub(crate) static NANOSECONDS_PER_MICROSECOND: u32 = 1_000;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Duration";

#[revisioned(revision = 1)]
#[derive(
	Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Ord,
)]
#[serde(rename = "$surrealdb::private::sql::Duration")]
#[non_exhaustive]
pub struct Duration(pub chrono::Duration);

impl From<chrono::Duration> for Duration {
	fn from(v: chrono::Duration) -> Self {
		Self(v)
	}
}

impl From<Duration> for chrono::Duration {
	fn from(s: Duration) -> Self {
		s.0
	}
}

impl FromStr for Duration {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<time::Duration> for Duration {
	type Error = ();
	fn try_from(v: time::Duration) -> Result<Self, Self::Error> {
		match chrono::Duration::from_std(v) {
			Ok(v) => Ok(v.into()),
			_ => Err(()),
		}
	}
}

impl TryFrom<time::Duration> for Value {
	type Error = ();
	fn try_from(v: time::Duration) -> Result<Self, Self::Error> {
		match chrono::Duration::from_std(v) {
			Ok(v) => Ok(Self::Duration(v.into())),
			_ => Err(()),
		}
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
	type Target = chrono::Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Duration {
	/// Create a duration from both seconds and nanoseconds components
	pub fn new(secs: i64, nanos: u32) -> Result<Duration, Error> {
		let secs = match secs.checked_add((nanos / NANOSECONDS_PER_SECOND) as i64) {
            Some(secs) => secs,
            None => panic!("overflow in Duration::new"),
        };
        let nanos = nanos % NANOSECONDS_PER_SECOND;

		match chrono::Duration::new(secs, nanos) {
			Some(v) => Ok(v.into()),
			None => Err(Error::ArithmeticOverflow(format!("Duration::new({secs}, {nanos})"))),
		}
	}
	/// Convert the Duration to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
	/// Check if the duration is zero
	pub fn is_zero(&self) -> bool {
		self.0.is_zero()
	}
	/// Get the total number of nanoseconds
	pub fn nanos(&self) -> i128 {
		self.0.subsec_nanos() as i128 + self.secs() as i128 * NANOSECONDS_PER_SECOND as i128
	}
	/// Get the total number of microseconds
	pub fn micros(&self) -> i128 {
		self.nanos() / 1_000
	}
	/// Get the total number of milliseconds
	pub fn millis(&self) -> i128 {
		self.micros() / 1_000
	}
	/// Get the total number of seconds
	pub fn secs(&self) -> i64 {
		self.0.num_seconds()
	}
	/// Get the total number of minutes
	pub fn mins(&self) -> i64 {
		self.0.num_seconds() / SECONDS_PER_MINUTE
	}
	/// Get the total number of hours
	pub fn hours(&self) -> i64 {
		self.0.num_seconds() / SECONDS_PER_HOUR
	}
	/// Get the total number of dats
	pub fn days(&self) -> i64 {
		self.0.num_seconds() / SECONDS_PER_DAY
	}
	/// Get the total number of months
	pub fn weeks(&self) -> i64 {
		self.0.num_seconds() / SECONDS_PER_WEEK
	}
	/// Get the total number of years
	pub fn years(&self) -> i64 {
		self.0.num_seconds() / SECONDS_PER_YEAR
	}
	/// Create a duration from nanoseconds
	pub fn from_nanos(nanos: i64) -> Duration {
		chrono::Duration::nanoseconds(nanos).into()
	}
	/// Create a duration from microseconds
	pub fn from_micros(micros: i64) -> Duration {
		chrono::Duration::microseconds(micros).into()
	}
	/// Create a duration from milliseconds
	pub fn from_millis(millis: i64) -> Duration {
		chrono::Duration::milliseconds(millis).into()
	}
	/// Create a duration from seconds
	pub fn from_secs(secs: i64) -> Duration {
		chrono::Duration::seconds(secs).into()
	}
	/// Create a duration from minutes
	pub fn from_mins(mins: i64) -> Duration {
		chrono::Duration::seconds(mins * SECONDS_PER_MINUTE).into()
	}
	/// Create a duration from hours
	pub fn from_hours(hours: i64) -> Duration {
		chrono::Duration::seconds(hours * SECONDS_PER_HOUR).into()
	}
	/// Create a duration from days
	pub fn from_days(days: i64) -> Duration {
		chrono::Duration::seconds(days * SECONDS_PER_DAY).into()
	}
	/// Create a duration from weeks
	pub fn from_weeks(days: i64) -> Duration {
		chrono::Duration::seconds(days * SECONDS_PER_WEEK).into()
	}
}

impl fmt::Display for Duration {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Split up the duration
		let mut secs = self.0.num_seconds();
		let mut nano = self.0.subsec_nanos();
		// Ensures nanos is positive when secs is positive
		if secs > 0 && nano < 0 {
			secs = secs.checked_sub(secs).ok_or(std::fmt::Error)?;
			nano = nano.checked_add(NANOSECONDS_PER_SECOND as i32).ok_or(std::fmt::Error)?;
		}
		// Ensures nanos is negative when secs is negative
		if secs < 0 && nano > 0 {
			secs = secs.checked_add(secs).ok_or(std::fmt::Error)?;
			nano = nano.checked_sub(NANOSECONDS_PER_SECOND as i32).ok_or(std::fmt::Error)?;
		}
		// Ensure no empty output
		if secs == 0 && nano == 0 {
			return write!(f, "0ns");
		}
		// Display negative duration
		let is_negative = secs < 0 || (secs == 0 && nano < 0);
		if is_negative {
			write!(f, "-")?;
			secs = secs.abs();
			nano = nano.abs();
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
		let msec = nano / NANOSECONDS_PER_MILLISECOND as i32;
		let nano = nano % NANOSECONDS_PER_MILLISECOND as i32;
		// Calculate the total microseconds
		let usec = nano / NANOSECONDS_PER_MICROSECOND as i32;
		let nano = nano % NANOSECONDS_PER_MICROSECOND as i32;
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
		match self.0.checked_add(&other.0) {
			Some(v) => Duration::from(v),
			None => Duration::from(chrono::Duration::MAX),
		}
	}
}

impl TryAdd for Duration {
	type Output = Self;
	fn try_add(self, other: Self) -> Result<Self, Error> {
		self.0
			.checked_add(&other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map(Duration::from)
	}
}

impl<'b> ops::Add<&'b Duration> for &Duration {
	type Output = Duration;
	fn add(self, other: &'b Duration) -> Duration {
		match self.0.checked_add(&other.0) {
			Some(v) => Duration::from(v),
			None => Duration::from(chrono::Duration::MAX),
		}
	}
}

impl<'b> TryAdd<&'b Duration> for &Duration {
	type Output = Duration;
	fn try_add(self, other: &'b Duration) -> Result<Duration, Error> {
		self.0
			.checked_add(&other.0)
			.ok_or_else(|| Error::ArithmeticOverflow(format!("{self} + {other}")))
			.map(Duration::from)
	}
}

impl ops::Sub for Duration {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		match self.0.checked_sub(&other.0) {
			Some(v) => Duration::from(v),
			None => Duration::default(),
		}
	}
}

impl TrySub for Duration {
	type Output = Self;
	fn try_sub(self, other: Self) -> Result<Self, Error> {
		self.0
			.checked_sub(&other.0)
			.ok_or_else(|| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map(Duration::from)
	}
}

impl<'b> ops::Sub<&'b Duration> for &Duration {
	type Output = Duration;
	fn sub(self, other: &'b Duration) -> Duration {
		match self.0.checked_sub(&other.0) {
			Some(v) => Duration::from(v),
			None => Duration::default(),
		}
	}
}

impl<'b> TrySub<&'b Duration> for &Duration {
	type Output = Duration;
	fn try_sub(self, other: &'b Duration) -> Result<Duration, Error> {
		self.0
			.checked_sub(&other.0)
			.ok_or_else(|| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map(Duration::from)
	}
}

impl ops::Add<Datetime> for Duration {
	type Output = Datetime;
	fn add(self, other: Datetime) -> Datetime {
		match other.0.checked_add_signed(self.0) {
			Some(v) => Datetime::from(v),
			None => Datetime::default(),
		}
	}
}

impl TryAdd<Datetime> for Duration {
	type Output = Datetime;
	fn try_add(self, other: Datetime) -> Result<Datetime, Error> {
		match other.0.checked_add_signed(self.0) {
			Some(v) => Ok(Datetime::from(v)),
			None => Err(Error::ArithmeticOverflow(format!("{self} + {other}"))),
		}
	}
}

impl ops::Sub<Datetime> for Duration {
	type Output = Datetime;
	fn sub(self, other: Datetime) -> Datetime {
		match other.0.checked_sub_signed(self.0) {
			Some(v) => Datetime::from(v),
			None => Datetime::default(),
		}
	}
}

impl TrySub<Datetime> for Duration {
	type Output = Datetime;
	fn try_sub(self, other: Datetime) -> Result<Datetime, Error> {
		match other.0.checked_sub_signed(self.0) {
			Some(v) => Ok(Datetime::from(v)),
			None => Err(Error::ArithmeticNegativeOverflow(format!("{self} - {other}"))),
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
