use crate::sql::common::take_u64;
use crate::sql::datetime::Datetime;
use crate::sql::ending::duration as ending;
use crate::sql::error::IResult;
use crate::sql::serde::is_internal_serialization;
use crate::sql::value::Value;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::multi::many1;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Sum;
use std::ops;
use std::ops::Deref;
use std::time;

static SECONDS_PER_YEAR: u64 = 365 * SECONDS_PER_DAY;
static SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;
static SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
static SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
static SECONDS_PER_MINUTE: u64 = 60;
static NANOSECONDS_PER_MILLISECOND: u32 = 1000000;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Duration";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Duration(pub time::Duration);

impl From<time::Duration> for Duration {
	fn from(v: time::Duration) -> Self {
		Self(v)
	}
}

impl From<String> for Duration {
	fn from(s: String) -> Self {
		s.as_str().into()
	}
}

impl From<&str> for Duration {
	fn from(s: &str) -> Self {
		match duration(s) {
			Ok((_, v)) => v,
			Err(_) => Self::default(),
		}
	}
}

impl From<Duration> for time::Duration {
	fn from(s: Duration) -> Self {
		s.0
	}
}

impl Deref for Duration {
	type Target = time::Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Duration {
	/// Convert the Duration to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
	/// Get the total number of seconds
	pub fn secs(&self) -> Value {
		self.0.as_secs().into()
	}
	/// Get the total number of minutes
	pub fn mins(&self) -> Value {
		let secs = self.0.as_secs();
		let mins = secs / SECONDS_PER_MINUTE;
		mins.into()
	}
	/// Get the total number of hours
	pub fn hours(&self) -> Value {
		let secs = self.0.as_secs();
		let hours = secs / SECONDS_PER_HOUR;
		hours.into()
	}
	/// Get the total number of dats
	pub fn days(&self) -> Value {
		let secs = self.0.as_secs();
		let days = secs / SECONDS_PER_DAY;
		days.into()
	}
	/// Get the total number of months
	pub fn weeks(&self) -> Value {
		let secs = self.0.as_secs();
		let weeks = secs / SECONDS_PER_WEEK;
		weeks.into()
	}
	/// Get the total number of years
	pub fn years(&self) -> Value {
		let secs = self.0.as_secs();
		let years = secs / SECONDS_PER_YEAR;
		years.into()
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
		// Calculate the total millseconds
		let msec = nano / NANOSECONDS_PER_MILLISECOND;
		let nano = nano % NANOSECONDS_PER_MILLISECOND;
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
		if nano > 0 {
			write!(f, "{nano}ns")?;
		}
		Ok(())
	}
}

impl Serialize for Duration {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct(TOKEN, &self.0)
		} else {
			serializer.serialize_some(&self.to_string())
		}
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

impl<'a, 'b> ops::Add<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn add(self, other: &'b Duration) -> Duration {
		match self.0.checked_add(other.0) {
			Some(v) => Duration::from(v),
			None => Duration::from(time::Duration::MAX),
		}
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

impl<'a, 'b> ops::Sub<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn sub(self, other: &'b Duration) -> Duration {
		match self.0.checked_sub(other.0) {
			Some(v) => Duration::from(v),
			None => Duration::default(),
		}
	}
}

impl ops::Add<Datetime> for Duration {
	type Output = Datetime;
	fn add(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.0) {
			Ok(d) => Datetime::from(other.0 + d),
			Err(_) => Datetime::default(),
		}
	}
}

impl ops::Sub<Datetime> for Duration {
	type Output = Datetime;
	fn sub(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.0) {
			Ok(d) => Datetime::from(other.0 - d),
			Err(_) => Datetime::default(),
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

pub fn duration(i: &str) -> IResult<&str, Duration> {
	let (i, v) = many1(duration_raw)(i)?;
	let (i, _) = ending(i)?;
	Ok((i, v.iter().sum::<Duration>()))
}

fn duration_raw(i: &str) -> IResult<&str, Duration> {
	let (i, v) = part(i)?;
	let (i, u) = unit(i)?;

	let std_duration = match u {
		"ns" => Some(time::Duration::from_nanos(v)),
		"µs" => Some(time::Duration::from_micros(v)),
		"us" => Some(time::Duration::from_micros(v)),
		"ms" => Some(time::Duration::from_millis(v)),
		"s" => Some(time::Duration::from_secs(v)),
		"m" => v.checked_mul(SECONDS_PER_MINUTE).map(time::Duration::from_secs),
		"h" => v.checked_mul(SECONDS_PER_HOUR).map(time::Duration::from_secs),
		"d" => v.checked_mul(SECONDS_PER_DAY).map(time::Duration::from_secs),
		"w" => v.checked_mul(SECONDS_PER_WEEK).map(time::Duration::from_secs),
		"y" => v.checked_mul(SECONDS_PER_YEAR).map(time::Duration::from_secs),
		_ => unreachable!("shouldn't have parsed {u} as duration unit"),
	};

	std_duration.map(|d| (i, Duration(d))).ok_or(nom::Err::Error(crate::sql::Error::Parser(i)))
}

fn part(i: &str) -> IResult<&str, u64> {
	take_u64(i)
}

fn unit(i: &str) -> IResult<&str, &str> {
	alt((
		tag("ns"),
		tag("µs"),
		tag("us"),
		tag("ms"),
		tag("s"),
		tag("m"),
		tag("h"),
		tag("d"),
		tag("w"),
		tag("y"),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use std::time::Duration;

	#[test]
	fn duration_nil() {
		let sql = "0ns";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("0ns", format!("{}", out));
		assert_eq!(out.0, Duration::new(0, 0));
	}

	#[test]
	fn duration_basic() {
		let sql = "1s";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1s", format!("{}", out));
		assert_eq!(out.0, Duration::new(1, 0));
	}

	#[test]
	fn duration_simple() {
		let sql = "1000ms";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1s", format!("{}", out));
		assert_eq!(out.0, Duration::new(1, 0));
	}

	#[test]
	fn duration_complex() {
		let sql = "86400s";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1d", format!("{}", out));
		assert_eq!(out.0, Duration::new(86_400, 0));
	}

	#[test]
	fn duration_days() {
		let sql = "5d";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("5d", format!("{}", out));
		assert_eq!(out.0, Duration::new(432_000, 0));
	}

	#[test]
	fn duration_weeks() {
		let sql = "4w";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("4w", format!("{}", out));
		assert_eq!(out.0, Duration::new(2_419_200, 0));
	}

	#[test]
	fn duration_split() {
		let sql = "129600s";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1d12h", format!("{}", out));
		assert_eq!(out.0, Duration::new(129_600, 0));
	}

	#[test]
	fn duration_multi() {
		let sql = "1d12h30m";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1d12h30m", format!("{}", out));
		assert_eq!(out.0, Duration::new(131_400, 0));
	}

	#[test]
	fn duration_milliseconds() {
		let sql = "500ms";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("500ms", format!("{}", out));
		assert_eq!(out.0, Duration::new(0, 500000000));
	}

	#[test]
	fn duration_overflow() {
		let sql = "10000000000000000d";
		let res = duration(sql);
		assert!(res.is_err());
	}
}
