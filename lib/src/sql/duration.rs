use crate::sql::common::take_u64;
use crate::sql::datetime::Datetime;
use crate::sql::error::IResult;
use crate::sql::serde::is_internal_serialization;
use chrono::DurationRound;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::multi::many1;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Sum;
use std::ops;
use std::ops::Deref;
use std::time;

static SECONDS_PER_YEAR: u64 = 31_536_000;
static SECONDS_PER_WEEK: u64 = 604_800;
static SECONDS_PER_DAY: u64 = 86400;
static SECONDS_PER_HOUR: u64 = 3600;
static SECONDS_PER_MINUTE: u64 = 60;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Duration(pub time::Duration);

impl From<time::Duration> for Duration {
	fn from(v: time::Duration) -> Self {
		Duration(v)
	}
}

impl From<String> for Duration {
	fn from(s: String) -> Self {
		match duration(s.as_ref()) {
			Ok((_, v)) => v,
			Err(_) => Duration::default(),
		}
	}
}

impl From<&str> for Duration {
	fn from(s: &str) -> Self {
		match duration(s) {
			Ok((_, v)) => v,
			Err(_) => Duration::default(),
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
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl fmt::Display for Duration {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Split up the duration
		let secs = self.0.as_secs();
		let nano = self.0.subsec_nanos();
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
		// Calculate the total mins
		let mins = secs / SECONDS_PER_MINUTE;
		let secs = secs % SECONDS_PER_MINUTE;
		// Prepare the outpit
		let mut o = Vec::with_capacity(7);
		// Write the different parts
		if year > 0 {
			o.push(format!("{year}y"));
		}
		if week > 0 {
			o.push(format!("{week}w"));
		}
		if days > 0 {
			o.push(format!("{days}d"));
		}
		if hour > 0 {
			o.push(format!("{hour}h"));
		}
		if mins > 0 {
			o.push(format!("{mins}m"));
		}
		if secs > 0 {
			o.push(format!("{secs}s"));
		}
		if nano > 0 {
			o.push(format!("{nano}ns"));
		}
		// Ensure no empty output
		if o.is_empty() {
			o.push("0ns".to_string());
		}
		// Concatenate together
		write!(f, "{}", o.concat())
	}
}

impl Serialize for Duration {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct("Duration", &self.0)
		} else {
			serializer.serialize_some(&self.to_string())
		}
	}
}

impl ops::Add for Duration {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		Duration::from(self.0 + other.0)
	}
}

impl<'a, 'b> ops::Add<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn add(self, other: &'b Duration) -> Duration {
		Duration::from(self.0 + other.0)
	}
}

impl ops::Sub for Duration {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		Duration::from(self.0 - other.0)
	}
}

impl<'a, 'b> ops::Sub<&'b Duration> for &'a Duration {
	type Output = Duration;
	fn sub(self, other: &'b Duration) -> Duration {
		Duration::from(self.0 - other.0)
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

impl ops::Div<Datetime> for Duration {
	type Output = Datetime;
	fn div(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.0) {
			Ok(d) => match other.duration_trunc(d) {
				Ok(v) => Datetime::from(v),
				Err(_) => Datetime::default(),
			},
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
	Ok((i, v.iter().sum::<Duration>()))
}

fn duration_raw(i: &str) -> IResult<&str, Duration> {
	let (i, v) = part(i)?;
	let (i, u) = unit(i)?;
	Ok((
		i,
		Duration(match u {
			"ns" => time::Duration::new(0, v as u32),
			"µs" => time::Duration::new(0, v as u32 * 1000),
			"ms" => time::Duration::new(0, v as u32 * 1000 * 1000),
			"s" => time::Duration::new(v, 0),
			"m" => time::Duration::new(v * 60, 0),
			"h" => time::Duration::new(v * 60 * 60, 0),
			"d" => time::Duration::new(v * 60 * 60 * 24, 0),
			"w" => time::Duration::new(v * 60 * 60 * 24 * 7, 0),
			"y" => time::Duration::new(v * 60 * 60 * 24 * 365, 0),
			_ => time::Duration::new(0, 0),
		}),
	))
}

fn part(i: &str) -> IResult<&str, u64> {
	let (i, v) = take_u64(i)?;
	Ok((i, v))
}

fn unit(i: &str) -> IResult<&str, &str> {
	alt((
		tag("ns"),
		tag("µs"),
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
}
