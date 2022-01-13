use crate::sql::datetime::Datetime;
use chrono::DurationRound;
use nom::branch::alt;
use nom::bytes::complete::is_a;
use nom::bytes::complete::tag;
use nom::IResult;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;
use std::str::FromStr;
use std::time;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Duration {
	pub input: String,
	pub value: time::Duration,
}

impl From<time::Duration> for Duration {
	fn from(t: time::Duration) -> Self {
		Duration {
			input: format!("{:?}", t),
			value: t,
		}
	}
}

impl<'a> From<&'a str> for Duration {
	fn from(s: &str) -> Self {
		match duration(s) {
			Ok((_, v)) => v,
			Err(_) => Duration::default(),
		}
	}
}

impl fmt::Display for Duration {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.input)
	}
}

impl Serialize for Duration {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_some(&self.input)
		} else {
			let mut val = serializer.serialize_struct("Duration", 2)?;
			val.serialize_field("input", &self.input)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

impl ops::Add for Duration {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		Duration::from(self.value + other.value)
	}
}

impl ops::Sub for Duration {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		Duration::from(self.value - other.value)
	}
}

impl ops::Add<Datetime> for Duration {
	type Output = Datetime;
	fn add(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.value) {
			Ok(d) => Datetime::from(other.value + d),
			Err(_) => Datetime::default(),
		}
	}
}

impl ops::Sub<Datetime> for Duration {
	type Output = Datetime;
	fn sub(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.value) {
			Ok(d) => Datetime::from(other.value - d),
			Err(_) => Datetime::default(),
		}
	}
}

impl ops::Div<Datetime> for Duration {
	type Output = Datetime;
	fn div(self, other: Datetime) -> Datetime {
		match chrono::Duration::from_std(self.value) {
			Ok(d) => match other.value.duration_trunc(d) {
				Ok(v) => Datetime::from(v),
				Err(_) => Datetime::default(),
			},
			Err(_) => Datetime::default(),
		}
	}
}

pub fn duration(i: &str) -> IResult<&str, Duration> {
	duration_raw(i)
}

pub fn duration_raw(i: &str) -> IResult<&str, Duration> {
	let (i, v) = part(i)?;
	let (i, u) = unit(i)?;
	Ok((
		i,
		Duration {
			input: format!("{}{}", v, u),
			value: match u {
				"ns" => time::Duration::new(0, v as u32),
				"µs" => time::Duration::new(0, v as u32 * 1000),
				"ms" => time::Duration::new(0, v as u32 * 1000 * 1000),
				"s" => time::Duration::new(v, 0),
				"m" => time::Duration::new(v * 60, 0),
				"h" => time::Duration::new(v * 60 * 60, 0),
				"d" => time::Duration::new(v * 60 * 60 * 24, 0),
				"w" => time::Duration::new(v * 60 * 60 * 24 * 7, 0),
				_ => time::Duration::new(0, 0),
			},
		},
	))
}

fn part(i: &str) -> IResult<&str, u64> {
	let (i, v) = is_a("1234567890")(i)?;
	let v = u64::from_str(v).unwrap();
	Ok((i, v))
}

fn unit(i: &str) -> IResult<&str, &str> {
	alt((tag("ns"), tag("µs"), tag("ms"), tag("s"), tag("m"), tag("h"), tag("d"), tag("w")))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn duration_nil() {
		let sql = "0ns";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("0ns", format!("{}", out));
		assert_eq!(out.value, Duration::from("0ns").value);
	}

	#[test]
	fn duration_basic() {
		let sql = "1s";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1s", format!("{}", out));
		assert_eq!(out.value, Duration::from("1s").value);
	}

	#[test]
	fn duration_simple() {
		let sql = "1000ms";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1000ms", format!("{}", out));
		assert_eq!(out.value, Duration::from("1s").value);
	}

	#[test]
	fn duration_complex() {
		let sql = "86400s";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("86400s", format!("{}", out));
		assert_eq!(out.value, Duration::from("1d").value);
	}

	#[test]
	fn duration_days() {
		let sql = "5d";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("5d", format!("{}", out));
		assert_eq!(out.value, Duration::from("5d").value);
	}

	#[test]
	fn duration_weeks() {
		let sql = "4w";
		let res = duration(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("4w", format!("{}", out));
		assert_eq!(out.value, Duration::from("4w").value);
	}
}
