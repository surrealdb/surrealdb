use nom::branch::alt;
use nom::bytes::complete::is_a;
use nom::bytes::complete::tag;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::time;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Duration {
	pub input: String,
	pub value: time::Duration,
}

impl<'a> From<&'a str> for Duration {
	fn from(s: &str) -> Self {
		duration(s).unwrap().1
	}
}

impl fmt::Display for Duration {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.input)
	}
}

pub fn duration(i: &str) -> IResult<&str, Duration> {
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
	alt((
		tag("ns"),
		tag("µs"),
		tag("ms"),
		tag("s"),
		tag("m"),
		tag("h"),
		tag("d"),
		tag("w"),
	))(i)
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
}
