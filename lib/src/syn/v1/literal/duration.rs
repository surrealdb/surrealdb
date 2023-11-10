use super::super::{ending::duration as ending, error::expected, IResult, ParseError};
use crate::{
	sql::{
		duration::{
			SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK,
			SECONDS_PER_YEAR,
		},
		Duration,
	},
	syn::v1::common::take_u64,
};
use nom::{branch::alt, bytes::complete::tag, multi::many1};
use std::time;

pub fn duration(i: &str) -> IResult<&str, Duration> {
	expected("a duration", |i| {
		let (i, v) = many1(duration_raw)(i)?;
		let (i, _) = ending(i)?;
		Ok((i, v.iter().sum::<Duration>()))
	})(i)
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

	std_duration.map(|d| (i, Duration(d))).ok_or(nom::Err::Error(ParseError::Base(i)))
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
		let out = res.unwrap().1;
		assert_eq!("0ns", format!("{}", out));
		assert_eq!(out.0, Duration::new(0, 0));
	}

	#[test]
	fn duration_basic() {
		let sql = "1s";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("1s", format!("{}", out));
		assert_eq!(out.0, Duration::new(1, 0));
	}

	#[test]
	fn duration_simple() {
		let sql = "1000ms";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("1s", format!("{}", out));
		assert_eq!(out.0, Duration::new(1, 0));
	}

	#[test]
	fn duration_complex() {
		let sql = "86400s";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("1d", format!("{}", out));
		assert_eq!(out.0, Duration::new(86_400, 0));
	}

	#[test]
	fn duration_days() {
		let sql = "5d";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("5d", format!("{}", out));
		assert_eq!(out.0, Duration::new(432_000, 0));
	}

	#[test]
	fn duration_weeks() {
		let sql = "4w";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("4w", format!("{}", out));
		assert_eq!(out.0, Duration::new(2_419_200, 0));
	}

	#[test]
	fn duration_split() {
		let sql = "129600s";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("1d12h", format!("{}", out));
		assert_eq!(out.0, Duration::new(129_600, 0));
	}

	#[test]
	fn duration_multi() {
		let sql = "1d12h30m";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("1d12h30m", format!("{}", out));
		assert_eq!(out.0, Duration::new(131_400, 0));
	}

	#[test]
	fn duration_milliseconds() {
		let sql = "500ms";
		let res = duration(sql);
		let out = res.unwrap().1;
		assert_eq!("500ms", format!("{}", out));
		assert_eq!(out.0, Duration::new(0, 500000000));
	}

	#[test]
	fn duration_overflow() {
		let sql = "10000000000000000d";
		let res = duration(sql);
		res.unwrap_err();
	}
}
