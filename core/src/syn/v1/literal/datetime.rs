use super::super::{
	common::{take_digits, take_digits_range},
	error::expected,
	IResult,
};
use crate::sql::Datetime;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};
use nom::{
	branch::alt,
	bytes::complete::tag,
	character::complete::{char, digit1},
	combinator::{cut, map},
	error::ErrorKind,
	error_position,
	sequence::delimited,
	Err,
};
use std::time::Duration;

pub fn datetime(i: &str) -> IResult<&str, Datetime> {
	expected("a datetime", alt((datetime_single, datetime_double)))(i)
}

fn datetime_single(i: &str) -> IResult<&str, Datetime> {
	alt((
		delimited(tag("d\'"), cut(datetime_raw), cut(char('\''))),
		delimited(char('\''), datetime_raw, char('\'')),
	))(i)
}

fn datetime_double(i: &str) -> IResult<&str, Datetime> {
	alt((
		delimited(tag("d\""), cut(datetime_raw), cut(char('"'))),
		delimited(char('"'), datetime_raw, char('"')),
	))(i)
}

pub fn datetime_all_raw(i: &str) -> IResult<&str, Datetime> {
	alt((nano, time, date))(i)
}

fn datetime_raw(i: &str) -> IResult<&str, Datetime> {
	alt((nano, time))(i)
}

fn date(i: &str) -> IResult<&str, Datetime> {
	let (i, year) = year(i)?;
	let (i, _) = char('-')(i)?;
	let (i, mon) = month(i)?;
	let (i, _) = char('-')(i)?;
	let (i, day) = day(i)?;
	convert(i, (year, mon, day), (0, 0, 0, 0), Utc.fix(), false)
}

fn time(i: &str) -> IResult<&str, Datetime> {
	let (i, year) = year(i)?;
	let (i, _) = char('-')(i)?;
	let (i, mon) = month(i)?;
	let (i, _) = char('-')(i)?;
	let (i, day) = day(i)?;
	let (i, _) = char('T')(i)?;
	let (i, hour) = hour(i)?;
	let (i, _) = char(':')(i)?;
	let (i, min) = minute(i)?;
	let (i, _) = char(':')(i)?;
	let (i, sec) = second(i)?;
	let (i, zone) = zone(i)?;
	convert(i, (year, mon, day), (hour, min, sec, 0), zone, false)
}

fn nano(i: &str) -> IResult<&str, Datetime> {
	let (i, year) = year(i)?;
	let (i, _) = char('-')(i)?;
	let (i, mon) = month(i)?;
	let (i, _) = char('-')(i)?;
	let (i, day) = day(i)?;
	let (i, _) = char('T')(i)?;
	let (i, hour) = hour(i)?;
	let (i, _) = char(':')(i)?;
	let (i, min) = minute(i)?;
	let (i, _) = char(':')(i)?;
	let (i, sec) = second(i)?;
	let (i, (nano, carry)) = nanosecond(i)?;
	let (i, zone) = zone(i)?;
	convert(i, (year, mon, day), (hour, min, sec, nano), zone, carry)
}

fn convert(
	i: &str,
	(year, mon, day): (i32, u32, u32),
	(hour, min, sec, nano): (u32, u32, u32, u32),
	zone: FixedOffset,
	carry: bool,
) -> IResult<&str, Datetime> {
	// Attempt to create date
	let d = NaiveDate::from_ymd_opt(year, mon, day)
		.ok_or_else(|| Err::Error(error_position!(i, ErrorKind::Verify)))?;
	// Attempt to create time
	let t = NaiveTime::from_hms_nano_opt(hour, min, sec, nano)
		.ok_or_else(|| Err::Error(error_position!(i, ErrorKind::Verify)))?;

	//
	let v = NaiveDateTime::new(d, t);

	let v = if carry {
		v + Duration::from_nanos(1)
	} else {
		v
	};

	// Attempt to create time
	let d = zone
		.from_local_datetime(&v)
		.earliest()
		.ok_or_else(|| Err::Error(error_position!(i, ErrorKind::Verify)))?
		.with_timezone(&Utc);
	// This is a valid datetime
	Ok((i, Datetime(d)))
}

fn year(i: &str) -> IResult<&str, i32> {
	let (i, s) = sign(i).unwrap_or((i, 1));
	let (i, y) = take_digits(i, 4)?;
	let v = s * y as i32;
	Ok((i, v))
}

fn month(i: &str) -> IResult<&str, u32> {
	take_digits_range(i, 2, 1..=12)
}

fn day(i: &str) -> IResult<&str, u32> {
	take_digits_range(i, 2, 1..=31)
}

fn hour(i: &str) -> IResult<&str, u32> {
	take_digits_range(i, 2, 0..=24)
}

fn minute(i: &str) -> IResult<&str, u32> {
	take_digits_range(i, 2, 0..=59)
}

fn second(i: &str) -> IResult<&str, u32> {
	take_digits_range(i, 2, 0..=60)
}

fn nanosecond(i: &str) -> IResult<&str, (u32, bool)> {
	let (i, _) = char('.')(i)?;
	let (i, digits) = digit1(i)?;

	let mut ns = 0u32;
	let mut carry = false;

	for d in digits.as_bytes().iter().rev().copied() {
		carry = (ns % 10) >= 5;
		ns /= 10;
		ns += (d - b'0') as u32 * 100_000_000;
	}

	Ok((i, (ns, carry)))
}

fn zone(i: &str) -> IResult<&str, FixedOffset> {
	alt((zone_utc, zone_all))(i)
}

fn zone_utc(i: &str) -> IResult<&str, FixedOffset> {
	let (i, _) = char('Z')(i)?;
	Ok((i, Utc.fix()))
}

fn zone_all(i: &str) -> IResult<&str, FixedOffset> {
	let (i, s) = sign(i)?;
	let (i, h) = hour(i)?;
	let (i, _) = char(':')(i)?;
	let (i, m) = minute(i)?;
	if h == 0 && m == 0 {
		Ok((i, Utc.fix()))
	} else if s < 0 {
		match FixedOffset::west_opt((h * 3600 + m * 60) as i32) {
			Some(v) => Ok((i, v)),
			None => Err(Err::Error(error_position!(i, ErrorKind::Verify))),
		}
	} else if s > 0 {
		match FixedOffset::east_opt((h * 3600 + m * 60) as i32) {
			Some(v) => Ok((i, v)),
			None => Err(Err::Error(error_position!(i, ErrorKind::Verify))),
		}
	} else {
		Ok((i, Utc.fix()))
	}
}

fn sign(i: &str) -> IResult<&str, i32> {
	map(alt((char('-'), char('+'))), |s: char| match s {
		'-' => -1,
		_ => 1,
	})(i)
}

#[cfg(test)]
mod tests {

	// use chrono::Date;

	use crate::{sql::Value, syn::Parse};

	use super::*;

	#[test]
	fn date_zone() {
		let sql = "2020-01-01T00:00:00Z";
		let res = datetime_all_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2020-01-01T00:00:00Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2020-01-01T00:00:00Z").unwrap());
	}

	#[test]
	fn date_time() {
		let sql = "2012-04-23T18:25:43Z";
		let res = datetime_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2012-04-23T18:25:43Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-23T18:25:43Z").unwrap());
	}

	#[test]
	fn date_time_nanos() {
		let sql = "2012-04-23T18:25:43.5631Z";
		let res = datetime_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2012-04-23T18:25:43.563100Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-23T18:25:43.563100Z").unwrap());
	}

	#[test]
	fn date_time_timezone_utc() {
		let sql = "2012-04-23T18:25:43.0000511Z";
		let res = datetime_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2012-04-23T18:25:43.000051100Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-23T18:25:43.000051100Z").unwrap());
	}

	#[test]
	fn date_time_timezone_pacific() {
		let sql = "2012-04-23T18:25:43.511-08:00";
		let res = datetime_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2012-04-24T02:25:43.511Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-24T02:25:43.511Z").unwrap());
	}

	#[test]
	fn date_time_timezone_pacific_partial() {
		let sql = "2012-04-23T18:25:43.511-08:30";
		let res = datetime_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2012-04-24T02:55:43.511Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-24T02:55:43.511Z").unwrap());
	}

	#[test]
	fn date_time_timezone_utc_nanoseconds() {
		let sql = "2012-04-23T18:25:43.5110000Z";
		let res = datetime_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2012-04-23T18:25:43.511Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-23T18:25:43.511Z").unwrap());
	}

	#[test]
	fn date_time_timezone_utc_sub_nanoseconds() {
		let sql = "2012-04-23T18:25:43.0000511Z";
		let res = datetime_raw(sql);
		let out = res.unwrap().1;
		assert_eq!("'2012-04-23T18:25:43.000051100Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-23T18:25:43.000051100Z").unwrap());
	}

	#[test]
	fn date_time_timezone_utc_sub_nanoseconds_from_value() {
		let sql = "'2012-04-23T18:25:43.0000511Z'";
		let res = Value::parse(sql);
		let Value::Datetime(out) = res else {
			panic!();
		};
		assert_eq!("'2012-04-23T18:25:43.000051100Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-23T18:25:43.000051100Z").unwrap());

		let sql = "d'2012-04-23T18:25:43.0000511Z'";
		let res = Value::parse(sql);
		let Value::Datetime(out) = res else {
			panic!();
		};
		assert_eq!("'2012-04-23T18:25:43.000051100Z'", format!("{}", out));
		assert_eq!(out, Datetime::try_from("2012-04-23T18:25:43.000051100Z").unwrap());
	}

	#[test]
	fn date_time_illegal_date() {
		// Hey! There's not a 31st of November!
		let sql = "2022-11-31T12:00:00.000Z";
		datetime_raw(sql).unwrap_err();
	}

	#[test]
	fn excessive_precision() {
		let (_, a) = datetime_raw("2024-06-06T12:00:00.0000999999999Z").unwrap();
		assert_eq!(a.to_string().as_str(), "2024-06-06T12:00:00.000100Z");
		let (_, a) = datetime_raw("2024-06-06T12:00:00.0000900000000Z").unwrap();
		assert_eq!(a.to_string().as_str(), "2024-06-06T12:00:00.000090Z");
		let (_, a) = datetime_raw("2024-06-06T12:00:00.0000999995Z").unwrap();
		assert_eq!(a.to_string().as_str(), "2024-06-06T12:00:00.000100Z");
		let (_, a) = datetime_raw("2024-06-06T12:00:00.00000000000000000000000009Z").unwrap();
		assert_eq!(a.to_string().as_str(), "2024-06-06T12:00:00Z");
		let (_, a) = datetime_raw("2024-06-06T12:00:00.0000000009Z").unwrap();
		assert_eq!(a.to_string().as_str(), "2024-06-06T12:00:00.000000001Z");
		let (_, a) = datetime_raw("2024-12-31T23:59:59.9999999999Z").unwrap();
		assert_eq!(a.to_string().as_str(), "2025-01-01T00:00:00Z");
	}
}
