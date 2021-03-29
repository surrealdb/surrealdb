use crate::sql::common::{take_digits, take_digits_range, take_u32};
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::map;
use nom::sequence::delimited;
use nom::IResult;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Datetime {
	pub value: DateTime<Utc>,
}

impl Default for Datetime {
	fn default() -> Self {
		Datetime {
			value: Utc::now(),
		}
	}
}

impl<'a> From<&'a str> for Datetime {
	fn from(s: &str) -> Self {
		match datetime_raw(s) {
			Ok((_, v)) => v,
			Err(_) => Datetime::default(),
		}
	}
}

impl fmt::Display for Datetime {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "\"{:?}\"", self.value)
	}
}

impl Serialize for Datetime {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_some(&self.value)
		} else {
			let mut val = serializer.serialize_struct("Datetime", 1)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

pub fn datetime(i: &str) -> IResult<&str, Datetime> {
	alt((
		delimited(tag("\""), datetime_raw, tag("\"")),
		delimited(tag("\'"), datetime_raw, tag("\'")),
	))(i)
}

pub fn datetime_raw(i: &str) -> IResult<&str, Datetime> {
	alt((nano, time, date))(i)
}

fn date(i: &str) -> IResult<&str, Datetime> {
	let (i, year) = year(i)?;
	let (i, _) = tag("-")(i)?;
	let (i, mon) = month(i)?;
	let (i, _) = tag("-")(i)?;
	let (i, day) = day(i)?;

	let d = Utc.ymd(year, mon, day).and_hms(0, 0, 0);
	Ok((
		i,
		Datetime {
			value: d,
		},
	))
}

fn time(i: &str) -> IResult<&str, Datetime> {
	let (i, year) = year(i)?;
	let (i, _) = tag("-")(i)?;
	let (i, mon) = month(i)?;
	let (i, _) = tag("-")(i)?;
	let (i, day) = day(i)?;
	let (i, _) = tag("T")(i)?;
	let (i, hour) = hour(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, min) = minute(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, sec) = second(i)?;
	let (i, zone) = zone(i)?;

	let v = match zone {
		Some(z) => {
			let d = z.ymd(year, mon, day).and_hms(hour, min, sec);
			let d = d.with_timezone(&Utc);
			Datetime {
				value: d,
			}
		}
		None => {
			let d = Utc.ymd(year, mon, day).and_hms(hour, min, sec);
			Datetime {
				value: d,
			}
		}
	};

	Ok((i, v))
}

fn nano(i: &str) -> IResult<&str, Datetime> {
	let (i, year) = year(i)?;
	let (i, _) = tag("-")(i)?;
	let (i, mon) = month(i)?;
	let (i, _) = tag("-")(i)?;
	let (i, day) = day(i)?;
	let (i, _) = tag("T")(i)?;
	let (i, hour) = hour(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, min) = minute(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, sec) = second(i)?;
	let (i, nano) = nanosecond(i)?;
	let (i, zone) = zone(i)?;

	let v = match zone {
		Some(z) => {
			let d = z.ymd(year, mon, day).and_hms_nano(hour, min, sec, nano);
			let d = d.with_timezone(&Utc);
			Datetime {
				value: d,
			}
		}
		None => {
			let d = Utc.ymd(year, mon, day).and_hms_nano(hour, min, sec, nano);
			Datetime {
				value: d,
			}
		}
	};

	Ok((i, v))
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
	take_digits_range(i, 2, 0..=59)
}

fn nanosecond(i: &str) -> IResult<&str, u32> {
	let (i, _) = tag(".")(i)?;
	let (i, v) = take_u32(i)?;
	Ok((i, v))
}

fn zone(i: &str) -> IResult<&str, Option<FixedOffset>> {
	alt((zone_utc, zone_all))(i)
}

fn zone_utc(i: &str) -> IResult<&str, Option<FixedOffset>> {
	let (i, _) = tag("Z")(i)?;
	Ok((i, None))
}

fn zone_all(i: &str) -> IResult<&str, Option<FixedOffset>> {
	let (i, s) = sign(i)?;
	let (i, h) = hour(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, m) = minute(i)?;
	if h == 0 && m == 0 {
		Ok((i, None))
	} else if s < 0 {
		Ok((i, { Some(FixedOffset::west((h * 3600 + m) as i32)) }))
	} else if s > 0 {
		Ok((i, { Some(FixedOffset::east((h * 3600 + m) as i32)) }))
	} else {
		Ok((i, None))
	}
}

fn sign(i: &str) -> IResult<&str, i32> {
	map(alt((tag("-"), tag("+"))), |s: &str| match s {
		"-" => -1,
		_ => 1,
	})(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn date() {
		let sql = "2012-04-23";
		let res = datetime_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("\"2012-04-23T00:00:00Z\"", format!("{}", out));
	}

	#[test]
	fn date_time() {
		let sql = "2012-04-23T18:25:43Z";
		let res = datetime_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("\"2012-04-23T18:25:43Z\"", format!("{}", out));
	}

	#[test]
	fn date_time_nanos() {
		let sql = "2012-04-23T18:25:43.5631Z";
		let res = datetime_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("\"2012-04-23T18:25:43.000005631Z\"", format!("{}", out));
	}

	#[test]
	fn date_time_timezone_utc() {
		let sql = "2012-04-23T18:25:43.511Z";
		let res = datetime_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("\"2012-04-23T18:25:43.000000511Z\"", format!("{}", out));
	}

	#[test]
	fn date_time_timezone_pacific() {
		let sql = "2012-04-23T18:25:43.511-08:00";
		let res = datetime_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("\"2012-04-24T02:25:43.000000511Z\"", format!("{}", out));
	}
}
