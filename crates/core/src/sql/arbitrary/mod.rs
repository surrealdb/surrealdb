mod idiom;
mod parts;
mod statements;
mod utils;
use std::time;

use arbitrary::{Arbitrary, Result, Unstructured};
use chrono::{
	FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset as _, TimeZone as _, Timelike as _,
	Utc,
};
pub(crate) use idiom::*;
pub(crate) use parts::*;
use regex::RegexBuilder;
use rust_decimal::Decimal;
pub(crate) use utils::*;

use crate::sql::changefeed::ChangeFeed;
use crate::sql::statements::SleepStatement;
use crate::sql::{Bytes, Datetime, Duration, Regex};

impl<'a> Arbitrary<'a> for ChangeFeed {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			expiry: u.arbitrary()?,
			store_diff: bool::arbitrary(u)?,
		})
	}
}

impl<'a> Arbitrary<'a> for SleepStatement {
	fn arbitrary(_u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			// When fuzzing we don't want to sleep, that's slow... we want insomnia.
			duration: Duration(time::Duration::new(0, 0)),
		})
	}
}

impl<'a> Arbitrary<'a> for Bytes {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Bytes(u.arbitrary()?))
	}
}

impl<'a> Arbitrary<'a> for Duration {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Duration(u.arbitrary()?))
	}
}

impl<'a> Arbitrary<'a> for Datetime {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let date = u.arbitrary::<NaiveDate>()?;
		let time = u.arbitrary::<NaiveTime>()?;
		// Arbitrary was able to create times with 60 seconds instead of the 59 second limit.
		let time = time.with_second(time.second() % 60).expect("0 to 59 is a valid second");
		let time = time
			.with_nanosecond(time.nanosecond() % 1_000_000_000)
			.expect("0 to 999_999_999 is a valid nanosecond");

		let offset = if u.arbitrary()? {
			Utc.fix()
		} else {
			let hour = u.int_in_range(0..=23)?;
			let minute = u.int_in_range(0..=59)?;
			if u.arbitrary()? {
				FixedOffset::west_opt(hour * 3600 + minute * 60)
					.expect("valid because range was ensured")
			} else {
				FixedOffset::east_opt(hour * 3600 + minute * 60)
					.expect("valid because range was ensured")
			}
		};

		let datetime = NaiveDateTime::new(date, time);

		let Some(x) = offset.from_local_datetime(&datetime).earliest() else {
			return Err(arbitrary::Error::IncorrectFormat);
		};

		Ok(Datetime(x.with_timezone(&Utc)))
	}
}

impl<'a> Arbitrary<'a> for Regex {
	fn arbitrary(u: &mut ::arbitrary::Unstructured<'a>) -> ::arbitrary::Result<Self> {
		let ast = regex_syntax::ast::Ast::arbitrary(u)?;
		let src = &ast.to_string();
		if src.is_empty() {
			return Err(::arbitrary::Error::IncorrectFormat);
		}
		let regex =
			RegexBuilder::new(src).build().map_err(|_| ::arbitrary::Error::IncorrectFormat)?;
		Ok(Regex(regex))
	}
}

pub fn arb_decimal<'a>(u: &mut Unstructured<'a>) -> Result<Decimal> {
	Ok(Decimal::arbitrary(u)?.normalize())
}
