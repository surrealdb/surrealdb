use std::ops::RangeInclusive;

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};
use thiserror::Error;

use crate::{
	sql::Datetime,
	syn::token::{Token, TokenKind},
};

use super::{Error as LexError, Lexer};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PartError {
	#[error("value outside of allowed range")]
	OutsideRange,
	#[error("missing digit(s)")]
	MissingDigits,
	#[error("too many digits")]
	TooManyDigits,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("invalid year, {0}")]
	Year(PartError),
	#[error("invalid month, {0}")]
	Month(PartError),
	#[error("invalid day, {0}")]
	Day(PartError),
	#[error("invalid hour, {0}")]
	Hour(PartError),
	#[error("invalid time minute, {0}")]
	Minute(PartError),
	#[error("invalid second, {0}")]
	Second(PartError),
	#[error("invalid nano_seconds, {0}")]
	NanoSeconds(PartError),
	#[error("invalid time-zone hour, {0}")]
	TimeZoneHour(PartError),
	#[error("invalid time-zone minute, {0}")]
	TimeZoneMinute(PartError),
	#[error("missing seperator `{}`",*(.0) as char)]
	MissingSeparator(u8),
	#[error("expected date-time strand to end")]
	ExpectedEnd,
	#[error("missing time-zone")]
	MissingTimeZone,
	#[error("date does not exist")]
	NonExistantDate,
	#[error("time does not exist")]
	NonExistantTime,
	#[error("time-zone offset too big")]
	TimeZoneOutOfRange,
}

impl<'a> Lexer<'a> {
	/// Lex a date-time strand.
	pub fn lex_datetime(&mut self, double: bool) -> Token {
		match self.lex_datetime_err(double) {
			Ok(x) => {
				self.datetime = Some(x);
				self.finish_token(TokenKind::DateTime)
			}
			Err(e) => self.invalid_token(LexError::DateTime(e)),
		}
	}

	/// Lex datetime without enclosing `"` or `'` but return a result or parser error.
	pub fn lex_datetime_raw_err(&mut self) -> Result<Datetime, Error> {
		let negative = match self.reader.peek() {
			Some(b'+') => {
				self.reader.next();
				false
			}
			Some(b'-') => {
				self.reader.next();
				true
			}
			_ => false,
		};

		let mut year = self.lex_datetime_part(4, 0..=9999).map_err(Error::Year)? as i16;
		if negative {
			year = -year;
		}
		if !self.eat(b'-') {
			return Err(Error::MissingSeparator(b'-'));
		}
		let month = self.lex_datetime_part(2, 1..=12).map_err(Error::Month)?;
		if !self.eat(b'-') {
			return Err(Error::MissingSeparator(b'-'));
		}
		let day = self.lex_datetime_part(2, 1..=31).map_err(Error::Day)?;

		if !self.eat(b'T') {
			let Some(date) = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) else {
				return Err(Error::NonExistantDate);
			};
			let time = NaiveTime::default();
			let date_time = NaiveDateTime::new(date, time);

			let datetime = Utc
				.fix()
				.from_local_datetime(&date_time)
				.earliest()
				// this should never panic with a fixed offset.
				.unwrap()
				.with_timezone(&Utc);

			return Ok(Datetime(datetime));
		}

		let hour = self.lex_datetime_part(2, 0..=24).map_err(Error::Hour)?;
		if !self.eat(b':') {
			return Err(Error::MissingSeparator(b':'));
		}

		let minutes = self.lex_datetime_part(2, 0..=59).map_err(Error::Minute)?;

		if !self.eat(b':') {
			return Err(Error::MissingSeparator(b':'));
		}

		let seconds = self.lex_datetime_part(2, 0..=59).map_err(Error::Second)?;

		// nano seconds
		let nano = if let Some(b'.') = self.reader.peek() {
			self.reader.next();
			// check if there is atleast one digit.
			if !matches!(self.reader.peek(), Some(b'0'..=b'9')) {
				return Err(Error::NanoSeconds(PartError::MissingDigits));
			}
			let mut number = 0u32;
			for i in 0..9 {
				let Some(c) = self.reader.peek() else {
					// always invalid token, just let the next section handle the error.
					break;
				};
				if !c.is_ascii_digit() {
					// If digits are missing they are counted as 0's
					for _ in i..9 {
						number *= 10;
					}
					break;
				}
				self.reader.next();
				number *= 10;
				number += (c - b'0') as u32;
			}
			// ensure nano_seconds are at most 9 digits.
			if matches!(self.reader.peek(), Some(b'0'..=b'9')) {
				return Err(Error::NanoSeconds(PartError::TooManyDigits));
			}
			number
		} else {
			0
		};

		// time zone
		let time_zone = match self.reader.peek() {
			Some(b'Z') => {
				self.reader.next();
				None
			}
			Some(x @ (b'-' | b'+')) => {
				self.reader.next();
				let negative = x == b'-';
				let hour = self.lex_datetime_part(2, 0..=24).map_err(Error::TimeZoneHour)? as i32;
				let Some(b':') = self.reader.next() else {
					return Err(Error::MissingSeparator(b':'));
				};
				let minute =
					self.lex_datetime_part(2, 0..=59).map_err(Error::TimeZoneMinute)? as i32;
				let time = hour * 3600 + minute * 60;
				if negative {
					Some(-time)
				} else {
					Some(time)
				}
			}
			_ => return Err(Error::MissingTimeZone),
		};

		// calculate the given datetime from individual parts.
		let Some(date) = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) else {
			return Err(Error::NonExistantDate);
		};
		let Some(time) =
			NaiveTime::from_hms_nano_opt(hour as u32, minutes as u32, seconds as u32, nano)
		else {
			return Err(Error::NonExistantTime);
		};

		let date_time = NaiveDateTime::new(date, time);

		let zone = match time_zone {
			None => Utc.fix(),
			Some(offset) => if offset < 0 {
				FixedOffset::west_opt(-offset)
			} else {
				FixedOffset::east_opt(offset)
			}
			.ok_or(Error::TimeZoneOutOfRange)?,
		};

		let datetime = zone
			.from_local_datetime(&date_time)
			.earliest()
			// this should never panic with a fixed offset.
			.unwrap()
			.with_timezone(&Utc);

		Ok(Datetime(datetime))
	}

	/// Lex full datetime but return an result instead of a token.
	pub fn lex_datetime_err(&mut self, double: bool) -> Result<Datetime, Error> {
		let datetime = self.lex_datetime_raw_err()?;

		let end_char = if double {
			b'"'
		} else {
			b'\''
		};

		if !self.eat(end_char) {
			return Err(Error::ExpectedEnd);
		}

		Ok(datetime)
	}

	/// Lexes a digit part of date time.
	///
	/// This function eats an amount of digits and then checks if the value the digits represent
	/// is within the given range.
	pub fn lex_datetime_part(
		&mut self,
		mut amount: u8,
		range: RangeInclusive<u16>,
	) -> Result<u16, PartError> {
		let mut value = 0u16;

		while amount != 0 {
			value *= 10;
			let Some(char) = self.reader.peek() else {
				return Err(PartError::MissingDigits);
			};
			if !char.is_ascii_digit() {
				return Err(PartError::MissingDigits);
			}
			self.reader.next();
			value += (char - b'0') as u16;
			amount -= 1;
		}

		if matches!(self.reader.peek(), Some(b'0'..=b'8')) {
			return Err(PartError::TooManyDigits);
		}

		if !range.contains(&value) {
			return Err(PartError::OutsideRange);
		}
		Ok(value)
	}
}
