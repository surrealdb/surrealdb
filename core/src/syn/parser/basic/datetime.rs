use std::ops::RangeInclusive;

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};

use crate::{
	fnc::array::min,
	sql::Datetime,
	syn::{
		parser::{
			mac::{expected_whitespace, unexpected},
			ParseError, ParseErrorKind, ParseResult, Parser,
		},
		token::{t, DatetimeChars, TokenKind},
	},
};

impl Parser<'_> {
	pub fn parse_datetime(&mut self) -> ParseResult<Datetime> {
		let start = self.peek();
		let double = match start.kind {
			t!("d\"") => true,
			t!("d'") => false,
			x => unexpected!(self, x, "a datetime"),
		};

		self.pop_peek();

		let start_date = self.peek_whitespace().span;

		let year_neg = self.eat_whitespace(t!("-"));
		if !year_neg {
			self.eat_whitespace(t!("+"));
		}

		let year = self.parse_datetime_digits(4, 0..=9999)?;
		expected_whitespace!(self, t!("-"));
		let month = self.parse_datetime_digits(2, 1..=12)?;
		expected_whitespace!(self, t!("-"));
		let day = self.parse_datetime_digits(2, 1..=31)?;

		let date_span = start_date.covers(self.last_span());

		let year = if year_neg {
			-(year as i32)
		} else {
			year as i32
		};

		let date = NaiveDate::from_ymd_opt(year, month as u32, day as u32)
			.ok_or_else(|| ParseError::new(ParseErrorKind::InvalidDatetimeDate, date_span))?;

		if !self.eat(TokenKind::DatetimeChars(DatetimeChars::T)) {
			return Err(ParseError::new(
				ParseErrorKind::Unexpected {
					found: TokenKind::Identifier,
					expected: "the charater `T`",
				},
				self.recent_span(),
			));
		}

		let start_time = self.peek_whitespace().span;

		let hour = self.parse_datetime_digits(2, 0..=24)?;
		expected_whitespace!(self, t!(":"));
		let minute = self.parse_datetime_digits(2, 0..=59)?;
		expected_whitespace!(self, t!(":"));
		let second = self.parse_datetime_digits(2, 0..=59)?;

		let nanos = if self.eat_whitespace(t!(".")) {
			let digits_token = expected_whitespace!(self, TokenKind::Digits);
			let slice = self.span_bytes(digits_token.span);

			if slice.len() > 9 {
				return Err(ParseError::new(
					ParseErrorKind::TooManyNanosecondsDatetime,
					digits_token.span,
				));
			}

			let mut number = 0u32;
			for i in 0..9 {
				let Some(c) = slice.get(i).copied() else {
					// If digits are missing they are counted as 0's
					for _ in i..9 {
						number *= 10;
					}
					break;
				};
				number *= 10;
				number += (c - b'0') as u32;
			}

			number
		} else {
			0
		};

		let time_span = start_time.covers(self.last_span());

		let time =
			NaiveTime::from_hms_nano_opt(hour as u32, minute as u32, second as u32, nanos)
				.ok_or_else(|| ParseError::new(ParseErrorKind::InvalidDatetimeTime, time_span))?;

		let peek = self.peek_whitespace();
		let timezone = match peek.kind {
			t!("+") => self.parse_datetime_timezone(false)?,
			t!("-") => self.parse_datetime_timezone(true)?,
			TokenKind::DatetimeChars(DatetimeChars::Z) => {
				self.pop_peek();
				Utc.fix()
			}
			x => unexpected!(self, x, "`Z` or a timezone"),
		};

		if double {
			expected_whitespace!(self, t!("\""));
		} else {
			expected_whitespace!(self, t!("'"));
		}

		let date_time = NaiveDateTime::new(date, time);

		let datetime = timezone
			.from_local_datetime(&date_time)
			.earliest()
			// this should never panic with a fixed offset.
			.unwrap()
			.with_timezone(&Utc);

		Ok(Datetime(datetime))
	}

	fn parse_datetime_timezone(&mut self, neg: bool) -> ParseResult<FixedOffset> {
		self.pop_peek();
		let hour = self.parse_datetime_digits(2, 0..=23)?;
		expected_whitespace!(self, t!(":"));
		let minute = self.parse_datetime_digits(2, 0..=59)?;

		// The range checks on the digits ensure that the offset can't exceed 23:59 so below
		// unwraps won't panic.
		if neg {
			Ok(FixedOffset::west_opt((hour * 3600 + minute * 60) as i32).unwrap())
		} else {
			Ok(FixedOffset::east_opt((hour * 3600 + minute * 60) as i32).unwrap())
		}
	}

	fn parse_datetime_digits(
		&mut self,
		len: usize,
		range: RangeInclusive<usize>,
	) -> ParseResult<usize> {
		let t = self.peek_whitespace();
		match t.kind {
			TokenKind::Digits => {}
			x => unexpected!(self, x, "datetime digits"),
		}

		let digits_str = self.span_str(t.span);
		if digits_str.len() != len {
			return Err(ParseError::new(
				ParseErrorKind::InvalidDatetimePart {
					len,
				},
				t.span,
			));
		}

		self.pop_peek();

		// This should always parse as it has been validated by the lexer.
		let value = digits_str.parse().unwrap();

		if !range.contains(&value) {
			return Err(ParseError::new(
				ParseErrorKind::OutrangeDatetimePart {
					range,
				},
				t.span,
			));
		}

		Ok(value)
	}
}
