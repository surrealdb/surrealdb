use std::ops::RangeInclusive;

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset as _, TimeZone as _, Utc};

use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::{BytesReader, Lexer};
use crate::types::PublicDatetime;

impl Lexer<'_> {
	/// Lex a datetime from a string.
	pub fn lex_datetime(str: &str) -> Result<PublicDatetime, SyntaxError> {
		// Taken from chrono docs, who took it from the rfc docs.
		// We don't use the chrone parse function as ours provides better error feedback.
		//
		// an adapted RFC 3339 syntax from Section 5.6:
		//
		// date-fullyear  = 4DIGIT
		// date-month     = 2DIGIT ; 01-12
		// date-mday      = 2DIGIT ; 01-28, 01-29, 01-30, 01-31 based on month/year
		// time-hour      = 2DIGIT ; 00-23
		// time-minute    = 2DIGIT ; 00-59
		// time-second    = 2DIGIT ; 00-58, 00-59, 00-60 based on leap second rules
		// time-secfrac   = "." 1*DIGIT
		// time-numoffset = ("+" / "-") time-hour ":" time-minute
		// time-offset    = "Z" / time-numoffset
		// partial-time   = time-hour ":" time-minute ":" time-second [time-secfrac]
		// full-date      = date-fullyear "-" date-month "-" date-mday
		// full-time      = partial-time time-offset
		// date-time      = full-date "T" full-time
		//
		// some notes:
		//
		// - quoted characters can be in any mixture of lower and upper cases.
		//
		// - it may accept any number of fractional digits for seconds. Chrono only supports up to
		//   nanoseconds this means that we should skip digits past first 9 digits.
		//
		// - unlike RFC 2822, the valid offset ranges from -23:59 to +23:59.
		//
		// - For readability a full-date and a full-time may be separated by a space character
		//
		//
		// Our implementation is actually slightly more flexible then the RFC 3339 demands.
		// We support ommiting the time part of the datetime in which case the resulting datetime
		// will be midnight. We also support the larger range of up to the year 99_9999 instead of
		// 9999. This is to deal with chrono's range of years which is larger then the rfc allows
		// for.

		let mut reader = BytesReader::new(str.as_bytes());

		let date_start = reader.offset();

		let neg = reader.eat(b'-');
		if !neg {
			reader.eat(b'+');
		}

		// remove limit as the this section is limited by the number of characters.
		let year = Self::parse_datetime_digits(&mut reader, 4..=6, 0..=usize::MAX)?;
		Self::expect_seperator(&mut reader, b'-')?;
		let month = Self::parse_datetime_digits(&mut reader, 2..=2, 1..=12)?;
		Self::expect_seperator(&mut reader, b'-')?;
		let day = Self::parse_datetime_digits(&mut reader, 2..=2, 1..=31)?;

		let year = if neg {
			-(year as i32)
		} else {
			year as i32
		};

		let date = NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(
			|| syntax_error!("Invalid Datetime date: date outside of valid range", @reader.span_since(date_start)),
		)?;

		let before = reader.offset();
		match reader.next() {
			Some(b't' | b'T' | b' ') => {}
			Some(x) => {
				let c = reader.convert_to_char(x)?;
				let span = reader.span_since(before);
				bail!("Unexpected character `{c}`, expected time seperator `T`", @span)
			}
			None => {
				let time = NaiveTime::default();
				let date_time = NaiveDateTime::new(date, time);

				// Since there is no offset, as there is no time specified the `earliest` call
				// cannot fail.
				let datetime = Utc
					.fix()
					.from_local_datetime(&date_time)
					.earliest()
					.expect("valid datetime")
					.with_timezone(&Utc);

				return Ok(PublicDatetime::from(datetime));
			}
		}

		let time_start = reader.offset();

		let hour = Self::parse_datetime_digits(&mut reader, 2..=2, 0..=24)?;
		Self::expect_seperator(&mut reader, b':')?;
		let minute = Self::parse_datetime_digits(&mut reader, 2..=2, 0..=59)?;
		Self::expect_seperator(&mut reader, b':')?;
		let second = Self::parse_datetime_digits(&mut reader, 2..=2, 0..=60)?;

		let nanos = if reader.eat(b'.') {
			let nanos_start = reader.offset();
			let mut number = 0u32;
			let mut count = 0;

			loop {
				let Some(d) = reader.peek() else {
					break;
				};

				if !d.is_ascii_digit() {
					break;
				}

				reader.next();

				if count == 9 {
					if d - b'0' >= 5 {
						//round up.
						number += 1;
					}
					//skip all remaining fractional seconds
					continue;
				} else if count >= 9 {
					//skip all remaining fractional seconds
					continue;
				}

				number *= 10;
				number += (d - b'0') as u32;
				count += 1;
			}

			if count == 0 {
				bail!("Invalid datetime nanoseconds, expected at least a single digit", @reader.span_since(nanos_start))
			}

			// if digits are missing they count as 0's
			for _ in count..9 {
				number *= 10;
			}

			number
		} else {
			0
		};

		let time = NaiveTime::from_hms_nano_opt(hour as u32, minute as u32, second as u32, nanos)
			.ok_or_else(
				|| syntax_error!("Invalid Datetime time: time outside of valid range", @reader.span_since(time_start)),
			)?;

		let timezone_start = reader.offset();
		let timezone = match reader.next() {
			Some(x @ (b'-' | b'+')) => {
				let hour = Self::parse_datetime_digits(&mut reader, 2..=2, 0..=23)?;
				Self::expect_seperator(&mut reader, b':')?;
				let minutes = Self::parse_datetime_digits(&mut reader, 2..=2, 0..=59)?;

				// The range checks on the digits ensure that the offset can't exceed 23:59 so
				// below unwraps won't panic.
				if x == b'-' {
					FixedOffset::west_opt((hour * 3600 + minutes * 60) as i32)
						.expect("valid timezone offset")
				} else {
					FixedOffset::east_opt((hour * 3600 + minutes * 60) as i32)
						.expect("valid timezone offset")
				}
			}
			Some(b'Z' | b'z') => Utc.fix(),
			Some(x) => {
				let c = reader.convert_to_char(x)?;
				let span = reader.span_since(before);
				bail!("Unexpected character `{c}`, expected `Z` or a timezone offset.",@span)
			}
			None => {
				let span = reader.span_since(timezone_start);
				bail!("Invalid end of datetime, expected datetime timezone", @span)
			}
		};

		let date_time = NaiveDateTime::new(date, time);

		let datetime = timezone
			.from_local_datetime(&date_time)
			.earliest()
			// this should never panic with a fixed offset
			.expect("valid datetime with fixed offset")
			.with_timezone(&Utc);

		Ok(PublicDatetime::from(datetime))
	}

	fn expect_seperator(reader: &mut BytesReader, sep: u8) -> Result<(), SyntaxError> {
		match reader.peek() {
			Some(x) if x == sep => {
				reader.next();
				Ok(())
			}
			Some(x) => {
				let before = reader.offset();
				reader.next();
				let c = reader.convert_to_char(x)?;
				let span = reader.span_since(before);
				bail!(
					"Unexpected character `{c}`, expected datetime seperator characters `{}`",
					sep as char,
					@span
				)
			}
			None => {
				let before = reader.offset();
				let span = reader.span_since(before);
				bail!(
					"Expected end of string, expected datetime seperator character `{}`",
					sep as char,
					@span
				);
			}
		}
	}

	fn parse_datetime_digits(
		reader: &mut BytesReader,
		amount: RangeInclusive<usize>,
		range: RangeInclusive<usize>,
	) -> Result<usize, SyntaxError> {
		let start = reader.offset();

		let mut value = 0usize;

		for _ in 0..(*amount.start()) {
			let offset = reader.offset();
			match reader.next() {
				Some(x) if x.is_ascii_digit() => {
					value *= 10;
					value += (x - b'0') as usize;
				}
				Some(x) => {
					let char = reader.convert_to_char(x)?;
					let span = reader.span_since(offset);
					bail!("Invalid datetime, expected digit character found `{char}`", @span);
				}
				None => {
					let span = reader.span_since(offset);
					bail!("Expected end of datetime, expected datetime digit character", @span);
				}
			}
		}

		for _ in amount {
			match reader.peek() {
				Some(x) if x.is_ascii_digit() => {
					reader.next();
					value *= 10;
					value += (x - b'0') as usize;
				}
				_ => break,
			}
		}

		if !range.contains(&value) {
			let span = reader.span_since(start);
			bail!("Invalid datetime digit section, section not within allowed range",
				@span => "This section must be within {}..={}",range.start(),range.end());
		}

		Ok(value)
	}
}
