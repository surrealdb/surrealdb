use std::ops::RangeInclusive;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};
use logos::{Lexer, Logos};

use crate::Error;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
enum DateTimeToken {
	#[regex("[0-9]+")]
	Digits,
	#[token(".")]
	Dot,
	#[token("T")]
	#[token("t")]
	#[token(" ")]
	T,
	#[token("Z")]
	#[token("z")]
	Z,
	#[token("+")]
	Plus,
	#[token("-")]
	Dash,
	#[token(":")]
	Colon,
}

fn expect_token(
	lexer: &mut Lexer<DateTimeToken>,
	expected: DateTimeToken,
	expected_description: &str,
) -> Result<(), Error> {
	if let Some(Ok(x)) = lexer.next()
		&& x == expected
	{
		return Ok(());
	}

	Err(Error {
		span: lexer.span(),
		message: format!(
			"Invalid datetime token, unexpected character, expected {expected_description}"
		),
	})
}

fn expect_digits(
	lexer: &mut Lexer<DateTimeToken>,
	count: RangeInclusive<usize>,
	range: RangeInclusive<u32>,
) -> Result<u32, Error> {
	expect_token(lexer, DateTimeToken::Digits, "digits")?;

	let digits = lexer.slice();
	if !count.contains(&digits.len()) {
		if count.start() != count.end() {
			return Err(Error {
				span: lexer.span(),
				message: format!(
					"Invalid datetime token, invalid number of digits, expected between {} and {} digits",
					count.start(),
					count.end()
				),
			});
		} else {
			return Err(Error {
				span: lexer.span(),
				message: format!(
					"Invalid datetime token, invalid number of digits, expected {} digits",
					count.start()
				),
			});
		}
	}

	let value = digits.parse().expect("caller to enforce integer limits via count limit");

	if !range.contains(&value) {
		return Err(Error {
			span: lexer.span(),
			message: format!(
				"Invalid datetime token, digit value out of range, expected value between {} and {}",
				range.start(),
				range.end()
			),
		});
	}

	Ok(value)
}

/// Parse a datetime in the SurrealQL datetime format.
///
/// The format is an adapted RFC 3339 syntax from Section 5.6:
///
/// ```text
/// date-fullyear  = 4DIGIT
/// date-month     = 2DIGIT ; 01-12
/// date-mday      = 2DIGIT ; 01-28, 01-29, 01-30, 01-31 based on month/year
/// time-hour      = 2DIGIT ; 00-23
/// time-minute    = 2DIGIT ; 00-59
/// time-second    = 2DIGIT ; 00-58, 00-59, 00-60 based on leap second rules
/// time-secfrac   = "." 1*DIGIT
/// time-numoffset = ("+" / "-") time-hour ":" time-minute
/// time-offset    = "Z" / time-numoffset
/// partial-time   = time-hour ":" time-minute ":" time-second [time-secfrac]
/// full-date      = date-fullyear "-" date-month "-" date-mday
/// full-time      = partial-time time-offset
/// date-time      = full-date "T" full-time
/// ```
///
/// some notes:
///
/// - quoted characters can be in any mixture of lower and upper cases.
///
/// - it may accept any number of fractional digits for seconds. Chrono only supports up to
///   nanoseconds this means that we skip digits past the first 9 digits.
///
/// - unlike RFC 2822, the valid offset ranges from -23:59 to +23:59.
///
/// - For readability a full-date and a full-time may be separated by a space character
///
/// Our implementation is actually slightly more flexible then the RFC 3339 demands.
/// We support omitting the time part of the datetime in which case the resulting datetime
/// will be midnight. We also support the larger range of up to the year 99_9999 instead of
/// 9999. This is to deal with chrono's range of years which is larger then the rfc allows
/// for.
///
/// We don't use the chrono parse function as ours provides better error feedback.
pub fn datetime(source: &str) -> Result<DateTime<Utc>, Error> {
	let mut lexer = DateTimeToken::lexer(source);

	let mut peeked_lexer = lexer.clone();
	let peek = peeked_lexer.next();
	// check for possible sign starting datetime.
	let neg = if let Some(Ok(DateTimeToken::Plus)) = peek {
		lexer = peeked_lexer;
		false
	} else if let Some(Ok(DateTimeToken::Dash)) = peek {
		lexer = peeked_lexer;
		true
	} else {
		false
	};

	// parse date section.
	let year = expect_digits(&mut lexer, 4..=6, 0..=u32::MAX)?;
	expect_token(&mut lexer, DateTimeToken::Dash, "`-`")?;
	let month = expect_digits(&mut lexer, 2..=2, 1..=12)?;
	expect_token(&mut lexer, DateTimeToken::Dash, "`-`")?;
	let day = expect_digits(&mut lexer, 2..=2, 1..=31)?;

	let year = if neg {
		-(year as i32)
	} else {
		year as i32
	};

	let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
		let date_span = 0..lexer.span().end;
		return Err(Error {
			span: date_span,
			message: "Invalid datetime token, invalid datetime date".to_string(),
		});
	};

	match lexer.next() {
		Some(Ok(DateTimeToken::T)) => {}
		None => {
			// NOTE: This is an extension of the normal RFC datetimes.
			// We allow omiting any time info.
			let time = NaiveTime::default();
			let datetime = NaiveDateTime::new(date, time);
			let datetime = Utc
				.fix()
				.from_local_datetime(&datetime)
				.earliest()
				.expect("valid datetime")
				.with_timezone(&Utc);
			return Ok(datetime);
		}
		_ => {
			return Err(Error {
				span: lexer.span(),
				message: "Invalid datetime token, invalid datetime date".to_string(),
			});
		}
	}

	// Start of the time section of the datetime.
	let time_start = lexer.span().end;
	let hour = expect_digits(&mut lexer, 2..=2, 0..=24)?;
	expect_token(&mut lexer, DateTimeToken::Colon, "`:`")?;
	let minute = expect_digits(&mut lexer, 2..=2, 0..=59)?;
	expect_token(&mut lexer, DateTimeToken::Colon, "`:`")?;
	let second = expect_digits(&mut lexer, 2..=2, 0..=60)?;

	let mut peeked_lexer = lexer.clone();
	let peek = peeked_lexer.next();
	// parsing possible nanoseconds.
	let nanos = if let Some(Ok(DateTimeToken::Dot)) = peek {
		lexer = peeked_lexer;

		expect_token(&mut lexer, DateTimeToken::Digits, "nanoseconds digits")?;

		let digits = lexer.slice();
		// There can be any number of nanoseconds digits
		// however for precision we cut of after nine digits.
		let digits = &digits[..digits.len().min(9)];
		let mut value: u32 = digits.parse().expect("lexer should have returned valid number");

		// If digits are lacking we need to multiply because it is a mantissa.
		for _ in digits.len()..9 {
			value *= 10;
		}
		value
	} else {
		0
	};

	let Some(time) = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos) else {
		let time_span = time_start..lexer.span().end;
		return Err(Error {
			span: time_span,
			message: "Invalid datetime token, invalid datetime time".to_string(),
		});
	};

	let timezone_start = lexer.span().end;
	let timezone = match lexer.next() {
		Some(Ok(x @ (DateTimeToken::Plus | DateTimeToken::Dash))) => {
			let hour = expect_digits(&mut lexer, 2..=2, 0..=23)?;
			expect_token(&mut lexer, DateTimeToken::Colon, "`:`")?;
			let minutes = expect_digits(&mut lexer, 2..=2, 0..=59)?;

			if x == DateTimeToken::Dash {
				FixedOffset::west_opt((hour * 3600 + minutes * 60) as i32)
					.expect("valid timezone offset")
			} else {
				FixedOffset::east_opt((hour * 3600 + minutes * 60) as i32)
					.expect("valid timezone offset")
			}
		}
		Some(Ok(DateTimeToken::Z)) => Utc.fix(),
		_ => {
			return Err(Error {
				span: lexer.span(),
				message: "Invalid datetime token, invalid timezone".to_string(),
			});
		}
	};

	let datetime = NaiveDateTime::new(date, time);
	let Some(datetime) = timezone.from_local_datetime(&datetime).earliest() else {
		let zone_span = timezone_start..lexer.span().end;
		return Err(Error {
			span: zone_span,
			message: "Invalid datetime token, invalid timezone".to_string(),
		});
	};

	Ok(datetime.with_timezone(&Utc))
}

#[cfg(test)]
mod test {
	use chrono::{DateTime, Datelike, Utc};

	use super::datetime;

	fn rfc(s: &str) -> DateTime<Utc> {
		DateTime::parse_from_rfc3339(s).unwrap().to_utc()
	}

	#[test]
	fn full_rfc3339() {
		assert_eq!(datetime("2024-06-15T10:30:00Z").unwrap(), rfc("2024-06-15T10:30:00Z"));
	}

	#[test]
	fn unsigned_year_is_positive() {
		let dt = datetime("2024-06-15T10:30:00Z").unwrap();
		assert_eq!(dt.year(), 2024);
	}

	#[test]
	fn signed_years() {
		assert_eq!(datetime("+2024-06-15T10:30:00Z").unwrap(), rfc("2024-06-15T10:30:00Z"));
		let neg = datetime("-0100-06-15T10:30:00Z").unwrap();
		assert_eq!(neg.year(), -100);
	}

	#[test]
	fn date_only_is_midnight() {
		assert_eq!(datetime("2024-06-15").unwrap(), rfc("2024-06-15T00:00:00Z"));
	}

	#[test]
	fn extended_years() {
		let dt = datetime("99999-01-01T00:00:00Z").unwrap();
		assert_eq!(dt.year(), 99999);
	}

	#[test]
	fn separators_and_case() {
		let expected = rfc("2024-06-15T10:30:00Z");
		assert_eq!(datetime("2024-06-15t10:30:00z").unwrap(), expected);
		assert_eq!(datetime("2024-06-15 10:30:00Z").unwrap(), expected);
	}

	#[test]
	fn fractional_seconds() {
		assert_eq!(datetime("2024-06-15T10:30:00.5Z").unwrap(), rfc("2024-06-15T10:30:00.5Z"));
		// Digits past the ninth are truncated.
		assert_eq!(
			datetime("2024-06-15T10:30:00.1234567899999Z").unwrap(),
			rfc("2024-06-15T10:30:00.123456789Z")
		);
	}

	#[test]
	fn timezone_offsets() {
		assert_eq!(
			datetime("2024-06-15T10:30:00+02:00").unwrap(),
			rfc("2024-06-15T10:30:00+02:00")
		);
		assert_eq!(
			datetime("2024-06-15T10:30:00-05:30").unwrap(),
			rfc("2024-06-15T10:30:00-05:30")
		);
	}

	#[test]
	fn invalid_month() {
		let err = datetime("2024-13-15T10:30:00Z").unwrap_err();
		assert_eq!(
			err.message,
			"Invalid datetime token, digit value out of range, expected value between 1 and 12"
		);
		assert_eq!(err.span, 5..7);
	}

	#[test]
	fn invalid_date() {
		let err = datetime("2024-02-30T10:30:00Z").unwrap_err();
		assert_eq!(err.message, "Invalid datetime token, invalid datetime date");
		assert_eq!(err.span, 0..10);
	}

	#[test]
	fn invalid_digit_count() {
		let err = datetime("2024-6-15").unwrap_err();
		assert_eq!(
			err.message,
			"Invalid datetime token, invalid number of digits, expected 2 digits"
		);
		assert_eq!(err.span, 5..6);
	}

	#[test]
	fn missing_timezone() {
		let err = datetime("2024-06-15T10:30:00").unwrap_err();
		assert_eq!(err.message, "Invalid datetime token, invalid timezone");
	}
}
