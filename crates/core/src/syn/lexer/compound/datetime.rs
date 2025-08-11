use std::ops::RangeInclusive;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};

use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, t};

pub fn datetime(lexer: &mut Lexer, start: Token) -> Result<DateTime<Utc>, SyntaxError> {
	let double = match start.kind {
		t!("d\"") => true,
		t!("d'") => false,
		x => panic!("Invalid start token of datetime compound: {x}"),
	};
	let datetime = datetime_inner(lexer)?;
	if double {
		lexer.expect('"')?;
	} else {
		lexer.expect('\'')?;
	}
	Ok(datetime)
}

/// Lexes a datetime without the surrounding `'` or `"`
pub fn datetime_inner(lexer: &mut Lexer) -> Result<DateTime<Utc>, SyntaxError> {
	let date_start = lexer.reader.offset();

	let year_neg = lexer.eat(b'-');
	if !year_neg {
		lexer.eat(b'+');
	}

	let year = parse_datetime_digits(lexer, 4, 0..=9999)?;
	lexer.expect('-')?;
	let month = parse_datetime_digits(lexer, 2, 1..=12)?;
	lexer.expect('-')?;
	let day = parse_datetime_digits(lexer, 2, 1..=31)?;

	let year = if year_neg {
		-(year as i32)
	} else {
		year as i32
	};

	let date = NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(
		|| syntax_error!("Invalid DateTime date: date outside of valid range", @lexer.span_since(date_start)),
	)?;

	if !lexer.eat_when(|x| x == b'T') {
		let time = NaiveTime::default();
		let date_time = NaiveDateTime::new(date, time);

		let datetime =
			Utc.fix().from_local_datetime(&date_time).earliest().unwrap().with_timezone(&Utc);

		return Ok(datetime);
	}

	let time_start = lexer.reader.offset();

	let hour = parse_datetime_digits(lexer, 2, 0..=24)?;
	lexer.expect(':')?;
	let minute = parse_datetime_digits(lexer, 2, 0..=59)?;
	lexer.expect(':')?;
	let second = parse_datetime_digits(lexer, 2, 0..=60)?;

	let nanos_start = lexer.reader.offset();
	let nanos = if lexer.eat(b'.') {
		let mut number = 0u32;
		let mut count = 0;

		loop {
			let Some(d) = lexer.reader.peek() else {
				break;
			};
			if !d.is_ascii_digit() {
				break;
			}

			if count == 9 {
				bail!("Invalid datetime nanoseconds, expected no more then 9 digits", @lexer.span_since(nanos_start))
			}

			lexer.reader.next();
			number *= 10;
			number += (d - b'0') as u32;
			count += 1;
		}

		if count == 0 {
			bail!("Invalid datetime nanoseconds, expected at least a single digit", @lexer.span_since(nanos_start))
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
			|| syntax_error!("Invalid DateTime time: time outside of valid range", @lexer.span_since(time_start)),
		)?;

	let timezone_start = lexer.reader.offset();
	let timezone = match lexer.reader.peek() {
		Some(b'-') => {
			lexer.reader.next();
			let (hour, minute) = parse_timezone(lexer)?;
			// The range checks on the digits ensure that the offset can't exceed 23:59 so
			// below unwraps won't panic.
			FixedOffset::west_opt((hour * 3600 + minute * 60) as i32).unwrap()
		}
		Some(b'+') => {
			lexer.reader.next();
			let (hour, minute) = parse_timezone(lexer)?;

			// The range checks on the digits ensure that the offset can't exceed 23:59 so
			// below unwraps won't panic.
			FixedOffset::east_opt((hour * 3600 + minute * 60) as i32).unwrap()
		}
		Some(b'Z') => {
			lexer.reader.next();
			Utc.fix()
		}
		Some(x) => {
			let char = lexer.reader.convert_to_char(x)?;
			bail!("Invalid datetime timezone, expected `Z` or a timezone offset, found {char}",@lexer.span_since(timezone_start));
		}
		None => {
			bail!("Invalid end of file, expected datetime to finish",@lexer.span_since(time_start));
		}
	};

	let date_time = NaiveDateTime::new(date, time);

	let datetime = timezone
		.from_local_datetime(&date_time)
		.earliest()
		// this should never panic with a fixed offset.
		.unwrap()
		.with_timezone(&Utc);

	Ok(datetime)
}

fn parse_timezone(lexer: &mut Lexer) -> Result<(u32, u32), SyntaxError> {
	let hour = parse_datetime_digits(lexer, 2, 0..=23)? as u32;
	lexer.expect(':')?;
	let minute = parse_datetime_digits(lexer, 2, 0..=59)? as u32;

	Ok((hour, minute))
}

fn parse_datetime_digits(
	lexer: &mut Lexer,
	count: usize,
	range: RangeInclusive<usize>,
) -> Result<usize, SyntaxError> {
	let start = lexer.reader.offset();

	let mut value = 0usize;

	for _ in 0..count {
		let offset = lexer.reader.offset();
		match lexer.reader.next() {
			Some(x) if x.is_ascii_digit() => {
				value *= 10;
				value += (x - b'0') as usize;
			}
			Some(x) => {
				let char = lexer.reader.convert_to_char(x)?;
				let span = lexer.span_since(offset);
				bail!("Invalid datetime, expected digit character found `{char}`", @span);
			}
			None => {
				bail!("Expected end of file, expected datetime digit character", @lexer.current_span());
			}
		}
	}

	if !range.contains(&value) {
		let span = lexer.span_since(start);
		bail!("Invalid datetime digit section, section not within allowed range",
			@span => "This section must be within {}..={}",range.start(),range.end());
	}

	Ok(value)
}
