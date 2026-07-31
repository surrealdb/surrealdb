use std::time::Duration;

use logos::Logos;

use crate::Error;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
enum DurationToken {
	#[regex("[0-9]+")]
	Digits,
	#[token("y")]
	Year,
	#[token("w")]
	Week,
	#[token("d")]
	Day,
	#[token("h")]
	Hour,
	#[token("m")]
	Minute,
	#[token("s")]
	Second,
	#[token("ms")]
	MiliSecond,
	#[token("us")]
	#[token("µs")]
	MicroSecond,
	#[token("ns")]
	NanoSecond,
}

/// Parse a duration in the SurrealQL duration format.
///
/// A duration is one or more terms, each a positive integer followed by a unit, e.g. `1h30m`.
/// Supported units are `y`, `w`, `d`, `h`, `m`, `s`, `ms`, `us`/`µs`, and `ns`. Terms are
/// summed; the total must fit within the maximum duration of `u64::MAX` seconds and 999 999
/// 999 nanoseconds. The empty string is not a valid duration.
pub fn duration(source: &str) -> Result<Duration, Error> {
	const NANOSECOND_DURATION_MAX: u128 = (u64::MAX as u128) * 1_000_000_000 + 999_999_999;
	const MICRO_SECOND: u128 = 1_000;
	const MILI_SECOND: u128 = 1_000 * MICRO_SECOND;
	const SECOND: u128 = 1_000 * MILI_SECOND;
	const MINUTE: u128 = 60 * SECOND;
	const HOUR: u128 = 60 * MINUTE;
	const DAY: u128 = 24 * HOUR;
	const WEEK: u128 = 7 * DAY;
	const YEAR: u128 = 365 * DAY;

	if source.is_empty() {
		return Err(Error {
			span: 0..0,
			message: "Invalid duration token, expected digits".to_string(),
		});
	}

	let mut lexer = DurationToken::lexer(source);
	let mut duration = 0u128;
	loop {
		let number =
			match lexer.next() {
				None => break,
				Some(Ok(DurationToken::Digits)) => {
					let Some(x) = lexer.slice().parse::<u128>().ok().and_then(|x| {
						if x > NANOSECOND_DURATION_MAX {
							None
						} else {
							Some(x)
						}
					}) else {
						return Err(Error {
						span: 0..source.len(),
						message: "Duration value overflowed, value larger then maximum supported value".to_string(),
					});
					};
					x
				}
				_ => {
					return Err(Error {
						span: lexer.span(),
						message: "Invalid duration token, unexpected character, expected digits"
							.to_string(),
					});
				}
			};

		let sub_duration = match lexer.next() {
			Some(Ok(DurationToken::Year)) => number.checked_mul(YEAR),
			Some(Ok(DurationToken::Week)) => number.checked_mul(WEEK),
			Some(Ok(DurationToken::Day)) => number.checked_mul(DAY),
			Some(Ok(DurationToken::Hour)) => number.checked_mul(HOUR),
			Some(Ok(DurationToken::Minute)) => number.checked_mul(MINUTE),
			Some(Ok(DurationToken::Second)) => number.checked_mul(SECOND),
			Some(Ok(DurationToken::MiliSecond)) => number.checked_mul(MILI_SECOND),
			Some(Ok(DurationToken::MicroSecond)) => number.checked_mul(MICRO_SECOND),
			Some(Ok(DurationToken::NanoSecond)) => Some(number),
			None => {
				return Err(Error {
					span: lexer.span(),
					message:
						"Invalid duration token, unexpected end of input, expected a duration unit"
							.to_string(),
				});
			}
			_ => {
				return Err(Error {
					span: lexer.span(),
					message:
						"Invalid duration token, unexpected character, expected a duration unit"
							.to_string(),
				});
			}
		};

		let Some(x) = sub_duration.and_then(|x| duration.checked_add(x)).and_then(|x| {
			if x > NANOSECOND_DURATION_MAX {
				None
			} else {
				Some(x)
			}
		}) else {
			return Err(Error {
				span: 0..source.len(),
				message: "Duration value overflowed, value larger then maximum supported value"
					.to_string(),
			});
		};
		duration = x;
	}

	let nanos = (duration % 1_000_000_000) as u32;
	let secs = (duration / 1_000_000_000) as u64;

	Ok(Duration::new(secs, nanos))
}

#[cfg(test)]
mod test {
	use std::time::Duration;

	use super::duration;

	#[test]
	fn single_units() {
		assert_eq!(duration("1ns").unwrap(), Duration::from_nanos(1));
		assert_eq!(duration("1us").unwrap(), Duration::from_micros(1));
		assert_eq!(duration("1µs").unwrap(), Duration::from_micros(1));
		assert_eq!(duration("1ms").unwrap(), Duration::from_millis(1));
		assert_eq!(duration("1s").unwrap(), Duration::from_secs(1));
		assert_eq!(duration("1m").unwrap(), Duration::from_secs(60));
		assert_eq!(duration("1h").unwrap(), Duration::from_secs(3600));
		assert_eq!(duration("1d").unwrap(), Duration::from_secs(24 * 3600));
		assert_eq!(duration("1w").unwrap(), Duration::from_secs(7 * 24 * 3600));
		assert_eq!(duration("1y").unwrap(), Duration::from_secs(365 * 24 * 3600));
	}

	#[test]
	fn multi_term() {
		assert_eq!(duration("1h30m").unwrap(), Duration::from_secs(5400));
		assert_eq!(duration("1s500ms1ns").unwrap(), Duration::new(1, 500_000_001));
	}

	#[test]
	fn max_boundary() {
		// The maximum supported duration.
		assert_eq!(
			duration(&format!("{}s999999999ns", u64::MAX)).unwrap(),
			Duration::new(u64::MAX, 999_999_999)
		);
		// One nanosecond more overflows.
		let err = duration(&format!("{}s1000000000ns", u64::MAX)).unwrap_err();
		assert_eq!(
			err.message,
			"Duration value overflowed, value larger then maximum supported value"
		);
	}

	#[test]
	fn per_number_overflow() {
		let source = format!("{}y", u128::MAX);
		let err = duration(&source).unwrap_err();
		assert_eq!(
			err.message,
			"Duration value overflowed, value larger then maximum supported value"
		);
		assert_eq!(err.span, 0..source.len());
	}

	#[test]
	fn empty_input() {
		let err = duration("").unwrap_err();
		assert_eq!(err.message, "Invalid duration token, expected digits");
		assert_eq!(err.span, 0..0);
	}

	#[test]
	fn missing_unit() {
		let err = duration("1h2").unwrap_err();
		assert_eq!(
			err.message,
			"Invalid duration token, unexpected end of input, expected a duration unit"
		);
		assert_eq!(err.span, 3..3);
	}

	#[test]
	fn invalid_character() {
		let err = duration("x1").unwrap_err();
		assert_eq!(err.message, "Invalid duration token, unexpected character, expected digits");
		assert_eq!(err.span.start, 0);

		let err = duration("1x").unwrap_err();
		assert_eq!(
			err.message,
			"Invalid duration token, unexpected character, expected a duration unit"
		);
		assert_eq!(err.span.start, 1);
	}
}
