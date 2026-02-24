use std::ops::{Range, RangeInclusive};

use ast::DateTime;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};
use common::source_error::{AnnotationKind, Level, Snippet};
use common::span::Span;
use logos::{Lexer, Logos};
use token::{BaseTokenKind, DateTimeToken, UuidToken, VersionToken};
use uuid::Uuid;

use crate::parse::{ParseError, ParseResult};
use crate::{ParseSync, Parser};

fn unexpected_error(
	full_source: &str,
	unescape_source: &str,
	unescape_source_offset: u32,
	span: Range<usize>,
	message: String,
) -> ParseError {
	let start =
		Parser::escape_str_offset(unescape_source, span.start as u32) + unescape_source_offset;
	let end = Parser::escape_str_offset(unescape_source, span.end as u32) + unescape_source_offset;
	let span = Span::from_range(start..end);
	ParseError::diagnostic(
		Level::Error
			.title(message)
			.snippet(Snippet::source(full_source).annotate(AnnotationKind::Primary.span(span)))
			.to_diagnostic()
			.to_owned(),
	)
}

impl ParseSync for ast::Version {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		fn eat_version(partial: bool, lexer: &mut Lexer<'_, VersionToken>) -> ParseResult<u64> {
			match lexer.next() {
				Some(Ok(VersionToken::Digits)) => {}
				None => {
					if partial {
						return Err(ParseError::missing_data_error());
					}
				}
				_ => {
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected version token, expected version digits")
							.snippet(
								Snippet::source(lexer.source())
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}

			lexer.slice().parse().map_err(|_| {
				let span = Span::from_usize_range(lexer.span())
					.expect("source should not be larger the u32::MAX");
				ParseError::diagnostic(
					Level::Error
						.title("Invalid version token, version number exceeds maximum value")
						.snippet(
							Snippet::source(lexer.source())
								.annotate(AnnotationKind::Primary.span(span)),
						)
						.to_diagnostic()
						.to_owned(),
				)
			})
		}

		let partial = parser.partial();
		parser.lex(|lexer| {
			let mut lexer = lexer.morph::<VersionToken>();

			let start = lexer.span().end;

			let major = eat_version(partial, &mut lexer)?;
			match lexer.next() {
				Some(Ok(VersionToken::Dot)) => {}
				x => {
					if x.is_none() && partial {
						return Err(ParseError::missing_data_error());
					}
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected version token, expected `.`")
							.snippet(
								Snippet::source(lexer.source())
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}
			let minor = eat_version(partial, &mut lexer)?;
			match lexer.next() {
				Some(Ok(VersionToken::Dot)) => {}
				x => {
					if x.is_none() && partial {
						return Err(ParseError::missing_data_error());
					}
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected version token, expected `.`")
							.snippet(
								Snippet::source(lexer.source())
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}
			let patch = eat_version(partial, &mut lexer)?;

			let end = lexer.span().end;
			let span = Span::from_usize_range(start..end)
				.expect("source should not be larger the u32::MAX");
			Ok((
				lexer.morph(),
				ast::Version {
					major,
					minor,
					patch,
					span,
				},
			))
		})
	}
}

impl ParseSync for Uuid {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::UuidString)?;
		let mut span = token.span;
		span.start += 2;
		span.end -= 1;
		let full_source = parser.source();
		let unescape_source = parser.slice(span);
		let str = Parser::unescape_common(
			span,
			unescape_source,
			parser.source(),
			&mut parser.unescape_buffer,
		)?;

		fn parse_hex_byte(byte: u8) -> u8 {
			match byte {
				b'0'..=b'9' => byte - b'0',
				b'a'..=b'f' => byte - b'a' + 10,
				b'A'..=b'F' => byte - b'A' + 10,
				_ => unreachable!(),
			}
		}

		fn eat_digits(
			full_source: &str,
			unescape_source: &str,
			unescape_source_offset: u32,
			lexer: &mut Lexer<UuidToken>,
			bytes: &mut [u8],
		) -> ParseResult<()> {
			match lexer.next() {
				Some(Ok(UuidToken::Digits)) => {
					let span = lexer.span();
					let span_len = span.len();
					if span_len != bytes.len() * 2 {
						return Err(unexpected_error(
							full_source,
							unescape_source,
							unescape_source_offset,
							span,
							format!(
								"Invalid uuid token, invalid number of hex digits, expected {}, found {}",
								bytes.len() * 2,
								span_len
							),
						));
					}

					let slice = lexer.slice().as_bytes();
					for (idx, b) in bytes.iter_mut().enumerate() {
						let u = parse_hex_byte(slice[idx * 2]);
						let l = parse_hex_byte(slice[idx * 2 + 1]);

						*b = (u << 4) | l
					}
				}
				_ => {
					let span = lexer.span();

					return Err(unexpected_error(
						full_source,
						unescape_source,
						unescape_source_offset,
						span,
						"Invalid uuid token, unexpected character, expected hex digit".to_owned(),
					));
				}
			}
			Ok(())
		}

		fn eat_dash(
			full_source: &str,
			unescape_source: &str,
			unescape_source_offset: u32,
			lexer: &mut Lexer<UuidToken>,
		) -> ParseResult<()> {
			match lexer.next() {
				Some(Ok(UuidToken::Dash)) => {}
				_ => {
					let span = lexer.span();
					return Err(unexpected_error(
						full_source,
						unescape_source,
						unescape_source_offset,
						span,
						"Invalid uuid token, unexpected character, expected `-`".to_owned(),
					));
				}
			}
			Ok(())
		}

		let mut lexer = UuidToken::lexer(str);

		let mut buffer = [0u8; 16];

		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[0..4])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[4..6])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[6..8])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[8..10])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[10..16])?;

		Ok(Uuid::from_bytes(buffer))
	}
}

impl ParseSync for DateTime {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
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

		let token = parser.expect(BaseTokenKind::DateTimeString)?;
		let mut span = token.span;
		span.start += 2;
		span.end -= 1;
		let full_source = parser.source();
		let unescape_source = parser.slice(span);
		let str = Parser::unescape_common(
			span,
			unescape_source,
			parser.source(),
			&mut parser.unescape_buffer,
		)?;

		let mut lexer = DateTimeToken::lexer(str);

		fn expect_token(
			full_source: &str,
			unescape_source: &str,
			unescape_source_offset: u32,
			lexer: &mut Lexer<DateTimeToken>,
			expected: DateTimeToken,
			expected_description: &str,
		) -> ParseResult<()> {
			if let Some(Ok(x)) = lexer.next() {
				if x == expected {
					return Ok(());
				}
			}

			let span = lexer.span();
			Err(unexpected_error(
				full_source,
				unescape_source,
				unescape_source_offset,
				span,
				format!(
					"Invalid datetime token, unexpected character, expected {expected_description}"
				),
			))
		}

		fn expect_digits(
			full_source: &str,
			unescape_source: &str,
			unescape_source_offset: u32,
			lexer: &mut Lexer<DateTimeToken>,
			count: RangeInclusive<usize>,
			range: RangeInclusive<u32>,
		) -> ParseResult<u32> {
			expect_token(
				full_source,
				unescape_source,
				unescape_source_offset,
				lexer,
				DateTimeToken::Digits,
				"digits",
			)?;

			let digits = lexer.slice();
			if !count.contains(&digits.len()) {
				let span = lexer.span();
				if count.start() != count.end() {
					return Err(unexpected_error(
						full_source,
						unescape_source,
						unescape_source_offset,
						span,
						format!(
							"Invalid datetime token, invalid number of digits, expected between {} and {} digits",
							count.start(),
							count.end()
						),
					));
				} else {
					return Err(unexpected_error(
						full_source,
						unescape_source,
						unescape_source_offset,
						span,
						format!(
							"Invalid datetime token, invalid number of digits, expected {} digits",
							count.start()
						),
					));
				}
			}

			let value = digits.parse().expect("caller to enforce integer limits via count limit");

			if !range.contains(&value) {
				let span = lexer.span();
				return Err(unexpected_error(
					full_source,
					unescape_source,
					unescape_source_offset,
					span,
					format!(
						"Invalid datetime token, digit value out of range, expected value between {} and {}",
						count.start(),
						count.end()
					),
				));
			}

			Ok(value)
		}

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
			true
		};

		// parse date section.
		let year = expect_digits(
			full_source,
			unescape_source,
			span.start,
			&mut lexer,
			4..=6,
			0..=u32::MAX,
		)?;
		expect_token(
			full_source,
			unescape_source,
			span.start,
			&mut lexer,
			DateTimeToken::Dash,
			"`-`",
		)?;
		let month =
			expect_digits(full_source, unescape_source, span.start, &mut lexer, 2..=2, 1..=12)?;
		expect_token(
			full_source,
			unescape_source,
			span.start,
			&mut lexer,
			DateTimeToken::Dash,
			"`-`",
		)?;
		let day =
			expect_digits(full_source, unescape_source, span.start, &mut lexer, 2..=2, 1..=31)?;

		let year = if neg {
			-(year as i32)
		} else {
			year as i32
		};

		let Some(date) = NaiveDate::from_ymd_opt(year, month as u32, day as u32) else {
			let date_span = 0..lexer.span().end;
			return Err(unexpected_error(
				full_source,
				unescape_source,
				span.start,
				date_span,
				"Invalid datetime token, invalid datetime date".to_owned(),
			));
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
				return Err(unexpected_error(
					full_source,
					unescape_source,
					span.start,
					lexer.span(),
					"Invalid datetime token, invalid datetime date".to_owned(),
				));
			}
		}

		// Start of the time section of the datetime.
		let time_start = lexer.span().end;
		let hour =
			expect_digits(full_source, unescape_source, span.start, &mut lexer, 2..=2, 0..=24)?;
		expect_token(
			full_source,
			unescape_source,
			span.start,
			&mut lexer,
			DateTimeToken::Colon,
			"`:`",
		)?;
		let minute =
			expect_digits(full_source, unescape_source, span.start, &mut lexer, 2..=2, 0..=59)?;
		expect_token(
			full_source,
			unescape_source,
			span.start,
			&mut lexer,
			DateTimeToken::Colon,
			"`:`",
		)?;
		let second =
			expect_digits(full_source, unescape_source, span.start, &mut lexer, 2..=2, 0..=60)?;

		let mut peeked_lexer = lexer.clone();
		let peek = peeked_lexer.next();
		// parsing possible nanoseconds.
		let nanos = if let Some(Ok(DateTimeToken::Dot)) = peek {
			lexer = peeked_lexer;

			expect_token(
				full_source,
				unescape_source,
				span.start,
				&mut lexer,
				DateTimeToken::Digits,
				"nanoseconds digits",
			)?;

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

		let Some(time) =
			NaiveTime::from_hms_nano_opt(hour as u32, minute as u32, second as u32, nanos)
		else {
			let time_span = time_start..lexer.span().end;
			return Err(unexpected_error(
				full_source,
				unescape_source,
				span.start,
				time_span,
				"Invalid datetime token, invalid datetime time".to_owned(),
			));
		};

		let timezone_start = lexer.span().end;
		let timezone = match lexer.next() {
			Some(Ok(x @ (DateTimeToken::Plus | DateTimeToken::Dash))) => {
				let hour = expect_digits(
					full_source,
					unescape_source,
					span.start,
					&mut lexer,
					2..=2,
					0..=23,
				)?;
				expect_token(
					full_source,
					unescape_source,
					span.start,
					&mut lexer,
					DateTimeToken::Colon,
					"`:`",
				)?;
				let minutes = expect_digits(
					full_source,
					unescape_source,
					span.start,
					&mut lexer,
					2..=2,
					0..=59,
				)?;

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
				return Err(unexpected_error(
					full_source,
					unescape_source,
					span.start,
					lexer.span(),
					"Invalid datetime token, invalid timezone".to_owned(),
				));
			}
		};

		let datetime = NaiveDateTime::new(date, time);
		let Some(datetime) = timezone.from_local_datetime(&datetime).earliest() else {
			let zone_span = timezone_start..lexer.span().end;
			return Err(unexpected_error(
				full_source,
				unescape_source,
				span.start,
				zone_span,
				"Invalid datetime token, invalid timezone".to_owned(),
			));
		};

		Ok(datetime.with_timezone(&Utc))
	}
}
