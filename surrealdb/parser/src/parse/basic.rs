use std::str::FromStr;
use std::time::Duration;

use ast::{NodeId, PathSegment};
use common::source_error::{AnnotationKind, Level};
use logos::Logos;
use rust_decimal::Decimal;
use token::{BaseTokenKind, DurationToken, T};

use super::{ParseResult, ParseSync, Parser};

impl ParseSync for ast::Ident {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.peek_expect("an identifier")?;
		if !token.token.is_identifier() {
			return Err(parser.unexpected("an identifier"));
		}
		let _ = parser.next();
		let text = parser.unescape_ident(token)?;

		Ok(ast::Ident {
			text,
			span: token.span,
		})
	}
}

impl ParseSync for ast::Param {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Param)?;
		let text = parser.unescape_param(token)?;

		Ok(ast::Param {
			text,
			span: token.span,
		})
	}
}

pub fn ununderscore_slice<'a>(slice: &'a str, buffer: &'a mut String) -> &'a str {
	let Some((a, mut rest)) = slice.split_once('_') else {
		return slice;
	};
	buffer.clear();
	buffer.push_str(a);
	while let Some((head, tail)) = rest.split_once('_') {
		buffer.push_str(head);
		rest = tail
	}
	buffer
}

impl ParseSync for f64 {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let sign = if parser.eat(T![+])?.is_some() {
			ast::Sign::Plus
		} else if parser.eat(T![-])?.is_some() {
			ast::Sign::Minus
		} else {
			ast::Sign::Plus
		};

		let token = parser.expect(BaseTokenKind::Float)?;
		let slice = parser.slice(token.span);
		let slice = ununderscore_slice(slice, &mut parser.unescape_buffer);
		let float: f64 =
			slice.trim_end_matches("f").parse().expect("lexer should ensure valid floats");
		if let ast::Sign::Minus = sign {
			Ok(-float)
		} else {
			Ok(float)
		}
	}
}

impl ParseSync for Decimal {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Decimal)?;
		let slice =
			parser.slice(token.span).strip_suffix("dec").expect("decimal tokens should end in dec");
		let slice = ununderscore_slice(slice, &mut parser.unescape_buffer);
		let decimal = if slice.contains(['e', 'E']) {
			Decimal::from_scientific(slice).expect("lexer should ensure valid decimals").normalize()
		} else {
			Decimal::from_str(slice).expect("lexer should ensure valid decimals").normalize()
		};
		Ok(decimal)
	}
}

impl ParseSync for ast::Path {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let span = parser.peek_span();
		let start = parser.parse_sync::<NodeId<ast::Ident>>()?;

		// Special path for ml::*<version> paths which don't have the ::< and are therefore
		// generally ambiguous,
		if start.index(parser).text.index(parser) == "ml" {
			let ml = parser.speculate_sync(|parser| {
				let _ = parser.expect(T![::])?;
				let name = parser.parse_sync()?;
				let open = parser.expect(T![<])?;
				parser.commit_sync(|parser| {
					let version = parser.parse_sync()?;
					let _ = parser.expect_closing_delimiter(T![>], open.span)?;
					Ok((name, version))
				})
			})?;
			if let Some((name, version)) = ml {
				let mut cur = None;
				let mut parts = None;
				let name = parser.push(name);
				parser.push_list(ast::PathSegment::Ident(name), &mut parts, &mut cur);
				parser.push_list(ast::PathSegment::Version(version), &mut parts, &mut cur);

				return Ok(ast::Path {
					start,
					parts,
					span: parser.span_since(span),
				});
			}
		}

		let mut cur = None;
		let mut parts = None;
		while let Some(token) = parser.peek()?
			&& let T![::] = token.token
		{
			let _ = parser.next();

			let peek = parser.peek_expect("a version or a identifier")?;
			let v = match peek.token {
				T![<] => {
					let _ = parser.next();
					let v = parser.parse_sync()?;
					let _ = parser.expect(T![>])?;
					PathSegment::Version(v)
				}
				x if x.is_identifier() => {
					let ident = parser.parse_sync()?;
					PathSegment::Ident(ident)
				}
				_ => return Err(parser.unexpected("a version or a identifier")),
			};
			parser.push_list(v, &mut parts, &mut cur);
		}

		Ok(ast::Path {
			start,
			parts,
			span: parser.span_since(span),
		})
	}
}

impl ParseSync for ast::Integer {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		fn parse_int_value(slice: &[u8]) -> Option<u64> {
			let mut res: u64 = 0;
			for b in slice.iter().copied() {
				if b == b'_' {
					continue;
				}
				// Lexer guarentees that no other characters then `[0-9_]` are present in the
				// slice.
				let v = (b - b'0') as u64;
				res = res.checked_mul(10u64)?.checked_add(v)?;
			}
			Some(res)
		}

		let sign = if parser.eat(T![+])?.is_some() {
			ast::Sign::Plus
		} else if parser.eat(T![-])?.is_some() {
			ast::Sign::Minus
		} else {
			ast::Sign::Plus
		};

		let token = parser.expect(BaseTokenKind::Int)?;
		let slice = parser.slice(token.span);
		let Some(x) = parse_int_value(slice.as_bytes()) else {
			return Err(parser.with_error(|parser| {
				Level::Error
					.title("Integer too large to fit in target type")
					.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(token.span)))
					.to_diagnostic()
			}));
		};

		Ok(ast::Integer {
			sign,
			value: x,
			span: token.span,
		})
	}
}

impl ParseSync for ast::StringLit {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::String)?;
		let slice = parser.unescape_str_push(token)?;
		Ok(ast::StringLit {
			text: slice,
			span: token.span,
		})
	}
}

impl ParseSync for ast::FileLit {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::FileString)?;
		let slice = parser.unescape_str_push(token)?;
		Ok(ast::FileLit {
			path: slice,
			span: token.span,
		})
	}
}

impl ParseSync for Duration {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		const NANOSECOND_DURATION_MAX: u128 = (u64::MAX as u128) * 1_000_000_000 + 999_999_999;
		const MICRO_SECOND: u128 = 1_000;
		const MILI_SECOND: u128 = 1_000 * MICRO_SECOND;
		const SECOND: u128 = 1_000 * MILI_SECOND;
		const MINUTE: u128 = 60 * SECOND;
		const HOUR: u128 = 60 * MINUTE;
		const DAY: u128 = 24 * HOUR;
		const WEEK: u128 = 7 * DAY;
		const YEAR: u128 = 365 * DAY;

		let token = parser.expect(BaseTokenKind::Duration)?;
		let slice = parser.slice(token.span);

		let mut lexer = DurationToken::lexer(slice);
		let mut duration = 0u128;
		loop {
			let number = match lexer.next() {
				None => break,
				Some(Ok(DurationToken::Digits)) => {
					let Some(x) = lexer.slice().parse::<u128>().ok().and_then(|x| {
						if x > NANOSECOND_DURATION_MAX {
							None
						} else {
							Some(x)
						}
					}) else {
						return Err(parser.with_error(|parser| {
							Level::Error
								.title(
									"Duration value overflowed, value larger then maximum supported value",
								)
								.snippet(
									parser
										.snippet()
										.annotate(AnnotationKind::Primary.span(token.span)),
								)
								.to_diagnostic()
						}));
					};
					x
				}
				// Previously already enforced by the base token lexer
				_ => unreachable!(),
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
				// Previously already enforced by the base token lexer
				_ => unreachable!(),
			};

			let Some(x) = sub_duration.and_then(|x| duration.checked_add(x)).and_then(|x| {
				if x > NANOSECOND_DURATION_MAX {
					None
				} else {
					Some(x)
				}
			}) else {
				return Err(parser.with_error(|parser| {
					Level::Error
						.title(
							"Duration value overflowed, value larger then maximum supported value",
						)
						.snippet(
							parser.snippet().annotate(AnnotationKind::Primary.span(token.span)),
						)
						.to_diagnostic()
				}));
			};
			duration = x;
		}

		let nanos = (duration % 1_000_000_000) as u32;
		let secs = (duration / 1_000_000_000) as u64;

		Ok(Duration::new(secs, nanos))
	}
}
