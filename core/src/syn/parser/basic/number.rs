use rust_decimal::Decimal;
use std::{
	borrow::Cow,
	num::{ParseFloatError, ParseIntError},
	str::FromStr,
};

use crate::{
	sql::Number,
	syn::{
		error::error,
		parser::{mac::unexpected, ParseResult, Parser},
		token::{t, NumberKind, TokenKind},
	},
};

use super::TokenValue;

fn prepare_number_str(str: &str) -> Cow<str> {
	if str.contains('_') {
		Cow::Owned(str.chars().filter(|x| *x != '_').collect())
	} else {
		Cow::Borrowed(str)
	}
}

/// Generic integer parsing method,
/// works for all unsigned integers.
fn parse_integer<I>(parser: &mut Parser<'_>) -> ParseResult<I>
where
	I: FromStr<Err = ParseIntError>,
{
	let mut peek = parser.peek();

	if let t!("-") = peek.kind {
		unexpected!(parser,peek,"an integer", => "only positive integers are allowed here")
	}

	if let t!("+") = peek.kind {
		peek = parser.peek_whitespace();
	}

	match peek.kind {
		TokenKind::Digits => {
			parser.pop_peek();
			assert!(!parser.has_peek());

			let p = parser.peek_whitespace();
			match p.kind {
				t!(".") => {
					unexpected!(parser, p, "an integer")
				}
				t!("dec") => {
					unexpected!(parser, p, "an integer", => "decimal numbers not supported here")
				}
				x if Parser::tokenkind_continues_ident(x) => {
					unexpected!(parser, p, "an integer")
				}
				_ => {}
			}

			// remove the possible "f" number suffix and any '_' characters
			let res = prepare_number_str(parser.lexer.span_str(peek.span))
				.parse()
				.map_err(|e| error!("Failed to parse integer: {e}", @peek.span))?;
			Ok(res)
		}
		_ => unexpected!(parser, peek, "an integer"),
	}
}

impl TokenValue for u64 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_integer(parser)
	}
}

impl TokenValue for u32 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_integer(parser)
	}
}

impl TokenValue for u16 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_integer(parser)
	}
}

impl TokenValue for u8 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_integer(parser)
	}
}

/// Generic float parsing method,
/// works for both f32 and f64
fn parse_float<F>(parser: &mut Parser<'_>) -> ParseResult<F>
where
	F: FromStr<Err = ParseFloatError>,
{
	let peek = parser.peek();
	// find initial  digits
	match peek.kind {
		TokenKind::NaN => return Ok("NaN".parse().unwrap()),
		TokenKind::Digits | t!("+") | t!("-") => {}
		_ => unexpected!(parser, peek, "a floating point number"),
	};
	let float_token = parser.glue_float()?;
	match float_token.kind {
		TokenKind::Number(NumberKind::Float) => {
			parser.pop_peek();
		}
		_ => unexpected!(parser, float_token, "a floating point number"),
	};

	let span = parser.lexer.span_str(float_token.span);

	// remove the possible "f" number suffix and any '_' characters
	prepare_number_str(span.strip_suffix('f').unwrap_or(span))
		.parse()
		.map_err(|e| error!("Failed to parser floating point number: {e}", @float_token.span))
}

impl TokenValue for f32 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_float(parser)
	}
}

impl TokenValue for f64 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_float(parser)
	}
}

impl TokenValue for Number {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let number = parser.glue_number()?;
		let number_kind = match number.kind {
			TokenKind::NaN => {
				parser.pop_peek();
				return Ok(Number::Float(f64::NAN));
			}
			TokenKind::Number(x) => x,
			_ => unexpected!(parser, number, "a number"),
		};

		parser.pop_peek();
		let span = parser.lexer.span_str(number.span);

		match number_kind {
			NumberKind::Decimal => {
				let str = prepare_number_str(span.strip_suffix("dec").unwrap_or(span));
				let decimal = if str.contains('e') {
					Decimal::from_scientific(str.as_ref())
						.map_err(|e| error!("Failed to parser decimal: {e}", @number.span))?
				} else {
					Decimal::from_str(str.as_ref())
						.map_err(|e| error!("Failed to parser decimal: {e}", @number.span))?
				};

				Ok(Number::Decimal(decimal))
			}
			NumberKind::Float => {
				let float =
					prepare_number_str(span.strip_suffix('f').unwrap_or(span)).parse().map_err(
						|e| error!("Failed to parser floating point number: {e}", @number.span),
					)?;

				Ok(Number::Float(float))
			}
			NumberKind::Integer => {
				let integer = prepare_number_str(span.strip_suffix('f').unwrap_or(span))
					.parse()
					.map_err(|e| error!("Failed to parse integer: {e}", @number.span))?;

				Ok(Number::Int(integer))
			}
		}
	}
}
