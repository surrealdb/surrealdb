use std::mem;
use std::num::ParseIntError;
use std::str::FromStr;

use rust_decimal::Decimal;

use super::TokenValue;
use crate::syn::error::{bail, syntax_error};
use crate::syn::lexer::compound::{self, NumberKind, Numeric};
use crate::syn::parser::mac::unexpected;
use crate::syn::parser::{GluedValue, ParseResult, Parser};
use crate::syn::token::{self, TokenKind, t};
use crate::val::DecimalExt as _;

/// Generic integer parsing method,
/// works for all unsigned integers.
fn parse_integer<I>(parser: &mut Parser<'_>) -> ParseResult<I>
where
	I: FromStr<Err = ParseIntError>,
{
	let token = parser.peek();
	match token.kind {
		t!("+") | TokenKind::Digits => {
			parser.pop_peek();
			Ok(parser.lexer.lex_compound(token, compound::integer)?.value)
		}
		t!("-") => {
			bail!("Unexpected token `-`", @token.span => "Only positive integers allowed here")
		}
		_ => unexpected!(parser, token, "an unsigned integer"),
	}
}

fn parse_signed_integer<I>(parser: &mut Parser<'_>) -> ParseResult<I>
where
	I: FromStr<Err = ParseIntError>,
{
	let token = parser.peek();
	match token.kind {
		t!("+") | t!("-") | TokenKind::Digits => {
			parser.pop_peek();
			Ok(parser.lexer.lex_compound(token, compound::integer)?.value)
		}
		_ => unexpected!(parser, token, "an signed integer"),
	}
}

impl TokenValue for u64 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_integer(parser)
	}
}

impl TokenValue for i64 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parse_signed_integer(parser)
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

impl TokenValue for f32 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			t!("+") | t!("-") | TokenKind::Digits => {
				parser.pop_peek();
				Ok(parser.lexer.lex_compound(token, compound::float)?.value)
			}
			_ => unexpected!(parser, token, "a floating point number"),
		}
	}
}

impl TokenValue for f64 {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			t!("+") | t!("-") | TokenKind::Digits => {
				parser.pop_peek();
				Ok(parser.lexer.lex_compound(token, compound::float)?.value)
			}
			_ => unexpected!(parser, token, "a floating point number"),
		}
	}
}

impl TokenValue for Numeric {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Number) => {
				parser.pop_peek();
				let GluedValue::Number(x) = mem::take(&mut parser.glued_value) else {
					panic!("Glued token was next but glued value was not of the correct value");
				};
				let number_str = parser.lexer.span_str(token.span);
				match x {
					NumberKind::Integer => number_str
						.parse()
						.map(Numeric::Integer)
						.map_err(|e| syntax_error!("Failed to parse number: {e}", @token.span)),
					NumberKind::Float => number_str
						.trim_end_matches("f")
						.parse()
						.map(Numeric::Float)
						.map_err(|e| syntax_error!("Failed to parse number: {e}", @token.span)),
					NumberKind::Decimal => {
						let number_str = number_str.trim_end_matches("dec");
						let decimal = if number_str.contains(['e', 'E']) {
							Decimal::from_scientific(number_str).map_err(
								|e| syntax_error!("Failed to parser decimal: {e}", @token.span),
							)?
						} else {
							Decimal::from_str_normalized(number_str).map_err(
								|e| syntax_error!("Failed to parser decimal: {e}", @token.span),
							)?
						};
						Ok(Numeric::Decimal(decimal))
					}
				}
			}
			t!("+") | t!("-") | TokenKind::Digits => {
				parser.pop_peek();
				Ok((parser.lexer.lex_compound(token, compound::number))?.value)
			}
			_ => unexpected!(parser, token, "a number"),
		}
	}
}
