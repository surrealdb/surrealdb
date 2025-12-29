use core::f64;
use std::num::ParseIntError;
use std::str::FromStr;

use super::TokenValue;
use crate::syn::error::bail;
use crate::syn::lexer::compound::{self, Numeric};
use crate::syn::parser::mac::unexpected;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{TokenKind, t};

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
			t!("+") | t!("-") | TokenKind::Digits | TokenKind::NaN | TokenKind::Infinity => {
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
			t!("+") | t!("-") | TokenKind::Digits | TokenKind::NaN | TokenKind::Infinity => {
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
			t!("+") | t!("-") => {
				parser.pop_peek();
				Ok((parser.lexer.lex_compound(token, compound::number))?.value)
			}
			TokenKind::Digits => {
				parser.pop_peek();
				Ok((parser.lexer.lex_compound(token, compound::numeric))?.value)
			}
			TokenKind::NaN => {
				parser.pop_peek();
				Ok(Numeric::Float(f64::NAN))
			}
			TokenKind::Infinity => {
				parser.pop_peek();
				Ok(Numeric::Float(f64::INFINITY))
			}
			_ => unexpected!(parser, token, "a number"),
		}
	}
}
