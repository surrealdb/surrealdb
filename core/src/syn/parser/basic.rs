use std::{
	num::{ParseFloatError, ParseIntError},
	str::FromStr,
};

use crate::{
	sql::{
		language::Language, Datetime, Duration, Ident, Number, Param, Regex, Strand, Table, Uuid,
	},
	syn::{
		parser::{mac::unexpected, ParseError, ParseErrorKind, ParseResult, Parser},
		token::{t, NumberKind, QouteKind, TokenKind},
	},
};

/// A trait for parsing single tokens with a specific value.
pub trait TokenValue: Sized {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self>;
}

impl TokenValue for Ident {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.glue_ident(false)?.kind {
			TokenKind::Identifier => {
				parser.pop_peek();
				let str = parser.lexer.string.take().unwrap();
				Ok(Ident(str))
			}
			x => {
				unexpected!(parser, x, "an identifier");
			}
		}
	}
}

impl TokenValue for Table {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parser.next_token_value::<Ident>().map(|x| Table(x.0))
	}
}

/// Generic integer parsing method,
/// works for all unsigned integers.
fn parse_integer<I>(parser: &mut Parser<'_>) -> ParseResult<I>
where
	I: FromStr<Err = ParseIntError>,
{
	let peek = parser.peek();
	match peek.kind {
		TokenKind::Digits => {
			parser.pop_peek();
			let digits_string = parser.lexer.string.take().unwrap();

			assert!(!parser.has_peek());

			let p = parser.peek_whitespace();
			match p.kind {
				t!(".") => {
					unexpected!(parser, p.kind, "an integer")
				}
				t!("dec") => {
					unexpected!(parser, p.kind, "an integer" => "decimal numbers not supported here")
				}
				x if Parser::tokenkind_continues_ident(x) => {
					unexpected!(parser, p.kind, "an integer")
				}
				_ => {}
			}
			let res = digits_string
				.parse()
				.map_err(ParseErrorKind::InvalidInteger)
				.map_err(|e| ParseError::new(e, peek.span))?;
			Ok(res)
		}
		x => unexpected!(parser, x, "an integer"),
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
		TokenKind::Digits => {}
		x => unexpected!(parser, x, "a floating point number"),
	};
	let float_token = parser.glue_float()?;
	match float_token.kind {
		TokenKind::Number(NumberKind::Float) => {
			parser.pop_peek();
		}
		x => unexpected!(parser, x, "a floating point number"),
	};

	let span = parser.span_str(float_token.span);
	// remove the possible "f" number suffix
	span.strip_suffix("f")
		.unwrap_or(span)
		.parse()
		.map_err(ParseErrorKind::InvalidFloat)
		.map_err(|e| ParseError::new(e, float_token.span))
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

impl TokenValue for Language {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.peek_kind() {
			TokenKind::Language(x) => {
				parser.pop_peek();
				Ok(x)
			}
			// `NO` can both be used as a keyword and as a language.
			t!("NO") => {
				parser.pop_peek();
				Ok(Language::Norwegian)
			}
			x => unexpected!(parser, x, "a language"),
		}
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
			x => unexpected!(parser, x, "a number"),
		};

		parser.pop_peek();
		let span = parser.span_str(number.span);

		match number_kind {
			NumberKind::Decimal => {
				let decimal =
					span.strip_suffix("dec").unwrap_or(span).parse().map_err(|e| {
						ParseError::new(ParseErrorKind::InvalidDecimal(e), number.span)
					})?;

				Ok(Number::Decimal(decimal))
			}
			NumberKind::Float => {
				let float =
					span.strip_suffix("f").unwrap_or(span).parse().map_err(|e| {
						ParseError::new(ParseErrorKind::InvalidFloat(e), number.span)
					})?;

				Ok(Number::Float(float))
			}
			NumberKind::Integer => {
				let integer =
					span.strip_suffix("f").unwrap_or(span).parse().map_err(|e| {
						ParseError::new(ParseErrorKind::InvalidInteger(e), number.span)
					})?;

				Ok(Number::Int(integer))
			}
		}
	}
}

impl TokenValue for Param {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.peek_kind() {
			TokenKind::Parameter => {
				parser.pop_peek();
				let param = parser.lexer.string.take().unwrap();
				Ok(Param(Ident(param)))
			}
			x => unexpected!(parser, x, "a parameter"),
		}
	}
}

impl TokenValue for Duration {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.glue_duration()?.kind {
			TokenKind::Duration => {
				parser.pop_peek();
				return Ok(Duration(parser.lexer.duration.unwrap()));
			}
			x => unexpected!(parser, x, "a duration"),
		}
	}
}

impl TokenValue for Datetime {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.glue_duration()?.kind {
			TokenKind::Datetime => {
				parser.pop_peek();
				return Ok(Datetime(parser.lexer.datetime.unwrap()));
			}
			x => unexpected!(parser, x, "a datetime"),
		}
	}
}

impl TokenValue for Strand {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Qoute(QouteKind::Plain | QouteKind::PlainDouble) => {
				let t = parser.lexer.relex_strand(token);
				let TokenKind::Strand = t.kind else {
					unexpected!(parser, t.kind, "a strand")
				};
				return Ok(Strand(parser.lexer.string.take().unwrap()));
			}
			x => unexpected!(parser, x, "a strand"),
		}
	}
}

impl TokenValue for Uuid {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.glue_uuid_strand()?.kind {
			TokenKind::Uuid => {
				parser.pop_peek();
				return Ok(Uuid(parser.lexer.uuid.unwrap()));
			}
			x => unexpected!(parser, x, "a uuid"),
		}
	}
}

impl TokenValue for Regex {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.glue_regex()?.kind {
			TokenKind::Regex => {
				let span = parser.pop_peek().span;
				let regex = parser
					.lexer
					.string
					.take()
					.unwrap()
					.parse()
					.map_err(|e| ParseError::new(ParseErrorKind::InvalidRegex(e), span))?;
				return Ok(Regex(regex));
			}
			x => unexpected!(parser, x, "a regex"),
		}
	}
}

impl Parser<'_> {
	/// Parse a token value from the next token in the parser.
	pub fn next_token_value<V: TokenValue>(&mut self) -> ParseResult<V> {
		V::from_token(self)
	}

	pub fn parse_signed_float(&mut self) -> ParseResult<f64> {
		let neg = self.eat(t!("-"));
		if !neg {
			self.eat(t!("+"));
		}
		let res: f64 = self.next_token_value()?;
		if neg {
			Ok(-res)
		} else {
			Ok(res)
		}
	}
}
