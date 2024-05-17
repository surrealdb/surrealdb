use std::{
	num::{ParseFloatError, ParseIntError},
	str::FromStr,
	time::Duration as StdDuration,
};

use crate::{
	sql::{
		duration::{
			SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK,
			SECONDS_PER_YEAR,
		},
		language::Language,
		Datetime, Duration, Ident, Number, Param, Regex, Strand, Table, Uuid,
	},
	syn::{
		parser::{mac::unexpected, ParseError, ParseErrorKind, ParseResult, Parser},
		token::{t, DurationSuffix, NumberSuffix, QouteKind, Token, TokenKind},
	},
};

/// A trait for parsing single tokens with a specific value.
pub trait TokenValue: Sized {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self>;
}

impl TokenValue for Ident {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Keyword(_)
			| TokenKind::Language(_)
			| TokenKind::Algorithm(_)
			| TokenKind::Distance(_)
			| TokenKind::VectorType(_) => {
				let str = parser.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(Ident(str))
			}
			TokenKind::DurationSuffix(suffix) => {
				if !suffix.can_be_ident() {
					unexpected!(parser, token.kind, "an identifier")
				}
				let mut buffer = suffix.as_str().to_owned();
				if let Err(span) = parser.glue_ident(token, &mut buffer) {
					return Err(ParseError::new(
						ParseErrorKind::Unexpected {
							found: parser.peek_kind(),
							expected: "an identifier",
						},
						span,
					));
				}
				Ok(Ident(buffer))
			}
			TokenKind::Exponent => {
				let str = parser.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let mut buffer = std::str::from_utf8(str).unwrap().to_owned();
				if let Err(span) = parser.glue_ident(token, &mut buffer) {
					return Err(ParseError::new(
						ParseErrorKind::Unexpected {
							found: parser.peek_kind(),
							expected: "an identifier",
						},
						span,
					));
				}
				Ok(Ident(buffer))
			}
			TokenKind::Identifier => {
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
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parser.token_value::<Ident>(token).map(|x| Table(x.0))
	}
}

/// Generic integer parsing method,
/// works for all unsigned integers.
fn parse_integer<I>(parser: &mut Parser<'_>, token: Token) -> ParseResult<I>
where
	I: FromStr<Err = ParseIntError>,
{
	match token.kind {
		TokenKind::Digits => {
			let digits_string = parser.lexer.string.take().unwrap();
			let p = parser.peek();
			if p.follows_from(&token) {
				match p.kind {
					t!(".")
					| TokenKind::NumberSuffix(_)
					| TokenKind::DurationSuffix(_)
					| TokenKind::Exponent => {
						unexpected!(parser, p.kind, "an integer")
					}
					_ => {}
				}
			}
			let res = digits_string
				.parse()
				.map_err(ParseErrorKind::InvalidInteger)
				.map_err(|e| ParseError::new(e, token.span))?;
			Ok(res)
		}
		x => unexpected!(parser, x, "an integer"),
	}
}

impl TokenValue for u64 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parse_integer(parser, token)
	}
}

impl TokenValue for u32 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parse_integer(parser, token)
	}
}

impl TokenValue for u16 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parse_integer(parser, token)
	}
}

impl TokenValue for u8 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parse_integer(parser, token)
	}
}

/// Generic float parsing method,
/// works for both f32 and f64
fn parse_float<F>(parser: &mut Parser<'_>, token: Token) -> ParseResult<F>
where
	F: FromStr<Err = ParseFloatError>,
{
	// find initial  digits
	let mut buffer: String = match token.kind {
		TokenKind::NaN => return Ok("NaN".parse().unwrap()),
		TokenKind::Digits => {
			let span = parser.lexer.reader.span(token.span);
			// filter out all the '_'
			span.iter().copied().filter(|x| *x != b'_').map(|x| x as char).collect()
		}

		x => unexpected!(parser, x, "a floating point number"),
	};

	if let Err(span) = parser.glue_float(token, &mut buffer) {
		return Err(ParseError::new(
			ParseErrorKind::Unexpected {
				found: parser.peek_kind(),
				expected: "a floating point number",
			},
			span,
		));
	}

	let span = token.span.covers(parser.last_span());
	buffer.parse().map_err(ParseErrorKind::InvalidFloat).map_err(|e| ParseError::new(e, span))
}

impl TokenValue for f32 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parse_float(parser, token)
	}
}

impl TokenValue for f64 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parse_float(parser, token)
	}
}

impl TokenValue for Language {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Language(x) => Ok(x),
			// `NO` can both be used as a keyword and as a language.
			t!("NO") => Ok(Language::Norwegian),
			x => unexpected!(parser, x, "a language"),
		}
	}
}

impl TokenValue for Number {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let mut digits_token = token;
		// handle +/-/digits/NaN
		let number_buffer: String = match digits_token.kind {
			t!("+") => {
				// next must be a digit or it is an invalid number
				let p = parser.peek();
				if !p.follows_from(&digits_token) {
					unexpected!(parser, digits_token.kind, "a number")
				}

				let TokenKind::Digits = p.kind else {
					unexpected!(parser, digits_token.kind, "a number")
				};

				digits_token = p;

				parser
					.lexer
					.reader
					.span(digits_token.span)
					.iter()
					.copied()
					.filter(|x| *x != b'_')
					.map(|x| x as char)
					.collect()
			}
			t!("-") => {
				// next must be a digit or it is an invalid number
				let p = parser.peek();
				if !p.follows_from(&digits_token) {
					unexpected!(parser, digits_token.kind, "a number")
				}

				let TokenKind::Digits = p.kind else {
					unexpected!(parser, digits_token.kind, "a number")
				};

				digits_token = p;

				let mut buffer = String::new();
				buffer.push('-');
				buffer.extend(
					parser
						.lexer
						.reader
						.span(digits_token.span)
						.iter()
						.copied()
						.filter(|x| *x != b'_')
						.map(|x| x as char),
				);
				buffer
			}
			TokenKind::NaN => return Ok(Number::Float(f64::NAN)),
			TokenKind::Digits => parser
				.lexer
				.reader
				.span(token.span)
				.iter()
				.copied()
				.filter(|x| *x != b'_')
				.map(|x| x as char)
				.collect(),
			x => unexpected!(parser, x, "a number"),
		};

		let p = parser.peek();
		if !p.follows_from(&digits_token) {
			let s = token.span.covers(digits_token.span);
			let v = number_buffer
				.parse()
				.map_err(ParseErrorKind::InvalidInteger)
				.map_err(|e| ParseError::new(e, s))?;
			return Ok(Number::Int(v));
		}

		match p.kind {
			TokenKind::NumberSuffix(NumberSuffix::Decimal) => {
				let s = token.span.covers(p.span);
				let v = number_buffer
					.parse()
					.map_err(ParseErrorKind::InvalidDecimal)
					.map_err(|e| ParseError::new(e, s))?;
				return Ok(Number::Decimal(v));
			}
			TokenKind::NumberSuffix(NumberSuffix::Float) => {
				let s = token.span.covers(p.span);
				let v = number_buffer
					.parse()
					.map_err(ParseErrorKind::InvalidFloat)
					.map_err(|e| ParseError::new(e, s))?;
				return Ok(Number::Float(v));
			}
			t!(".") | TokenKind::Exponent => {
				let mut number_buffer = number_buffer;
				if let Err(span) = parser.glue_float(digits_token, &mut number_buffer) {
					return Err(ParseError::new(
						ParseErrorKind::Unexpected {
							found: parser.peek_kind(),
							expected: "a number",
						},
						span,
					));
				}
				let s = token.span.covers(parser.last_span());
				let v = number_buffer
					.parse()
					.map_err(ParseErrorKind::InvalidFloat)
					.map_err(|e| ParseError::new(e, s))?;
				return Ok(Number::Float(v));
			}
			x => {
				unexpected!(parser, x, "a number")
			}
		}
	}
}

impl TokenValue for Param {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Parameter => {
				let param = parser.lexer.string.take().unwrap();
				Ok(Param(Ident(param)))
			}
			x => unexpected!(parser, x, "a parameter"),
		}
	}
}

impl TokenValue for Duration {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {}
}

impl TokenValue for Datetime {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let TokenKind::Qoute(QouteKind::DateTime) = token.kind else {
			unexpected!(parser, token.kind, "a datetime")
		};
		let datetime = parser.lexer.datetime.take().expect("token data was already consumed");
		Ok(datetime)
	}
}

impl TokenValue for Strand {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
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
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let TokenKind::Qoute(QouteKind::Uuid) = token.kind else {
			unexpected!(parser, token.kind, "a datetime")
		};
		Ok(parser.lexer.uuid.take().unwrap())
	}
}

impl TokenValue for Regex {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let TokenKind::Regex = token.kind else {
			unexpected!(parser, token.kind, "a regex")
		};
		Ok(parser.lexer.regex.take().expect("token data was already consumed"))
	}
}

impl Parser<'_> {
	/// Parse a token value from the next token in the parser.
	pub fn next_token_value<V: TokenValue>(&mut self) -> ParseResult<V> {
		let next = self.peek();
		let res = V::from_token(self, next);
		if res.is_ok() {
			self.pop_peek();
		}
		res
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

	/// Parse a token value from the given token.
	pub fn token_value<V: TokenValue>(&mut self, token: Token) -> ParseResult<V> {
		V::from_token(self, token)
	}
}
