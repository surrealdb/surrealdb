use crate::{
	sql::{
		language::Language, Datetime, Duration, Ident, Number, Param, Regex, Strand, Table, Uuid,
	},
	syn::v2::{
		parser::mac::unexpected,
		token::{t, NumberKind, Token, TokenKind},
	},
};

use super::{ParseError, ParseErrorKind, ParseResult, Parser};

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
			| TokenKind::Distance(_) => {
				let str = parser.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(Ident(str))
			}
			TokenKind::Identifier => {
				let str = parser.lexer.string.take().unwrap();
				Ok(Ident(str))
			}
			x => {
				unexpected!(parser, x, "a identifier");
			}
		}
	}
}

impl TokenValue for Table {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		parser.token_value::<Ident>(token).map(|x| Table(x.0))
	}
}

impl TokenValue for u64 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number(NumberKind::Integer) => {
				let number = parser.lexer.string.take().unwrap().parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidInteger {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(number)
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for u32 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number(NumberKind::Integer) => {
				let number = parser.lexer.string.take().unwrap().parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidInteger {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(number)
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for u16 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number(NumberKind::Integer) => {
				let number = parser.lexer.string.take().unwrap().parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidInteger {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(number)
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for u8 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number(NumberKind::Integer) => {
				let number = parser.lexer.string.take().unwrap().parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidInteger {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(number)
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for f32 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number(NumberKind::NaN) => Ok(f32::NAN),
			TokenKind::Number(
				NumberKind::Integer
				| NumberKind::Float
				| NumberKind::FloatMantissa
				| NumberKind::Mantissa
				| NumberKind::MantissaExponent,
			) => {
				let number = parser.lexer.string.take().unwrap().parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidFloat {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(number)
			}
			x => unexpected!(parser, x, "a floating point number"),
		}
	}
}

impl TokenValue for f64 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number(NumberKind::NaN) => Ok(f64::NAN),
			TokenKind::Number(
				NumberKind::Integer
				| NumberKind::Float
				| NumberKind::FloatMantissa
				| NumberKind::Mantissa
				| NumberKind::MantissaExponent,
			) => {
				let number = parser.lexer.string.take().unwrap().parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidFloat {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(number)
			}
			x => unexpected!(parser, x, "a floating point number"),
		}
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
		match token.kind {
			TokenKind::Number(NumberKind::NaN) => Ok(Number::Float(f64::NAN)),
			TokenKind::Number(NumberKind::Integer) => {
				let source = parser.lexer.string.take().unwrap();
				if let Ok(x) = source.parse() {
					return Ok(Number::Int(x));
				}
				// integer overflowed, fallback to floating point
				// As far as I can tell this will never fail for valid integers.
				let x = source.parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidFloat {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(Number::Float(x))
			}
			TokenKind::Number(
				NumberKind::Mantissa
				| NumberKind::MantissaExponent
				| NumberKind::Float
				| NumberKind::FloatMantissa,
			) => {
				let source = parser.lexer.string.take().unwrap();
				// As far as I can tell this will never fail for valid integers.
				let x = source.parse().map_err(|e| {
					ParseError::new(
						ParseErrorKind::InvalidFloat {
							error: e,
						},
						token.span,
					)
				})?;
				Ok(Number::Float(x))
			}
			TokenKind::Number(NumberKind::Decimal) => {
				let source = parser.lexer.string.take().unwrap();
				// As far as I can tell this will never fail for valid integers.
				let x: rust_decimal::Decimal = source.parse().map_err(|error| {
					ParseError::new(
						ParseErrorKind::InvalidDecimal {
							error,
						},
						token.span,
					)
				})?;
				Ok(Number::Decimal(x))
			}
			TokenKind::Number(NumberKind::DecimalExponent) => {
				let source = parser.lexer.string.take().unwrap();
				// As far as I can tell this will never fail for valid integers.
				let x = rust_decimal::Decimal::from_scientific(&source).map_err(|error| {
					ParseError::new(
						ParseErrorKind::InvalidDecimal {
							error,
						},
						token.span,
					)
				})?;
				Ok(Number::Decimal(x))
			}
			x => unexpected!(parser, x, "a number"),
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
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let TokenKind::Duration = token.kind else {
			unexpected!(parser, token.kind, "a duration")
		};
		let duration = parser.lexer.duration.take().expect("token data was already consumed");
		Ok(duration)
	}
}

impl TokenValue for Datetime {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let TokenKind::DateTime = token.kind else {
			unexpected!(parser, token.kind, "a duration")
		};
		let datetime = parser.lexer.datetime.take().expect("token data was already consumed");
		Ok(datetime)
	}
}

impl TokenValue for Strand {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Strand => {
				let strand = parser.lexer.string.take().unwrap();
				Ok(Strand(strand))
			}
			x => unexpected!(parser, x, "a strand"),
		}
	}
}

impl TokenValue for Uuid {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let TokenKind::Uuid = token.kind else {
			unexpected!(parser, token.kind, "a duration")
		};
		Ok(parser.lexer.uuid.take().expect("token data was already consumed"))
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

	/// Returns if the peeked token can be a identifier.
	pub fn peek_can_be_ident(&mut self) -> bool {
		matches!(
			self.peek_kind(),
			TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::Identifier
		)
	}

	#[test]
	fn weird_things() {
		use crate::sql;

		fn assert_ident_parses_correctly(ident: &str) {
			let thing = format!("t:{}", ident);
			let mut parser = Parser::new(thing.as_bytes());
			parser.allow_fexible_record_id(true);
			let mut stack = Stack::new();
			let r = stack
				.enter(|ctx| async move { parser.parse_thing(ctx).await })
				.finish()
				.expect(&format!("failed on {}", ident))
				.id;
			assert_eq!(r, Id::String(ident.to_string()),);

			let mut parser = Parser::new(thing.as_bytes());
			let r = stack
				.enter(|ctx| async move { parser.parse_query(ctx).await })
				.finish()
				.expect(&format!("failed on {}", ident));

			assert_eq!(
				r,
				sql::Query(sql::Statements(vec![sql::Statement::Value(sql::Value::Thing(
					sql::Thing {
						tb: "t".to_string(),
						id: Id::String(ident.to_string())
					}
				))]))
			)
		}

		assert_ident_parses_correctly("123abc");
		assert_ident_parses_correctly("123d");
		assert_ident_parses_correctly("123de");
		assert_ident_parses_correctly("123dec");
		assert_ident_parses_correctly("1e23dec");
		assert_ident_parses_correctly("1e23f");
		assert_ident_parses_correctly("123f");
		assert_ident_parses_correctly("1ns");
		assert_ident_parses_correctly("1ns1");
		assert_ident_parses_correctly("1ns1h");
	}
}
