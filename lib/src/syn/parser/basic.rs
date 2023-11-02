use crate::{
	sql::{language::Language, Datetime, Dir, Duration, Ident, Number, Param, Strand, Table, Uuid},
	syn::{
		parser::mac::{to_do, unexpected},
		token::{t, Token, TokenKind},
	},
};

use super::{NumberParseError, ParseError, ParseErrorKind, ParseResult, Parser};

pub trait TokenValue: Sized {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self>;
}

impl TokenValue for Ident {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Keyword(_) | TokenKind::Language(_) | TokenKind::Algorithm(_) => {
				let str = parser.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(Ident(str))
			}
			TokenKind::Identifier => {
				let data_index = token.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				let str = parser.lexer.strings[idx].clone();
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
		parser.from_token::<Ident>(token).map(|x| Table(x.0))
	}
}

impl TokenValue for u64 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number => {
				let number =
					parser.lexer.numbers[u32::from(token.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => {
						if x < 0 {
							to_do!(parser)
						}
						Ok(x as u64)
					}
					Number::Float(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::FloatToInt,
						},
						token.span,
					)),
					Number::Decimal(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::DecimalToInt,
						},
						token.span,
					)),
				}
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for u32 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number => {
				let number =
					parser.lexer.numbers[u32::from(token.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => {
						if x < 0 {
							to_do!(parser)
						}
						let res = match u32::try_from(x) {
							Ok(x) => x,
							Err(_) => {
								return Err(ParseError::new(
									ParseErrorKind::InvalidNumber {
										error: NumberParseError::IntegerOverflow,
									},
									token.span,
								))
							}
						};
						Ok(res)
					}
					Number::Float(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::FloatToInt,
						},
						token.span,
					)),
					Number::Decimal(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::DecimalToInt,
						},
						token.span,
					)),
				}
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for u16 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number => {
				let number =
					parser.lexer.numbers[u32::from(token.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => {
						if x < 0 {
							to_do!(parser)
						}
						let res = match u16::try_from(x) {
							Ok(x) => x,
							Err(_) => {
								return Err(ParseError::new(
									ParseErrorKind::InvalidNumber {
										error: NumberParseError::IntegerOverflow,
									},
									token.span,
								))
							}
						};
						Ok(res)
					}
					Number::Float(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::FloatToInt,
						},
						token.span,
					)),
					Number::Decimal(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::DecimalToInt,
						},
						token.span,
					)),
				}
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for u8 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Number => {
				let number =
					parser.lexer.numbers[u32::from(token.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => {
						if x < 0 {
							to_do!(parser)
						}
						let res = match u8::try_from(x) {
							Ok(x) => x,
							Err(_) => {
								return Err(ParseError::new(
									ParseErrorKind::InvalidNumber {
										error: NumberParseError::IntegerOverflow,
									},
									token.span,
								))
							}
						};
						Ok(res)
					}
					Number::Float(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::FloatToInt,
						},
						token.span,
					)),
					Number::Decimal(_) => Err(ParseError::new(
						ParseErrorKind::InvalidNumber {
							error: NumberParseError::DecimalToInt,
						},
						token.span,
					)),
				}
			}
			x => unexpected!(parser, x, "an integer"),
		}
	}
}

impl TokenValue for f32 {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let number = match token.kind {
			TokenKind::Number => {
				let number =
					parser.lexer.numbers[u32::from(token.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => x as f32,
					Number::Float(x) => x as f32,
					Number::Decimal(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::DecimalToInt,
							},
							token.span,
						))
					}
				}
			}
			x => unexpected!(parser, x, "an floating point"),
		};
		Ok(number)
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
			TokenKind::Number => {
				let number =
					parser.lexer.numbers[u32::from(token.data_index.unwrap()) as usize].clone();
				Ok(number)
			}
			x => unexpected!(parser, x, "a number"),
		}
	}
}

impl TokenValue for Param {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Parameter => {
				let index = u32::from(token.data_index.unwrap());
				let param = parser.lexer.strings[index as usize].clone();
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
		let index = u32::from(token.data_index.unwrap());
		let duration = parser.lexer.durations[index as usize];
		Ok(Duration(duration))
	}
}

impl TokenValue for Datetime {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		let TokenKind::DateTime = token.kind else {
			unexpected!(parser, token.kind, "a duration")
		};
		to_do!(parser)
	}
}

impl TokenValue for Strand {
	fn from_token(parser: &mut Parser<'_>, token: Token) -> ParseResult<Self> {
		match token.kind {
			TokenKind::Strand => {
				let index = u32::from(token.data_index.unwrap());
				let strand = parser.lexer.strings[index as usize].clone();
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
		let index = u32::from(token.data_index.unwrap());
		Ok(parser.lexer.uuid[index as usize].clone())
	}
}

impl Parser<'_> {
	pub fn parse_token_value<V: TokenValue>(&mut self) -> ParseResult<V> {
		let next = self.peek();
		let res = V::from_token(self, next);
		if res.is_ok() {
			self.pop_peek();
		}
		res
	}

	pub fn from_token<V: TokenValue>(&mut self, token: Token) -> ParseResult<V> {
		V::from_token(self, token)
	}

	pub fn parse_dir(&mut self) -> ParseResult<Dir> {
		match self.next().kind {
			t!("<-") => Ok(Dir::In),
			t!("<->") => Ok(Dir::Both),
			t!("->") => Ok(Dir::Out),
			x => unexpected!(self, x, "a direction"),
		}
	}
}
