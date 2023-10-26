use crate::{
	sql::{Dir, Duration, Ident, Number, Param, Strand},
	syn::{
		parser::mac::{to_do, unexpected},
		token::{t, Token, TokenKind},
	},
};

use super::{NumberParseError, ParseError, ParseErrorKind, ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_ident(&mut self) -> ParseResult<Ident> {
		self.parse_raw_ident().map(Ident)
	}

	pub fn parse_dir(&mut self) -> ParseResult<Dir> {
		match self.next().kind {
			t!("<-") => Ok(Dir::In),
			t!("<->") => Ok(Dir::Both),
			t!("->") => Ok(Dir::Out),
			x => unexpected!(self, x, "a direction"),
		}
	}

	pub fn parse_raw_ident(&mut self) -> ParseResult<String> {
		let next = self.next();
		self.parse_raw_ident_from_token(next)
	}

	pub fn parse_raw_ident_from_token(&mut self, token: Token) -> ParseResult<String> {
		match token.kind {
			TokenKind::Keyword(_) | TokenKind::Language(_) | TokenKind::Algorithm(_) => {
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(str)
			}
			TokenKind::Identifier => {
				let data_index = token.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				let str = self.lexer.strings[idx].clone();
				Ok(str)
			}
			x => {
				unexpected!(self, x, "a identifier");
			}
		}
	}

	pub(super) fn parse_u64(&mut self) -> ParseResult<u64> {
		let next = self.next();
		let number = match next.kind {
			TokenKind::Number => {
				let number =
					self.lexer.numbers[u32::from(next.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => {
						if x < 0 {
							to_do!(self)
						}
						x as u64
					}
					Number::Float(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::FloatToInt,
							},
							self.last_span(),
						))
					}
					Number::Decimal(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::DecimalToInt,
							},
							self.last_span(),
						))
					}
				}
			}
			x => unexpected!(self, x, "an integer"),
		};
		Ok(number)
	}

	pub(super) fn parse_u32(&mut self) -> ParseResult<u32> {
		let next = self.next();
		let number = match next.kind {
			TokenKind::Number => {
				let number =
					self.lexer.numbers[u32::from(next.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => {
						if x < 0 {
							to_do!(self)
						}
						match u32::try_from(x) {
							Ok(x) => x,
							Err(_) => {
								return Err(ParseError::new(
									ParseErrorKind::InvalidNumber {
										error: NumberParseError::IntegerOverflow,
									},
									self.last_span(),
								))
							}
						}
					}
					Number::Float(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::FloatToInt,
							},
							self.last_span(),
						))
					}
					Number::Decimal(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::DecimalToInt,
							},
							self.last_span(),
						))
					}
				}
			}
			x => unexpected!(self, x, "an integer"),
		};
		Ok(number)
	}

	pub(super) fn parse_u16(&mut self) -> ParseResult<u16> {
		let next = self.next();
		let number = match next.kind {
			TokenKind::Number => {
				let number =
					self.lexer.numbers[u32::from(next.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => {
						if x < 0 {
							to_do!(self)
						}
						match u16::try_from(x) {
							Ok(x) => x,
							Err(_) => {
								return Err(ParseError::new(
									ParseErrorKind::InvalidNumber {
										error: NumberParseError::IntegerOverflow,
									},
									self.last_span(),
								))
							}
						}
					}
					Number::Float(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::FloatToInt,
							},
							self.last_span(),
						))
					}
					Number::Decimal(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::DecimalToInt,
							},
							self.last_span(),
						))
					}
				}
			}
			x => unexpected!(self, x, "an integer"),
		};
		Ok(number)
	}

	pub(super) fn parse_f32(&mut self) -> ParseResult<f32> {
		let next = self.next();
		let number = match next.kind {
			TokenKind::Number => {
				let number =
					self.lexer.numbers[u32::from(next.data_index.unwrap()) as usize].clone();
				match number {
					Number::Int(x) => x as f32,
					Number::Float(x) => x as f32,
					Number::Decimal(_) => {
						return Err(ParseError::new(
							ParseErrorKind::InvalidNumber {
								error: NumberParseError::DecimalToInt,
							},
							self.last_span(),
						))
					}
				}
			}
			x => unexpected!(self, x, "an floating point"),
		};
		Ok(number)
	}

	pub(super) fn parse_number(&mut self) -> ParseResult<Number> {
		let next = self.next();
		match next.kind {
			TokenKind::Number => {
				let number =
					self.lexer.numbers[u32::from(next.data_index.unwrap()) as usize].clone();
				return Ok(number);
			}
			x => unexpected!(self, x, "a number"),
		}
	}

	pub(super) fn parse_param(&mut self) -> ParseResult<Param> {
		let next = self.next();
		match next.kind {
			TokenKind::Parameter => {
				let index = u32::from(next.data_index.unwrap());
				let param = self.lexer.strings[index as usize].clone();
				Ok(Param(Ident(param)))
			}
			x => unexpected!(self, x, "a parameter"),
		}
	}

	pub(super) fn parse_duration(&mut self) -> ParseResult<Duration> {
		let next = self.next();
		match next.kind {
			TokenKind::Duration => {
				let index = u32::from(next.data_index.unwrap());
				let duration = self.lexer.durations[index as usize];
				Ok(Duration(duration))
			}
			x => unexpected!(self, x, "a duration"),
		}
	}

	pub(super) fn parse_strand(&mut self) -> ParseResult<Strand> {
		let next = self.next();
		match next.kind {
			TokenKind::Strand => {
				let index = u32::from(next.data_index.unwrap());
				let strand = self.lexer.strings[index as usize].clone();
				Ok(Strand(strand))
			}
			x => unexpected!(self, x, "a strand"),
		}
	}
}
