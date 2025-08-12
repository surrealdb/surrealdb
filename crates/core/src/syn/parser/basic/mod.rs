use std::mem;

use rust_decimal::Decimal;

use super::GluedValue;
use super::mac::pop_glued;
use crate::sql::language::Language;
use crate::sql::{Ident, Param};
use crate::syn::error::{bail, syntax_error};
use crate::syn::lexer::compound::{self, NumberKind};
use crate::syn::parser::mac::unexpected;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{self, TokenKind, t};
use crate::val::{Bytes, Datetime, DecimalExt as _, Duration, File, Number, Regex, Strand, Uuid};

mod number;

/// A trait for parsing single tokens with a specific value.
pub(crate) trait TokenValue: Sized {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self>;
}

impl TokenValue for Ident {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Identifier => {
				parser.pop_peek();
				let str = parser.lexer.string.take().unwrap();
				// Safety: lexer ensures no null bytes are present in the identifier.
				Ok(unsafe { Ident::new_unchecked(str) })
			}
			x if Parser::kind_is_keyword_like(x) => {
				let s = parser.pop_peek().span;
				// Safety: lexer ensures no null bytes are present in the identifier.
				Ok(unsafe { Ident::new_unchecked(parser.lexer.span_str(s).to_owned()) })
			}
			_ => {
				unexpected!(parser, token, "an identifier");
			}
		}
	}
}

impl TokenValue for Language {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let peek = parser.peek();
		match peek.kind {
			TokenKind::Language(x) => {
				parser.pop_peek();
				Ok(x)
			}
			// `NO` can both be used as a keyword and as a language.
			t!("NO") => {
				parser.pop_peek();
				Ok(Language::Norwegian)
			}
			_ => unexpected!(parser, peek, "a language"),
		}
	}
}

impl TokenValue for Param {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let peek = parser.peek();
		match peek.kind {
			TokenKind::Parameter => {
				parser.pop_peek();
				let param = parser.lexer.string.take().unwrap();
				// Safety: Lexer guarentees no null bytes.
				Ok(unsafe { Param::new_unchecked(param) })
			}
			_ => unexpected!(parser, peek, "a parameter"),
		}
	}
}

impl TokenValue for Duration {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Duration) => Ok(pop_glued!(parser, Duration)),
			TokenKind::Digits => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::duration)?.value;
				Ok(Duration(v))
			}
			_ => unexpected!(parser, token, "a duration"),
		}
	}
}

impl TokenValue for Datetime {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Datetime) => Ok(pop_glued!(parser, Datetime)),
			t!("d\"") | t!("d'") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::datetime)?.value;
				Ok(Datetime(v))
			}
			_ => unexpected!(parser, token, "a datetime"),
		}
	}
}

impl TokenValue for Strand {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Strand) => Ok(pop_glued!(parser, Strand)),
			t!("\"") | t!("'") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::strand)?.value;
				// Safety: The lexer ensures that no null bytes can be present in the string.
				Ok(unsafe { Strand::new_unchecked(v) })
			}
			_ => unexpected!(parser, token, "a strand"),
		}
	}
}

impl TokenValue for Uuid {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Uuid) => Ok(pop_glued!(parser, Uuid)),
			t!("u\"") | t!("u'") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::uuid)?.value;
				Ok(Uuid(v))
			}
			_ => unexpected!(parser, token, "a uuid"),
		}
	}
}

impl TokenValue for File {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		if !parser.settings.files_enabled {
			unexpected!(parser, token, "the experimental files feature to be enabled");
		}

		match token.kind {
			TokenKind::Glued(token::Glued::File) => Ok(pop_glued!(parser, File)),
			t!("f\"") | t!("f'") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::file)?.value;
				Ok(v)
			}
			_ => unexpected!(parser, token, "a file"),
		}
	}
}

impl TokenValue for Bytes {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Bytes) => Ok(pop_glued!(parser, Bytes)),
			t!("b\"") | t!("b'") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::bytes)?.value;
				Ok(v)
			}
			_ => unexpected!(parser, token, "a bytestring"),
		}
	}
}

impl TokenValue for Regex {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let peek = parser.peek();
		match peek.kind {
			t!("/") => {
				parser.pop_peek();
				if parser.has_peek() {
					// If the parser peeks past a `/` lexing the compound token can fail.
					// Peeking past `/` can happen when parsing `{/bla`.
					parser.backup_after(peek.span);
				}
				let v = parser.lexer.lex_compound(peek, compound::regex)?.value;
				Ok(v)
			}
			_ => unexpected!(parser, peek, "a regex"),
		}
	}
}

pub enum NumberToken {
	Float(f64),
	Integer(i64),
	Decimal(Decimal),
}

impl TokenValue for NumberToken {
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
						.map(NumberToken::Integer)
						.map_err(|e| syntax_error!("Failed to parse number: {e}", @token.span)),
					NumberKind::Float => number_str
						.trim_end_matches("f")
						.parse()
						.map(NumberToken::Float)
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
						Ok(NumberToken::Decimal(decimal))
					}
				}
			}
			t!("+") | t!("-") | TokenKind::Digits => {
				parser.pop_peek();
				let token = parser.lexer.lex_compound(token, compound::number)?;
				match token.value {
					compound::Numeric::Float(f) => Ok(NumberToken::Float(f)),
					compound::Numeric::Integer(i) => Ok(NumberToken::Integer(i)),
					compound::Numeric::Decimal(d) => Ok(NumberToken::Decimal(d)),
					compound::Numeric::Duration(_) => {
						bail!("Unexpected token `duration`, expected a number", @token.span)
					}
				}
			}
			_ => unexpected!(parser, token, "a number"),
		}
	}
}

// TODO: Remove once properly seperating AST from Expr.
impl TokenValue for Number {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.next_token_value::<NumberToken>()?;
		match token {
			NumberToken::Float(x) => Ok(Number::Float(x)),
			NumberToken::Integer(x) => Ok(Number::Int(x)),
			NumberToken::Decimal(x) => Ok(Number::Decimal(x)),
		}
	}
}

impl Parser<'_> {
	/// Parse a token value from the next token in the parser.
	pub(crate) fn next_token_value<V: TokenValue>(&mut self) -> ParseResult<V> {
		V::from_token(self)
	}

	pub(crate) fn parse_flexible_ident(&mut self) -> ParseResult<Ident> {
		let token = self.next();
		match token.kind {
			TokenKind::Digits => {
				let peek = self.peek_whitespace();
				let span = match peek.kind {
					x if Self::kind_is_keyword_like(x) => {
						self.pop_peek();
						token.span.covers(peek.span)
					}
					TokenKind::Identifier => {
						self.pop_peek();
						token.span.covers(peek.span)
					}
					_ => token.span,
				};
				// Safety: Lexer guarentees no null bytes.
				Ok(unsafe { Ident::new_unchecked(self.lexer.span_str(span).to_owned()) })
			}
			TokenKind::Identifier => {
				let str = self.lexer.string.take().unwrap();
				// Safety: Lexer guarentees no null bytes.
				Ok(unsafe { Ident::new_unchecked(str) })
			}
			x if Self::kind_is_keyword_like(x) => {
				// Safety: Lexer guarentees no null bytes.
				Ok(unsafe { Ident::new_unchecked(self.lexer.span_str(token.span).to_owned()) })
			}
			_ => {
				unexpected!(self, token, "an identifier");
			}
		}
	}
}

#[cfg(test)]
mod test {
	use crate::sql::{Ident, Part};

	#[test]
	fn identifiers() {
		use crate::sql;

		fn assert_ident_parses_correctly(ident: &str) {
			use reblessive::Stack;

			use crate::syn::Parser;

			let mut parser = Parser::new(ident.as_bytes());
			let mut stack = Stack::new();
			let r = stack
				.enter(|ctx| async move { parser.parse_query(ctx).await })
				.finish()
				.unwrap_or_else(|_| panic!("failed on {}", ident));

			assert_eq!(
				r.expressions,
				vec![sql::TopLevelExpr::Expr(sql::Expr::Idiom(sql::Idiom(vec![Part::Field(
					Ident::new(ident.to_owned()).unwrap()
				)])))]
			)
		}

		assert_ident_parses_correctly("select123");

		assert_ident_parses_correctly("e123");

		assert_ident_parses_correctly("dec123");
		assert_ident_parses_correctly("f123");

		assert_ident_parses_correctly("y123");
		assert_ident_parses_correctly("w123");
		assert_ident_parses_correctly("d123");
		assert_ident_parses_correctly("h123");
		assert_ident_parses_correctly("m123");
		assert_ident_parses_correctly("s123");
		assert_ident_parses_correctly("ms123");
		assert_ident_parses_correctly("us123");
		assert_ident_parses_correctly("ns123");
	}
}
