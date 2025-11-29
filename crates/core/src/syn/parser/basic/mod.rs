use std::mem;

use rust_decimal::Decimal;

use super::GluedValue;
use super::mac::pop_glued;
use crate::sql::Param;
use crate::sql::language::Language;
use crate::syn::error::{bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::lexer::compound::{self, NumberKind};
use crate::syn::parser::mac::unexpected;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{self, Span, TokenKind, t};
use crate::val::DecimalExt as _;

mod number;

/// A trait for parsing single tokens with a specific value.
pub(crate) trait TokenValue: Sized {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self>;
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
				let mut span = peek.span;
				span.offset += 1;
				span.len -= 1;
				let str = parser.lexer.span_str(span);
				let ident = Lexer::unescape_ident_span(str, span, &mut parser.unscape_buffer)?;
				// Safety: Lexer guarentees no null bytes.
				Ok(Param::new(ident.to_owned()))
			}
			_ => unexpected!(parser, peek, "a parameter"),
		}
	}
}

impl TokenValue for surrealdb_types::Duration {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Duration) => Ok(pop_glued!(parser, Duration)),
			TokenKind::Digits => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::duration)?.value;
				Ok(surrealdb_types::Duration::from(v))
			}
			_ => unexpected!(parser, token, "a duration"),
		}
	}
}

impl TokenValue for surrealdb_types::Datetime {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			t!("d\"") | t!("d'") => {
				parser.pop_peek();
				let string_source = parser.lexer.span_str(token.span);
				let str = Lexer::unescape_string_span(
					string_source,
					token.span,
					&mut parser.unscape_buffer,
				)?;

				// +2 to skip over the `d"`
				let file = Lexer::lex_datetime(str).map_err(|e| {
					e.update_spans(|span| {
						let range = span.to_range();
						let start = Lexer::escaped_string_offset(string_source, range.start);
						let end = Lexer::escaped_string_offset(string_source, range.end);
						*span = Span::from_range(
							(token.span.offset + start)..(token.span.offset + end),
						);
					})
				})?;

				Ok(file)
			}
			_ => unexpected!(parser, token, "a datetime"),
		}
	}
}

impl TokenValue for surrealdb_types::Uuid {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			t!("u\"") | t!("u'") => {
				parser.pop_peek();
				let string_source = parser.lexer.span_str(token.span);
				let str = Lexer::unescape_string_span(
					string_source,
					token.span,
					&mut parser.unscape_buffer,
				)?;

				let file = Lexer::lex_uuid(str).map_err(|e| {
					e.update_spans(|span| {
						let range = span.to_range();
						let start = Lexer::escaped_string_offset(string_source, range.start);
						let end = Lexer::escaped_string_offset(string_source, range.end);
						*span = Span::from_range(
							(token.span.offset + start)..(token.span.offset + end),
						);
					})
				})?;

				Ok(file)
			}
			_ => unexpected!(parser, token, "a uuid"),
		}
	}
}

impl TokenValue for surrealdb_types::File {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		if !parser.settings.files_enabled {
			unexpected!(parser, token, "the experimental files feature to be enabled");
		}

		match token.kind {
			t!("f\"") | t!("f'") => {
				parser.pop_peek();
				let string_source = parser.lexer.span_str(token.span);
				let str = Lexer::unescape_string_span(
					string_source,
					token.span,
					&mut parser.unscape_buffer,
				)?;

				let file = Lexer::lex_file(str).map_err(|e| {
					e.update_spans(|span| {
						let range = span.to_range();
						let start = Lexer::escaped_string_offset(string_source, range.start);
						let end = Lexer::escaped_string_offset(string_source, range.end);
						*span = Span::from_range(
							(token.span.offset + start)..(token.span.offset + end),
						);
					})
				})?;

				Ok(file)
			}
			_ => unexpected!(parser, token, "a file"),
		}
	}
}

impl TokenValue for surrealdb_types::Bytes {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			t!("b\"") | t!("b'") => {
				parser.pop_peek();
				let string_source = parser.lexer.span_str(token.span);
				let str = Lexer::unescape_string_span(
					string_source,
					token.span,
					&mut parser.unscape_buffer,
				)?;

				let bytes = Lexer::lex_bytes(str).map_err(|e| {
					e.update_spans(|span| {
						let range = span.to_range();
						let start = Lexer::escaped_string_offset(string_source, range.start);
						let end = Lexer::escaped_string_offset(string_source, range.end);
						*span = Span::from_range(
							(token.span.offset + start)..(token.span.offset + end),
						);
					})
				})?;

				Ok(bytes)
			}
			_ => unexpected!(parser, token, "a bytestring"),
		}
	}
}

impl TokenValue for surrealdb_types::Regex {
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
impl TokenValue for surrealdb_types::Number {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.next_token_value::<NumberToken>()?;
		match token {
			NumberToken::Float(x) => Ok(Self::Float(x)),
			NumberToken::Integer(x) => Ok(Self::Int(x)),
			NumberToken::Decimal(x) => Ok(Self::Decimal(x)),
		}
	}
}

impl Parser<'_> {
	/// Parse a token value from the next token in the parser.
	pub(crate) fn next_token_value<V: TokenValue>(&mut self) -> ParseResult<V> {
		V::from_token(self)
	}

	pub(crate) fn parse_string_lit(&mut self) -> ParseResult<String> {
		let token = self.peek();
		match token.kind {
			t!("\"") | t!("'") => {
				self.pop_peek();
				let str = self.lexer.span_str(token.span);
				let str = Lexer::unescape_string_span(str, token.span, &mut self.unscape_buffer)?;
				Ok(str.to_owned())
			}
			_ => unexpected!(self, token, "a strand"),
		}
	}

	pub(crate) fn parse_ident(&mut self) -> ParseResult<String> {
		self.parse_ident_str().map(|x| x.to_owned())
	}

	pub(crate) fn parse_ident_str(&mut self) -> ParseResult<&str> {
		let token = self.next();
		match token.kind {
			TokenKind::Identifier => {
				let str = self.lexer.span_str(token.span);
				Ok(Lexer::unescape_ident_span(str, token.span, &mut self.unscape_buffer)?)
			}
			x if Self::kind_is_keyword_like(x) => {
				// Safety: Lexer guarentees no null bytes.
				Ok(self.lexer.span_str(token.span))
			}
			_ => {
				unexpected!(self, token, "an identifier");
			}
		}
	}

	pub(crate) fn parse_flexible_ident(&mut self) -> ParseResult<String> {
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
				Ok(self.lexer.span_str(span).to_owned())
			}
			TokenKind::Identifier => {
				let str = self.lexer.span_str(token.span);
				let str = Lexer::unescape_ident_span(str, token.span, &mut self.unscape_buffer)?;
				Ok(str.to_owned())
			}
			x if Self::kind_is_keyword_like(x) => Ok(self.lexer.span_str(token.span).to_owned()),
			_ => {
				unexpected!(self, token, "an identifier");
			}
		}
	}
}

#[cfg(test)]
mod test {
	use crate::sql::Part;

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
					ident.to_owned()
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
