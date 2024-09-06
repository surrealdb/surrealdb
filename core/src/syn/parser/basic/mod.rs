use crate::{
	sql::{language::Language, Datetime, Duration, Ident, Param, Regex, Strand, Table, Uuid},
	syn::{
		lexer::compound,
		parser::{mac::unexpected, ParseResult, Parser},
		token::{self, t, TokenKind},
	},
};

use super::mac::pop_glued;

mod number;

/// A trait for parsing single tokens with a specific value.
pub trait TokenValue: Sized {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self>;
}

impl TokenValue for Ident {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Identifier => {
				parser.pop_peek();
				let str = parser.lexer.string.take().unwrap();
				Ok(Ident(str))
			}
			x if Parser::kind_is_keyword_like(x) => {
				let s = parser.pop_peek().span;
				Ok(Ident(parser.lexer.span_str(s).to_owned()))
			}
			_ => {
				unexpected!(parser, token, "an identifier");
			}
		}
	}
}

impl TokenValue for Table {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parser.next_token_value::<Ident>().map(|x| Table(x.0))
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
				Ok(Param(Ident(param)))
			}
			_ => unexpected!(parser, peek, "a parameter"),
		}
	}
}

impl TokenValue for Duration {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Duration) => {
				let x = pop_glued!(parser, Duration);
				parser.pop_peek();
				Ok(x)
			}
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
			TokenKind::Glued(token::Glued::Datetime) => {
				let x = pop_glued!(parser, Datetime);
				parser.pop_peek();
				Ok(x)
			}
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
			TokenKind::Glued(token::Glued::Strand) => {
				let x = pop_glued!(parser, Strand);
				parser.pop_peek();
				Ok(x)
			}
			t!("\"") | t!("'") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::strand)?.value;
				Ok(Strand(v))
			}
			_ => unexpected!(parser, token, "a datetime"),
		}
	}
}

impl TokenValue for Uuid {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Glued(token::Glued::Uuid) => {
				let x = pop_glued!(parser, Uuid);
				parser.pop_peek();
				Ok(x)
			}
			t!("u\"") | t!("u'") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(token, compound::uuid)?.value;
				Ok(Uuid(v))
			}
			_ => unexpected!(parser, token, "a datetime"),
		}
	}
}

impl TokenValue for Regex {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let peek = parser.peek();
		match peek.kind {
			t!("/") => {
				parser.pop_peek();
				let v = parser.lexer.lex_compound(peek, compound::regex)?.value;
				Ok(Regex(v))
			}
			_ => unexpected!(parser, peek, "a regex"),
		}
	}
}

impl Parser<'_> {
	/// Parse a token value from the next token in the parser.
	pub fn next_token_value<V: TokenValue>(&mut self) -> ParseResult<V> {
		V::from_token(self)
	}

	pub fn parse_flexible_ident(&mut self) -> ParseResult<Ident> {
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
				Ok(Ident(self.lexer.span_str(span).to_owned()))
			}
			TokenKind::Identifier => {
				let str = self.lexer.string.take().unwrap();
				Ok(Ident(str))
			}
			x if Self::kind_is_keyword_like(x) => {
				Ok(Ident(self.lexer.span_str(token.span).to_owned()))
			}
			_ => {
				unexpected!(self, token, "an identifier");
			}
		}
	}
}

#[cfg(test)]
mod test {

	#[test]
	fn identifiers() {
		use crate::sql;

		fn assert_ident_parses_correctly(ident: &str) {
			use crate::syn::Parser;
			use reblessive::Stack;

			let mut parser = Parser::new(ident.as_bytes());
			let mut stack = Stack::new();
			let r = stack
				.enter(|ctx| async move { parser.parse_query(ctx).await })
				.finish()
				.unwrap_or_else(|_| panic!("failed on {}", ident));

			assert_eq!(
				r,
				sql::Query(sql::Statements(vec![sql::Statement::Value(sql::Value::Idiom(
					sql::Idiom(vec![sql::Part::Field(sql::Ident(ident.to_string()))])
				))]))
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
