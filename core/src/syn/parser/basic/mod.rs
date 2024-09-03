use crate::{
	sql::{language::Language, Datetime, Duration, Ident, Param, Regex, Strand, Table, Uuid},
	syn::{
		parser::{mac::unexpected, ParseResult, Parser},
		token::{t, QouteKind, TokenKind},
	},
};

mod datetime;
mod number;

/// A trait for parsing single tokens with a specific value.
pub trait TokenValue: Sized {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self>;
}

impl TokenValue for Ident {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.glue_ident(false)?;
		match token.kind {
			TokenKind::Identifier => {
				parser.pop_peek();
				let str = parser.lexer.string.take().unwrap();
				Ok(Ident(str))
			}
			TokenKind::Keyword(_) | TokenKind::Language(_) | TokenKind::Algorithm(_) => {
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
		let token = parser.glue_duration()?;
		match token.kind {
			TokenKind::Duration => {
				parser.pop_peek();
				Ok(Duration(parser.lexer.duration.unwrap()))
			}
			_ => unexpected!(parser, token, "a duration"),
		}
	}
}

impl TokenValue for Datetime {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		parser.parse_datetime()
	}
}

impl TokenValue for Strand {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Qoute(QouteKind::Plain | QouteKind::PlainDouble) => {
				parser.pop_peek();
				let t = parser.lexer.relex_strand(token);
				let TokenKind::Strand = t.kind else {
					unexpected!(parser, t, "a strand")
				};
				Ok(Strand(parser.lexer.string.take().unwrap()))
			}
			TokenKind::Strand => {
				parser.pop_peek();
				Ok(Strand(parser.lexer.string.take().unwrap()))
			}
			_ => unexpected!(parser, token, "a strand"),
		}
	}
}

impl TokenValue for Uuid {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let peek = parser.peek();
		match peek.kind {
			t!("u\"") | t!("u'") => {
				let pop = parser.pop_peek();
				Ok(parser.lexer.lex_compound(pop)?.value)
			}
			_ => unexpected!(parser, peek, "a UUID"),
		}
	}
}

impl TokenValue for Regex {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let peek = parser.peek();
		match peek.kind {
			t!("/") => {
				let pop = parser.pop_peek();
				Ok(parser.lexer.lex_compound(pop)?.value)
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
