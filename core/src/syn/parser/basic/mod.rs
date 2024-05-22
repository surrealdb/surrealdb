use crate::{
	sql::{language::Language, Datetime, Duration, Ident, Param, Regex, Strand, Table, Uuid},
	syn::{
		parser::{mac::unexpected, ParseError, ParseErrorKind, ParseResult, Parser},
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
		parser.parse_uuid()
	}
}

impl TokenValue for Regex {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		match parser.peek().kind {
			t!("/") => {
				assert!(!parser.has_peek());
				let pop = parser.pop_peek();
				let token = parser.lexer.relex_regex(pop);
				let regex = parser
					.span_str(token.span)
					.parse()
					.map_err(|e| ParseError::new(ParseErrorKind::InvalidRegex(e), token.span))?;
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
