use crate::{
	sql::{language::Language, Datetime, Duration, Ident, Param, Regex, Strand, Table, Uuid},
	syn::{
		parser::{mac::unexpected, ParseError, ParseErrorKind, ParseResult, Parser},
		token::{t, QouteKind, TokenKind},
	},
};

mod datetime;
mod number;
mod uuid;

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
			TokenKind::Keyword(_) | TokenKind::Language(_) | TokenKind::Algorithm(_) => {
				let s = parser.pop_peek().span;
				Ok(Ident(parser.span_str(s).to_owned()))
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
		parser.parse_datetime()
	}
}

impl TokenValue for Strand {
	fn from_token(parser: &mut Parser<'_>) -> ParseResult<Self> {
		let token = parser.peek();
		match token.kind {
			TokenKind::Qoute(QouteKind::Plain | QouteKind::PlainDouble) | TokenKind::Strand => {
				parser.pop_peek();
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
				let pop = parser.pop_peek();
				assert!(!parser.has_peek());
				let token = parser.lexer.relex_regex(pop);
				let mut span = token.span;

				// remove the starting and ending `/` characters.
				span.offset += 1;
				span.len -= 2;

				let regex = parser
					.span_str(span)
					.parse()
					.map_err(|e| ParseError::new(ParseErrorKind::InvalidRegex(e), token.span))?;
				return Ok(regex);
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
}
