use crate::sql::{
	lexer::Lexer,
	token::{Span, Token, TokenKind},
};

mod mac;
mod stmt;
mod value;

use super::{Query, Statement};

pub enum Expected {
	Identifier,
	TokenKind(TokenKind),
}

impl From<TokenKind> for Expected {
	fn from(value: TokenKind) -> Self {
		Expected::TokenKind(value)
	}
}

pub enum ParseErrorKind {
	Unexpected {
		found: TokenKind,
		expected: Expected,
	},
	UnexpectedEof {
		expected: TokenKind,
	},
	Todo,
}

pub struct ParseError {
	pub kind: ParseErrorKind,
	pub at: Span,
}

pub type ParseResult<T> = Result<T, ParseError>;

pub struct Parser<'a> {
	lexer: Lexer<'a>,
	peek: Option<Token>,
	last_span: Span,
}

impl<'a> Parser<'a> {
	pub fn new(source: &'a str) -> Self {
		Parser {
			lexer: Lexer::new(source),
			peek: None,
			last_span: Span::empty(),
		}
	}

	pub fn next_token(&mut self) -> Token {
		let res = self.peek.take().unwrap_or_else(|| self.lexer.next_token());
		self.last_span = res.span;
		res
	}

	pub fn peek_token(&mut self) -> Token {
		self.peek.get_or_insert_with(|| self.lexer.next_token()).clone()
	}

	pub fn last_span(&mut self) -> Span {
		self.peek.as_ref().map(|x| x.span).unwrap_or(self.last_span)
	}

	pub fn into_remaining(self) -> &'a str {
		todo!()
	}

	pub fn parse_query(&mut self) -> ParseResult<Query> {
		todo!()
	}

	pub fn parse_statement(&mut self) -> ParseResult<Statement> {
		todo!()
	}
}
