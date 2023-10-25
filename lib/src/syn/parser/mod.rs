use crate::{
	sql,
	syn::{
		lexer::Lexer,
		parser::mac::unexpected,
		token::{t, Span, Token, TokenKind},
	},
};

use self::token_buffer::TokenBuffer;

mod basic;
mod idiom;
mod kind;
mod mac;
mod object;
mod operator;
mod prime;
mod stmt;
mod token_buffer;
mod value;

#[derive(Debug, Clone, Copy)]
pub struct Checkpoint(usize);

#[derive(Debug)]
pub enum ParseErrorKind {
	/// The parser encountered an unexpected token.
	Unexpected {
		found: TokenKind,
		expected: &'static str,
	},
	/// The parser encountered an unexpected token.
	UnexpectedEof {
		expected: &'static str,
	},
	UnclosedDelimiter {
		expected: TokenKind,
		should_close: Span,
	},
	Retried {
		first: Box<ParseError>,
		then: Box<ParseError>,
	},
	DisallowedStatement,
	/// The parser encountered an token which could not be lexed correctly.
	InvalidToken,
	/// A path in the parser which was not yet finished.
	/// Should eventually be removed.
	Todo,
}

#[derive(Debug)]
pub struct ParseError {
	pub kind: ParseErrorKind,
	pub at: Span,
	pub backtrace: std::backtrace::Backtrace,
}

impl ParseError {
	pub fn new(kind: ParseErrorKind, at: Span) -> Self {
		ParseError {
			kind,
			at,
			backtrace: std::backtrace::Backtrace::force_capture(),
		}
	}
}

pub type ParseResult<T> = Result<T, ParseError>;

pub struct Parser<'a> {
	lexer: Lexer<'a>,
	last_span: Span,
	token_buffer: TokenBuffer<4>,
}

impl<'a> Parser<'a> {
	pub fn new(source: &'a str) -> Self {
		Parser {
			lexer: Lexer::new(source),
			last_span: Span::empty(),
			token_buffer: TokenBuffer::new(),
		}
	}

	/// Returns the next token.
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap_or_else(|| self.lexer.next_token());
		self.last_span = res.span;
		res
	}

	/// Consume the current peeked value.
	///
	/// Should only be called after peeking a value.
	pub fn pop_peek(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap();
		self.last_span = res.span;
		res
	}

	/// Returns the next token without consuming it.
	pub fn peek(&mut self) -> Token {
		let Some(x) = self.token_buffer.first() else {
			let res = self.lexer.next_token();
			self.token_buffer.push(res);
			return res;
		};
		x
	}

	pub fn peek_kind(&mut self) -> TokenKind {
		let Some(x) = self.token_buffer.first().map(|x| x.kind) else {
			let res = self.lexer.next_token();
			self.token_buffer.push(res);
			return res.kind;
		};
		x
	}

	pub fn peek_token_at(&mut self, at: u8) -> Token {
		for _ in at..self.token_buffer.len() {
			self.token_buffer.push(self.lexer.next_token());
		}
		self.token_buffer.at(at).unwrap()
	}

	/// Returns the span of the last peeked or consumed token.
	pub fn last_span(&mut self) -> Span {
		self.token_buffer.first().map(|x| x.span).unwrap_or(self.last_span)
	}

	/// Eat the next token if it is of the given kind.
	/// Returns whether a token was eaten.
	pub fn eat(&mut self, token: TokenKind) -> bool {
		if token == self.peek().kind {
			self.token_buffer.pop();
			true
		} else {
			false
		}
	}

	fn expect_closing_delimiter(&mut self, kind: TokenKind, should_close: Span) -> ParseResult<()> {
		if !self.eat(kind) {
			return Err(ParseError::new(
				ParseErrorKind::UnclosedDelimiter {
					expected: kind,
					should_close,
				},
				self.last_span(),
			));
		}
		Ok(())
	}

	/// Recover the parser state to before a given span.
	pub fn backup_before(&mut self, span: Span) {
		self.token_buffer.clear();
		self.lexer.backup_before(span);
	}

	/// Recover the parser state to after a given span.
	pub fn backup_after(&mut self, span: Span) {
		self.token_buffer.clear();
		self.lexer.backup_after(span);
	}

	pub fn recover<Ff, Ft, R>(&mut self, to: Span, first: Ff, then: Ft) -> ParseResult<R>
	where
		Ff: FnOnce(&mut Parser) -> ParseResult<R>,
		Ft: FnOnce(&mut Parser) -> ParseResult<R>,
	{
		match first(self) {
			Ok(x) => Ok(x),
			Err(e_first) => {
				self.backup_before(to);
				match then(self) {
					Ok(x) => Ok(x),
					Err(e_then) => {
						let kind = ParseErrorKind::Retried {
							first: Box::new(e_first),
							then: Box::new(e_then),
						};
						Err(ParseError::new(kind, to))
					}
				}
			}
		}
	}

	/// Parse a full query.
	pub fn parse_query(&mut self) -> ParseResult<sql::Query> {
		let mut statements = dbg!(vec![self.parse_stmt()?]);
		while self.eat(t!(";")) {
			while self.eat(t!(";")) {}

			if let TokenKind::Eof = self.peek().kind {
				break;
			};

			statements.push(self.parse_stmt()?);
		}
		let token = self.peek();
		if TokenKind::Eof != token.kind {
			unexpected!(self, token.kind, ";");
		};
		Ok(sql::Query(sql::Statements(statements)))
	}

	/// Parse a single statement.
	pub fn parse_statement(&mut self) -> ParseResult<sql::Statement> {
		self.parse_stmt()
	}
}
