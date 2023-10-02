use crate::{
	sql,
	syn::{
		lexer::Lexer,
		parser::mac::unexpected,
		token::{t, Span, Token, TokenKind},
	},
};

mod idiom;
mod mac;
mod stmt;
mod value;

#[derive(Debug, Clone, Copy)]
pub struct Checkpoint(usize);

#[derive(Debug, Clone, Copy)]
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
	/// The parser encountered an token which could not be lexed correctly.
	InvalidToken,
	/// A path in the parser which was not yet finished.
	/// Should eventually be removed.
	Todo,
}

#[derive(Debug, Clone, Copy)]
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

	/// Returns the next token.
	pub fn next_token(&mut self) -> Token {
		let res = self.peek.take().unwrap_or_else(|| self.lexer.next_token());
		self.last_span = res.span;
		dbg!(res)
	}

	/// Returns the next token without consuming it.
	pub fn peek_token(&mut self) -> Token {
		self.peek.get_or_insert_with(|| self.lexer.next_token()).clone()
	}

	/// Returns the span of the last peeked or consumed token.
	pub fn last_span(&mut self) -> Span {
		self.peek.as_ref().map(|x| x.span).unwrap_or(self.last_span)
	}

	/// Eat the next token if it is of the given kind.
	/// Returns whether a token was eaten.
	pub fn eat(&mut self, token: TokenKind) -> bool {
		if token == self.peek_token().kind {
			self.peek = None;
			true
		} else {
			false
		}
	}

	/// Returns a checkpoint of the lexer state.
	/// The checkpoint can be pased into recover to return the parser to the given state.
	pub fn checkpoint(&self) -> Checkpoint {
		Checkpoint(self.lexer.reader.offset())
	}

	/// Recover the lexer to a given checkpoint.
	/// Will reset the current peeked value.
	pub fn recover(&mut self, checkpoint: Checkpoint) {
		self.peek = None;
		self.lexer.reader.backup(checkpoint.0)
	}

	pub fn into_remaining(self) -> &'a str {
		todo!()
	}

	/// Parse a full query.
	pub fn parse_query(&mut self) -> ParseResult<sql::Query> {
		let mut statements = dbg!(vec![self.parse_stmt()?]);
		while self.eat(t!(";")) {
			while self.eat(t!(";")) {}

			if let TokenKind::Eof = self.peek_token().kind {
				break;
			};

			statements.push(self.parse_stmt()?);
		}
		let token = self.peek_token();
		let TokenKind::Eof = token.kind else {
			unexpected!(self, token.kind, ";");
		};
		Ok(sql::Query(sql::Statements(statements)))
	}

	/// Parse a single statement.
	pub fn parse_statement(&mut self) -> ParseResult<sql::Statement> {
		self.parse_stmt()
	}
}
