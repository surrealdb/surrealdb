//! PostgreSQL utility statements parser (`SET`, `SHOW`, `RESET`, `EXPLAIN`).
//!
//! gram.y: VariableSetStmt, VariableShowStmt, VariableResetStmt, ExplainStmt

use crate::ast::stmt::{ExplainStmt, ResetStmt, SetStmt, ShowStmt};
use crate::error::ParseResult;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL `SET` statement.
	///
	/// gram.y: VariableSetStmt
	pub fn parse_set_stmt(&mut self) -> ParseResult<SetStmt> {
		let set_tok = self.expect(&TokenKind::Keyword(Keyword::Set), "'SET'")?;

		// Optional SESSION or LOCAL
		self.eat(&TokenKind::Keyword(Keyword::Session))?;
		self.eat(&TokenKind::Keyword(Keyword::Local))?;

		let (name, _name_span) = self.parse_ident()?;

		// TO or =
		if !self.eat(&TokenKind::Keyword(Keyword::To))? {
			self.expect(&TokenKind::Operator("="), "'TO' or '='")?;
		}

		// Read value until semicolon or EOF
		let start_val_span = self.peek().span;
		let mut end_val_span = start_val_span;

		while self.peek().kind != TokenKind::Semi && !self.peek().is_eof() {
			let tok = self.bump()?;
			end_val_span = tok.span;
		}

		let val_span = start_val_span.merge(end_val_span);
		let value = val_span.slice(self.src()).unwrap_or("").trim().to_string();
		let span = set_tok.span.merge(end_val_span);

		Ok(SetStmt {
			name,
			value,
			span,
		})
	}

	/// Parse a PostgreSQL `SHOW` statement.
	///
	/// gram.y: VariableShowStmt
	pub fn parse_show_stmt(&mut self) -> ParseResult<ShowStmt> {
		let show_tok = self.expect(&TokenKind::Keyword(Keyword::Show), "'SHOW'")?;
		let (name, name_span) = self.parse_ident()?;
		let span = show_tok.span.merge(name_span);

		Ok(ShowStmt {
			name,
			span,
		})
	}

	/// Parse a PostgreSQL `RESET` statement.
	///
	/// gram.y: VariableResetStmt
	pub fn parse_reset_stmt(&mut self) -> ParseResult<ResetStmt> {
		let reset_tok = self.expect(&TokenKind::Keyword(Keyword::Reset), "'RESET'")?;

		let (name, name_span) = if self.eat(&TokenKind::Keyword(Keyword::All))? {
			("ALL".to_string(), reset_tok.span)
		} else {
			self.parse_ident()?
		};

		let span = reset_tok.span.merge(name_span);

		Ok(ResetStmt {
			name,
			span,
		})
	}

	/// Parse a PostgreSQL `EXPLAIN` statement.
	///
	/// gram.y: ExplainStmt
	pub fn parse_explain_stmt(&mut self) -> ParseResult<ExplainStmt> {
		let explain_tok = self.expect(&TokenKind::Keyword(Keyword::Explain), "'EXPLAIN'")?;

		// Optional options in parentheses e.g. `(ANALYZE, VERBOSE, BUFFERS)`
		let mut options = Vec::new();
		if self.eat(&TokenKind::LParen)? {
			while self.peek().kind != TokenKind::RParen {
				let tok = self.bump()?;
				let opt = match tok.kind {
					TokenKind::Ident(s) => s.to_string(),
					TokenKind::QuotedIdent(s) => s,
					TokenKind::Keyword(kw) => kw.as_str().to_lowercase(),
					_ => {
						return Err(crate::error::ParseError::new(
							tok.span,
							crate::error::ErrorKind::UnexpectedToken {
								expected: "explain option name",
								found: tok.kind.to_string(),
							},
						));
					}
				};
				options.push(opt);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		// Query being explained
		let statement = Box::new(self.parse_statement()?);
		let span = explain_tok.span.merge(statement.span());

		Ok(ExplainStmt {
			options,
			statement,
			span,
		})
	}
}
