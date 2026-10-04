//! PostgreSQL utility statements parser (`SET`, `SHOW`, `RESET`, `EXPLAIN`).
//!
//! gram.y: VariableSetStmt, VariableShowStmt, VariableResetStmt, ExplainStmt

use crate::ast::stmt::{
	DeallocateStmt, ExecuteStmt, ExplainStmt, PrepareStmt, ResetStmt, SetStmt, ShowStmt,
};
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

		// Reject SET LOCAL
		if self.eat(&TokenKind::Keyword(Keyword::Local))? {
			return Err(crate::error::ParseError::new(
				set_tok.span,
				crate::error::ErrorKind::InvalidStatement("SET LOCAL is not supported"),
			));
		}
		// Optional SESSION
		self.eat(&TokenKind::Keyword(Keyword::Session))?;

		let (name, _name_span) = self.parse_ident()?;

		// TO or =
		if !self.eat(&TokenKind::Keyword(Keyword::To))? {
			self.expect(&TokenKind::Operator("="), "'TO' or '='")?;
		}

		// Read value tokens strictly without swallowing next statements
		let start_val_span = self.peek().span;
		let mut end_val_span = start_val_span;

		let mut has_val = false;
		while let TokenKind::Ident(_)
		| TokenKind::Keyword(_)
		| TokenKind::StringConstant(_)
		| TokenKind::Integer(_)
		| TokenKind::IntegerRaw(_)
		| TokenKind::Float(_)
		| TokenKind::FloatRaw(_) = &self.peek().kind
		{
			let tok = self.bump()?;
			end_val_span = tok.span;
			has_val = true;
			if self.peek().kind == TokenKind::Comma {
				let tok = self.bump()?;
				end_val_span = tok.span;
			} else {
				break;
			}
		}

		if !has_val {
			return Err(crate::error::ParseError::new(
				self.peek().span,
				crate::error::ErrorKind::UnexpectedToken {
					expected: "variable value",
					found: self.peek().kind.to_string(),
				},
			));
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

	/// Parse a PostgreSQL `PREPARE` statement.
	///
	/// gram.y: PrepareStmt
	pub fn parse_prepare_stmt(&mut self) -> ParseResult<PrepareStmt> {
		let prep_tok = self.expect(&TokenKind::Keyword(Keyword::Prepare), "'PREPARE'")?;
		let (name, _) = self.parse_ident()?;

		let mut param_types = Vec::new();
		if self.eat(&TokenKind::LParen)? {
			loop {
				param_types.push(self.parse_type_name()?);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
		}

		self.expect(&TokenKind::Keyword(Keyword::As), "'AS'")?;
		let statement = Box::new(self.parse_statement()?);
		let span = prep_tok.span.merge(statement.span());

		Ok(PrepareStmt {
			name,
			param_types,
			statement,
			span,
		})
	}

	/// Parse a PostgreSQL `EXECUTE` statement.
	///
	/// gram.y: ExecuteStmt
	pub fn parse_execute_stmt(&mut self) -> ParseResult<ExecuteStmt> {
		let exec_tok = self.expect(&TokenKind::Keyword(Keyword::Execute), "'EXECUTE'")?;
		let (name, name_span) = self.parse_ident()?;

		let mut params = Vec::new();
		let mut end_span = name_span;
		if self.eat(&TokenKind::LParen)? {
			loop {
				params.push(self.parse_expr()?);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			let rparen = self.expect(&TokenKind::RParen, "')'")?;
			end_span = rparen.span;
		}

		let span = exec_tok.span.merge(end_span);
		Ok(ExecuteStmt {
			name,
			params,
			span,
		})
	}

	/// Parse a PostgreSQL `DEALLOCATE` statement.
	///
	/// gram.y: DeallocateStmt
	pub fn parse_deallocate_stmt(&mut self) -> ParseResult<DeallocateStmt> {
		let dealloc_tok = self.expect(&TokenKind::Keyword(Keyword::Deallocate), "'DEALLOCATE'")?;
		self.eat(&TokenKind::Keyword(Keyword::Prepare))?;

		let (name, end_span) = if self.eat(&TokenKind::Keyword(Keyword::All))? {
			(None, self.peek().span)
		} else {
			let (n, nspan) = self.parse_ident()?;
			(Some(n), nspan)
		};

		let span = dealloc_tok.span.merge(end_span);
		Ok(DeallocateStmt {
			name,
			span,
		})
	}
}
