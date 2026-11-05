//! PostgreSQL utility statements parser (`SET`, `SHOW`, `RESET`, `EXPLAIN`).
//!
//! gram.y: VariableSetStmt, VariableShowStmt, VariableResetStmt, ExplainStmt

use crate::ast::stmt::{
	DeallocateStmt, ExecuteStmt, ExplainStmt, PrepareStmt, ResetStmt, SetStmt, ShowStmt,
	TruncateStmt,
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

		let is_local = self.eat(&TokenKind::Keyword(Keyword::Local))?;
		if !is_local {
			self.eat(&TokenKind::Keyword(Keyword::Session))?;
		}

		let (name, _name_span) = if self.peek().kind == TokenKind::Keyword(Keyword::Time) {
			let tok = self.bump()?;
			if self.peek().kind == TokenKind::Keyword(Keyword::Zone) {
				let zone_tok = self.bump()?;
				("timezone".to_string(), tok.span.merge(zone_tok.span))
			} else {
				("time".to_string(), tok.span)
			}
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Names) {
			let tok = self.bump()?;
			("client_encoding".to_string(), tok.span)
		} else {
			self.parse_ident()?
		};

		// TO or = (unless TIME ZONE, client_encoding, or role which can be followed directly by
		// value)
		if self.eat(&TokenKind::Keyword(Keyword::To))? || self.eat(&TokenKind::Operator("="))? {
			// eaten
		} else if !name.eq_ignore_ascii_case("timezone")
			&& !name.eq_ignore_ascii_case("client_encoding")
			&& !name.eq_ignore_ascii_case("role")
		{
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
			is_local,
			span,
		})
	}

	/// Parse a PostgreSQL `SHOW` statement.
	///
	/// gram.y: VariableShowStmt
	pub fn parse_show_stmt(&mut self) -> ParseResult<ShowStmt> {
		let show_tok = self.expect(&TokenKind::Keyword(Keyword::Show), "'SHOW'")?;
		let (name, name_span) = if self.eat(&TokenKind::Keyword(Keyword::All))? {
			("ALL".to_string(), show_tok.span)
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Time) {
			let tok = self.bump()?;
			if self.peek().kind == TokenKind::Keyword(Keyword::Zone) {
				let zone_tok = self.bump()?;
				("timezone".to_string(), tok.span.merge(zone_tok.span))
			} else {
				("time".to_string(), tok.span)
			}
		} else {
			self.parse_ident()?
		};
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
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Time) {
			let tok = self.bump()?;
			if self.peek().kind == TokenKind::Keyword(Keyword::Zone) {
				let zone_tok = self.bump()?;
				("timezone".to_string(), tok.span.merge(zone_tok.span))
			} else {
				("time".to_string(), tok.span)
			}
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
				let opt = if let TokenKind::Ident(s) = tok.kind {
					s.to_string()
				} else if let TokenKind::QuotedIdent(s) = tok.kind {
					s
				} else if let TokenKind::Keyword(kw) = tok.kind {
					kw.as_str().to_lowercase()
				} else {
					return Err(crate::error::ParseError::new(
						tok.span,
						crate::error::ErrorKind::UnexpectedToken {
							expected: "explain option name",
							found: tok.kind.to_string(),
						},
					));
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

	/// Parse a PostgreSQL `TRUNCATE` statement.
	///
	/// gram.y: TruncateStmt
	pub fn parse_truncate_stmt(&mut self) -> ParseResult<TruncateStmt> {
		let start = self.expect(&TokenKind::Keyword(Keyword::Truncate), "'TRUNCATE'")?.span;
		self.eat(&TokenKind::Keyword(Keyword::Table))?;
		self.eat(&TokenKind::Keyword(Keyword::Only))?;
		let mut tables = vec![self.parse_qualified_name()?];
		while self.eat(&TokenKind::Comma)? {
			self.eat(&TokenKind::Keyword(Keyword::Only))?;
			tables.push(self.parse_qualified_name()?);
		}
		let mut restart_identity = false;
		if self.eat(&TokenKind::Keyword(Keyword::Restart))? {
			restart_identity = true;
			self.eat(&TokenKind::Keyword(Keyword::Identity))?;
		} else if self.eat(&TokenKind::Keyword(Keyword::Continue))? {
			self.eat(&TokenKind::Keyword(Keyword::Identity))?;
		}
		let _ = self.eat(&TokenKind::Keyword(Keyword::Cascade))?;
		let _ = self.eat(&TokenKind::Keyword(Keyword::Restrict))?;
		let span = tables.iter().fold(start, |span, table| span.merge(table.span));
		Ok(TruncateStmt {
			tables,
			restart_identity,
			span,
		})
	}
}
