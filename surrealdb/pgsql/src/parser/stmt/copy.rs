//! PostgreSQL `COPY` statement parser.
//!
//! gram.y: CopyStmt

use crate::ast::stmt::{CopyDirection, CopyFormat, CopyOptions, CopyStmt, CopyTarget};
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL `COPY` statement.
	///
	/// gram.y: CopyStmt
	pub fn parse_copy_stmt(&mut self) -> ParseResult<CopyStmt> {
		let copy_tok = self.expect(&TokenKind::Keyword(Keyword::Copy), "'COPY'")?;
		let start_span = copy_tok.span;

		// 1. Target: either `(query)` or `table [(columns)]`
		let target = if self.peek().kind == TokenKind::LParen {
			self.bump()?;
			let query = self.parse_select_stmt()?;
			self.expect(&TokenKind::RParen, "')'")?;
			CopyTarget::Query(Box::new(query))
		} else {
			let table = self.parse_qualified_name()?;
			let columns = if self.eat(&TokenKind::LParen)? {
				let mut cols = Vec::new();
				loop {
					let (col_name, _) = self.parse_ident()?;
					cols.push(col_name);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')'")?;
				cols
			} else {
				Vec::new()
			};
			CopyTarget::Table {
				table,
				columns,
			}
		};

		// 2. Direction: `FROM STDIN` or `TO STDOUT`
		let direction = if self.eat(&TokenKind::Keyword(Keyword::From))? {
			self.expect(&TokenKind::Keyword(Keyword::Stdin), "'STDIN'")?;
			CopyDirection::FromStdin
		} else if self.eat(&TokenKind::Keyword(Keyword::To))? {
			self.expect(&TokenKind::Keyword(Keyword::Stdout), "'STDOUT'")?;
			CopyDirection::ToStdout
		} else {
			return Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "'FROM' or 'TO'",
					found: self.peek().kind.to_string(),
				},
			));
		};

		// 3. Options
		let mut options = CopyOptions::default();
		if self.eat(&TokenKind::Keyword(Keyword::With))? {
			if self.eat(&TokenKind::LParen)? {
				self.parse_copy_paren_options(&mut options)?;
			} else {
				self.parse_copy_legacy_options(&mut options)?;
			}
		} else if self.eat(&TokenKind::LParen)? {
			self.parse_copy_paren_options(&mut options)?;
		} else {
			self.parse_copy_legacy_options(&mut options)?;
		}

		let end_span = self.peek().span;
		let span = start_span.merge(end_span);

		Ok(CopyStmt {
			target,
			direction,
			options,
			span,
		})
	}

	fn parse_copy_paren_options(&mut self, options: &mut CopyOptions) -> ParseResult<()> {
		loop {
			if self.peek().kind == TokenKind::RParen {
				break;
			}
			let opt_name = self.parse_copy_opt_name()?;
			match opt_name.to_ascii_lowercase().as_str() {
				"format" => {
					let fmt_name = self.parse_copy_opt_name()?;
					match fmt_name.to_ascii_lowercase().as_str() {
						"csv" => options.format = CopyFormat::Csv,
						"text" => options.format = CopyFormat::Text,
						"binary" => options.format = CopyFormat::Binary,
						other => {
							return Err(ParseError::new(
								self.peek().span,
								ErrorKind::UnexpectedToken {
									expected: "valid COPY format ('CSV', 'TEXT', 'BINARY')",
									found: other.to_string(),
								},
							));
						}
					}
				}
				"delimiter" => {
					let s = self.parse_string_lit()?;
					if let Some(&b) = s.as_bytes().first() {
						options.delimiter = Some(b);
					}
				}
				"null" => {
					let s = self.parse_string_lit()?;
					options.null = Some(s);
				}
				"header" => {
					if self.peek().kind == TokenKind::Keyword(Keyword::True)
						|| self.peek().kind == TokenKind::Keyword(Keyword::False)
					{
						let b = self.eat(&TokenKind::Keyword(Keyword::True))?;
						options.header = Some(b);
					} else if let Ok((s, _)) = self.parse_ident() {
						match s.to_ascii_lowercase().as_str() {
							"true" | "on" | "1" => options.header = Some(true),
							"false" | "off" | "0" => options.header = Some(false),
							_ => options.header = Some(true),
						}
					} else {
						options.header = Some(true);
					}
				}
				"quote" => {
					let s = self.parse_string_lit()?;
					if let Some(&b) = s.as_bytes().first() {
						options.quote = Some(b);
					}
				}
				"escape" => {
					let s = self.parse_string_lit()?;
					if let Some(&b) = s.as_bytes().first() {
						options.escape = Some(b);
					}
				}
				_ => {
					// An option the server has no use for (e.g. `FREEZE`) is read with its value
					// and ignored.
					self.eat(&TokenKind::Operator("="))?;
					if !matches!(self.peek().kind, TokenKind::Comma | TokenKind::RParen) {
						self.bump()?;
					}
				}
			}
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}
		self.expect(&TokenKind::RParen, "')'")?;
		Ok(())
	}

	fn parse_copy_legacy_options(&mut self, options: &mut CopyOptions) -> ParseResult<()> {
		loop {
			if self.peek().kind == TokenKind::Keyword(Keyword::Csv) {
				self.bump()?;
				options.format = CopyFormat::Csv;
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Binary) {
				self.bump()?;
				options.format = CopyFormat::Binary;
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Header) {
				self.bump()?;
				options.header = Some(true);
			} else if let TokenKind::Keyword(Keyword::Delimiter | Keyword::Delimiters) =
				self.peek().kind
			{
				self.bump()?;
				let _ = self.eat(&TokenKind::Keyword(Keyword::As));
				let s = self.parse_string_lit()?;
				if let Some(&b) = s.as_bytes().first() {
					options.delimiter = Some(b);
				}
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Null) {
				self.bump()?;
				let _ = self.eat(&TokenKind::Keyword(Keyword::As));
				let s = self.parse_string_lit()?;
				options.null = Some(s);
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Quote) {
				self.bump()?;
				let _ = self.eat(&TokenKind::Keyword(Keyword::As));
				let s = self.parse_string_lit()?;
				if let Some(&b) = s.as_bytes().first() {
					options.quote = Some(b);
				}
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Escape) {
				self.bump()?;
				let _ = self.eat(&TokenKind::Keyword(Keyword::As));
				let s = self.parse_string_lit()?;
				if let Some(&b) = s.as_bytes().first() {
					options.escape = Some(b);
				}
			} else {
				break;
			}
		}
		Ok(())
	}

	fn parse_string_lit(&mut self) -> ParseResult<String> {
		if let TokenKind::StringConstant(s) = &self.peek().kind {
			let s = s.clone();
			self.bump()?;
			Ok(s)
		} else {
			Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "string literal",
					found: self.peek().kind.to_string(),
				},
			))
		}
	}

	fn parse_copy_opt_name(&mut self) -> ParseResult<String> {
		if let TokenKind::Ident(_) = self.peek().kind {
			Ok(self.parse_ident()?.0)
		} else if let TokenKind::Keyword(kw) = self.peek().kind {
			self.bump()?;
			Ok(kw.as_str().to_string())
		} else {
			Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "option name",
					found: self.peek().kind.to_string(),
				},
			))
		}
	}
}
