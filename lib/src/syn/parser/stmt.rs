use crate::sql::{
	output::Output,
	statements::{
		analyze::AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement,
		CommitStatement, ContinueStatement, CreateStatement, UseStatement,
	},
	Data, Ident, Operator, Statement,
};
use crate::syn::{
	parser::mac::expected,
	token::{t, TokenKind},
};

use super::{
	mac::{to_do, unexpected},
	ParseResult, Parser,
};

impl Parser<'_> {
	pub(super) fn parse_stmt(&mut self) -> ParseResult<Statement> {
		let token = self.peek_token();
		match token.kind {
			t!("ANALYZE") => self.parse_analyze(),
			t!("BEGIN") => self.parse_begin(),
			t!("BREAK") => self.parse_break(),
			t!("CANCEL") => self.parse_cancel(),
			t!("COMMIT") => self.parse_commit(),
			t!("CONTINUE") => self.parse_continue(),
			t!("CREATE") => self.parse_create(),
			t!("DEFINE") => self.parse_begin(),
			t!("DELETE") => self.parse_begin(),
			t!("FOR") => self.parse_begin(),
			t!("IF") => self.parse_begin(),
			t!("INFO") => self.parse_begin(),
			t!("INSERT") => self.parse_begin(),
			t!("KILL") => self.parse_begin(),
			t!("LIVE") => self.parse_begin(),
			t!("OPTION") => self.parse_begin(),
			t!("RETURN") => self.parse_begin(),
			t!("RELATE") => self.parse_begin(),
			t!("REMOVE") => self.parse_begin(),
			t!("SELECT") => self.parse_begin(),
			t!("LET") => self.parse_begin(),
			t!("SHOW") => self.parse_begin(),
			t!("SLEEP") => self.parse_begin(),
			t!("THROW") => self.parse_begin(),
			t!("UPDATE") => self.parse_begin(),
			t!("USE") => self.parse_use(),
			_ => to_do!(self),
		}
	}

	/// Parsers a analyze statement.
	fn parse_analyze(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("ANALYZE"));

		let index = self.peek_token();
		let t!("INDEX") = index.kind else {
			// Failed to parse next keyword, might be a value.
			// TODO: Check the token could continue a value statement?
			// Possibly check for some form of operator.
			let value = self.parse_fallback_value(keyword)?;
			return Ok(Statement::Value(value));
		};
		self.next_token();

		let index = self.parse_ident()?;
		expected!(self, "ON");
		let table = self.parse_ident()?;

		let res = AnalyzeStatement::Idx(index, table);
		Ok(Statement::Analyze(res))
	}

	fn parse_begin(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("BEGIN"));

		if let t!("TRANSACTION") = self.peek_token().kind {
			self.next_token();
		}
		Ok(Statement::Begin(BeginStatement))
	}

	fn parse_break(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("BREAK"));

		Ok(Statement::Break(BreakStatement))
	}

	fn parse_cancel(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("CANCEL"));

		if let t!("TRANSACTION") = self.peek_token().kind {
			self.next_token();
		}
		Ok(Statement::Cancel(CancelStatement))
	}

	fn parse_commit(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("COMMIT"));

		if let t!("TRANSACTION") = self.peek_token().kind {
			self.next_token();
		}
		Ok(Statement::Commit(CommitStatement))
	}

	fn parse_continue(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("CONTINUE"));

		Ok(Statement::Continue(ContinueStatement))
	}

	fn parse_create(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("CREATE"));

		let only = if let t!("ONLY") = self.peek_token().kind {
			self.next_token();
			true
		} else {
			false
		};

		let what = self.parse_whats()?;

		let data = match self.peek_token().kind {
			t!("SET") => {
				self.next_token();
				let mut res = Vec::new();
				loop {
					let idiom = self.parse_plain_idiom()?;
					let operator = match self.next_token().kind {
						t!("=") => Operator::Equal,
						t!("+=") => Operator::Inc,
						t!("-=") => Operator::Dec,
						t!("+?=") => Operator::Ext,
						x => unexpected!(self, x, "an assignment operator"),
					};
					let value = self.parse_value()?;
					res.push((idiom, operator, value));
					if !self.eat(t!(",")) {
						break;
					}
				}

				Some(Data::SetExpression(res))
			}
			t!("UNSET") => {
				self.next_token();
				let mut res = Vec::new();
				loop {
					let idiom = self.parse_plain_idiom()?;
					res.push(idiom);
					if !self.eat(t!(",")) {
						break;
					}
				}

				Some(Data::UnsetExpression(res))
			}
			t!("PATCH") => {
				self.next_token();
				let value = self.parse_value()?;
				Some(Data::PatchExpression(value))
			}
			t!("MERGE") => {
				self.next_token();
				let value = self.parse_value()?;
				Some(Data::MergeExpression(value))
			}
			t!("REPLACE") => {
				self.next_token();
				let value = self.parse_value()?;
				Some(Data::ReplaceExpression(value))
			}
			t!("CONTENT") => {
				self.next_token();
				let value = self.parse_value()?;
				Some(Data::ContentExpression(value))
			}
			_ => None,
		};

		let output = if self.eat(t!("RETURN")) {
			let output = match self.next_token().kind {
				t!("NONE") => Output::None,
				t!("NULL") => Output::Null,
				t!("DIFF") => Output::Diff,
				t!("AFTER") => Output::After,
				t!("BEFORE") => Output::Before,
				// TODO: Field
				x => unexpected!(self, x, "an output"),
			};
			Some(output)
		} else {
			None
		};

		let timeout = if self.eat(t!("TIMEOUT")) {
			to_do!(self)
		} else {
			None
		};

		let parallel = self.eat(t!("PARALLEL"));
		let res = CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
			parallel,
		};
		Ok(Statement::Create(res))
	}

	fn parse_use(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("USE"));

		let (ns, db) = if self.eat(t!("NAMESPACE")) {
			let ns = self.parse_ident()?;

			let db = if self.eat(t!("DATABASE")) {
				Some(self.parse_ident()?)
			} else {
				None
			};
			(Some(ns), db)
		} else {
			expected!(self, "DATABASE");

			let db = self.parse_ident()?;
			(None, Some(db))
		};

		let res = UseStatement {
			ns: ns.map(|x| x.0),
			db: db.map(|x| x.0),
		};
		Ok(Statement::Use(res))
	}

	pub fn parse_ident(&mut self) -> ParseResult<Ident> {
		self.parse_raw_ident().map(Ident)
	}

	pub fn parse_raw_ident(&mut self) -> ParseResult<String> {
		let token = self.next_token();
		match token.kind {
			TokenKind::Keyword(_) | TokenKind::Number => {
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(str)
			}
			TokenKind::Identifier => {
				let data_index = token.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				let str = self.lexer.strings[idx].clone();
				Ok(str)
			}
			x => {
				unexpected!(self, x, "a identifier");
			}
		}
	}
}
