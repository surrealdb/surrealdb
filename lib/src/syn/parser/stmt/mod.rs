use crate::sql::{
	statements::{
		analyze::AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement,
		CommitStatement, ContinueStatement, DeleteStatement, IfelseStatement, InsertStatement,
		OutputStatement, RelateStatement, RemoveStatement, SelectStatement, UseStatement,
	},
	Expression, Operator, Statement, Statements, Value,
};
use crate::syn::token::t;

use super::{
	mac::{expected, to_do},
	ParseResult, Parser,
};

mod create;
mod define;
mod parts;
mod update;

impl Parser<'_> {
	pub fn parse_stmt_list(&mut self) -> ParseResult<Statements> {
		let mut res = Vec::new();
		loop {
			match self.peek_kind() {
				t!(";") => continue,
				t!("eof") => break,
				_ => {
					let stmt = self.parse_stmt()?;
					res.push(stmt);
					if !self.eat(t!(";")) {
						expected!(self, "eof");
						break;
					}
				}
			}
		}
		Ok(Statements(res))
	}

	pub(super) fn parse_stmt(&mut self) -> ParseResult<Statement> {
		let token = self.peek();
		match token.kind {
			t!("ANALYZE") => self.parse_analyze(),
			t!("BEGIN") => self.parse_begin(),
			t!("BREAK") => self.parse_break(),
			t!("CANCEL") => self.parse_cancel(),
			t!("COMMIT") => self.parse_commit(),
			t!("CONTINUE") => self.parse_continue(),
			t!("CREATE") => self.parse_create_stmt().map(Statement::Create),
			t!("DEFINE") => self.parse_define_stmt().map(Statement::Define),
			t!("DELETE") => self.parse_delete_stmt().map(Statement::Delete),
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
			t!("UPDATE") => self.parse_update_stmt().map(Statement::Update),
			t!("USE") => self.parse_use(),
			_ => {
				// TODO: Provide information about keywords.
				let value = self.parse_value()?;
				return Ok(Self::refine_stmt_value(value));
			}
		}
	}

	/// Turns [Param] `=` [Value] into a set statment.
	fn refine_stmt_value(value: Value) -> Statement {
		match value {
			Value::Expression(x) => {
				match *x {
					Expression::Binary {
						l: Value::Param(x),
						o: Operator::Equal,
						r,
					} => {
						return Statement::Set(crate::sql::statements::SetStatement {
							name: x.0 .0,
							what: r,
						})
					}
					x => {}
				}
				Statement::Value(Value::Expression(x))
			}
			x => Statement::Value(value),
		}
	}

	/// Parsers a analyze statement.
	fn parse_analyze(&mut self) -> ParseResult<Statement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("ANALYZE"));

		expected!(self, "INDEX");

		let index = self.parse_ident()?;
		expected!(self, "ON");
		let table = self.parse_ident()?;

		let res = AnalyzeStatement::Idx(index, table);
		Ok(Statement::Analyze(res))
	}

	fn parse_begin(&mut self) -> ParseResult<Statement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("BEGIN"));

		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(Statement::Begin(BeginStatement))
	}

	fn parse_break(&mut self) -> ParseResult<Statement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("BREAK"));

		Ok(Statement::Break(BreakStatement))
	}

	fn parse_cancel(&mut self) -> ParseResult<Statement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("CANCEL"));

		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(Statement::Cancel(CancelStatement))
	}

	fn parse_commit(&mut self) -> ParseResult<Statement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("COMMIT"));

		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(Statement::Commit(CommitStatement))
	}

	fn parse_continue(&mut self) -> ParseResult<Statement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("CONTINUE"));

		Ok(Statement::Continue(ContinueStatement))
	}

	fn parse_use(&mut self) -> ParseResult<Statement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("USE"));

		let (ns, db) = if self.eat(t!("NAMESPACE")) {
			let ns = self.parse_ident()?;

			let db = self.eat(t!("DATABASE")).then(|| self.parse_ident()).transpose()?;
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

	pub(crate) fn parse_if_stmt(&mut self) -> ParseResult<IfelseStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_return_stmt(&mut self) -> ParseResult<OutputStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_select_stmt(&mut self) -> ParseResult<SelectStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_delete_stmt(&mut self) -> ParseResult<DeleteStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_relate_stmt(&mut self) -> ParseResult<RelateStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_insert_stmt(&mut self) -> ParseResult<InsertStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_remove_stmt(&mut self) -> ParseResult<RemoveStatement> {
		to_do!(self)
	}
}
