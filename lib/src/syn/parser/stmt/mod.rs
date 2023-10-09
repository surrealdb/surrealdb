use crate::sql::{
	statements::{
		analyze::AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement,
		CommitStatement, ContinueStatement, DefineStatement, DeleteStatement, IfelseStatement,
		InsertStatement, OutputStatement, RelateStatement, RemoveStatement, SelectStatement,
		UpdateStatement, UseStatement,
	},
	Statement,
};
use crate::syn::token::t;

use super::{
	mac::{expected, to_do},
	ParseResult, Parser,
};

mod create;

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
			t!("CREATE") => self.parse_create_stmt().map(Statement::Create),
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

	pub(crate) fn parse_if_stmt(&mut self) -> ParseResult<IfelseStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_return_stmt(&mut self) -> ParseResult<OutputStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_select_stmt(&mut self) -> ParseResult<SelectStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_update_stmt(&mut self) -> ParseResult<UpdateStatement> {
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

	pub(crate) fn parse_define_stmt(&mut self) -> ParseResult<DefineStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_remove_stmt(&mut self) -> ParseResult<RemoveStatement> {
		to_do!(self)
	}
}
