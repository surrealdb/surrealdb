use crate::sql::statements::{KillStatement, LiveStatement, OptionStatement};
use crate::sql::{Fields, Ident, Param, Table, Uuid};
use crate::syn::token::{t, TokenKind};
use crate::{
	sql::{
		statements::{
			analyze::AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement,
			CommitStatement, ContinueStatement, ForeachStatement, InfoStatement, OutputStatement,
			RelateStatement, RemoveStatement, SelectStatement, UseStatement,
		},
		Expression, Operator, Statement, Statements, Value,
	},
	syn::parser::mac::unexpected,
};

use super::{
	mac::{expected, to_do},
	ParseResult, Parser,
};

mod create;
mod define;
mod delete;
mod r#if;
mod insert;
mod parts;
mod relate;
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
			t!("FOR") => self.parse_for_stmt().map(Statement::Foreach),
			t!("IF") => self.parse_if_stmt().map(Statement::Ifelse),
			t!("INFO") => self.parse_info_stmt().map(Statement::Info),
			t!("INSERT") => self.parse_insert_stmt().map(Statement::Insert),
			t!("KILL") => self.parse_kill_stmt().map(Statement::Kill),
			t!("LIVE") => self.parse_live_stmt().map(Statement::Live),
			t!("OPTION") => self.parse_option_stmt().map(Statement::Option),
			t!("RETURN") => self.parse_return_stmt().map(Statement::Output),
			t!("RELATE") => self.parse_relate_stmt().map(Statement::Relate),
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

	pub fn parse_for_stmt(&mut self) -> ParseResult<ForeachStatement> {
		let param = self.parse_param()?;
		expected!(self, "IN");
		let range = self.parse_value()?;

		let span = expected!(self, "{").span;
		let block = self.parse_block(span)?;
		Ok(ForeachStatement {
			param,
			range,
			block,
		})
	}

	pub(crate) fn parse_info_stmt(&mut self) -> ParseResult<InfoStatement> {
		expected!(self, "FOR");
		let stmt = match self.next().kind {
			t!("ROOT") => InfoStatement::Root,
			t!("NAMESPACE") => InfoStatement::Ns,
			t!("DATABASE") => InfoStatement::Db,
			t!("SCOPE") => {
				let ident = self.parse_ident()?;
				InfoStatement::Sc(ident)
			}
			t!("TABLE") => {
				let ident = self.parse_table()?;
				InfoStatement::Tb(ident)
			}
			t!("USER") => {
				let ident = self.parse_table()?;
				let base = self.eat(t!("ON")).then(|| self.parse_base(false)).transpose()?;
				InfoStatement::User(ident, base)
			}
			x => unexpected!(self, x, "an info target"),
		};
		Ok(stmt)
	}

	pub(crate) fn parse_kill_stmt(&mut self) -> ParseResult<KillStatement> {
		let id = match self.peek().kind {
			TokenKind::Uuid => {
				to_do!(self)
			}
			t!("$param") => {
				let token = self.pop_peek();
				let param =
					self.lexer.strings[u32::from(token.data_index.unwrap()) as usize].clone();
				Value::Param(Param(Ident(param)))
			}
			x => unexpected!(self, x, "a UUID or a parameter"),
		};
		Ok(KillStatement {
			id,
		})
	}

	pub(crate) fn parse_live_stmt(&mut self) -> ParseResult<LiveStatement> {
		expected!(self, "SELECT");
		let expr = match self.peek().kind {
			t!("DIFF") => {
				self.pop_peek();
				Fields::default()
			}
			_ => self.parse_fields()?,
		};
		expected!(self, "FROM");
		let what = match self.peek().kind {
			t!("$param") => Value::Param(self.parse_param()?),
			_ => Value::Table(Table(self.parse_raw_ident()?)),
		};
		let cond = self.try_parse_condition()?;
		let fetch = self.try_parse_fetch()?;

		Ok(LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			what,
			cond,
			fetch,
			..Default::default()
		})
	}

	pub(crate) fn parse_option_stmt(&mut self) -> ParseResult<OptionStatement> {
		let name = self.parse_ident()?;
		expected!(self, "=");
		let what = match self.next().kind {
			t!("true") => true,
			t!("false") => true,
			x => unexpected!(self, x, "either 'true' or 'false'"),
		};
		Ok(OptionStatement {
			name,
			what,
		})
	}

	pub(crate) fn parse_return_stmt(&mut self) -> ParseResult<OutputStatement> {
		let what = self.parse_value()?;
		let fetch = self.try_parse_fetch()?;
		Ok(OutputStatement {
			what,
			fetch,
		})
	}

	pub(crate) fn parse_select_stmt(&mut self) -> ParseResult<SelectStatement> {
		to_do!(self)
	}

	pub(crate) fn parse_remove_stmt(&mut self) -> ParseResult<RemoveStatement> {
		to_do!(self)
	}
}
