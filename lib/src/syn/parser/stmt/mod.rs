use crate::sql::statements::show::{ShowSince, ShowStatement};
use crate::sql::statements::sleep::SleepStatement;
use crate::sql::statements::{
	KillStatement, LiveStatement, OptionStatement, SetStatement, ThrowStatement,
};
use crate::sql::{Fields, Ident, Param, Table, Uuid};
use crate::syn::token::{t, TokenKind};
use crate::{
	sql::{
		statements::{
			analyze::AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement,
			CommitStatement, ContinueStatement, ForeachStatement, InfoStatement, OutputStatement,
			UseStatement,
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
mod remove;
mod select;
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
			t!("ANALYZE") => {
				self.pop_peek();
				self.parse_analyze().map(Statement::Analyze)
			}
			t!("BEGIN") => {
				self.pop_peek();
				self.parse_begin().map(Statement::Begin)
			}
			t!("BREAK") => {
				self.pop_peek();
				Ok(Statement::Break(BreakStatement))
			}
			t!("CANCEL") => {
				self.pop_peek();
				self.parse_cancel().map(Statement::Cancel)
			}
			t!("COMMIT") => {
				self.pop_peek();
				self.parse_commit().map(Statement::Commit)
			}
			t!("CONTINUE") => {
				self.pop_peek();
				Ok(Statement::Continue(ContinueStatement))
			}
			t!("CREATE") => {
				self.pop_peek();
				self.parse_create_stmt().map(Statement::Create)
			}
			t!("DEFINE") => {
				self.pop_peek();
				self.parse_define_stmt().map(Statement::Define)
			}
			t!("DELETE") => {
				self.pop_peek();
				self.parse_delete_stmt().map(Statement::Delete)
			}
			t!("FOR") => {
				self.pop_peek();
				self.parse_for_stmt().map(Statement::Foreach)
			}
			t!("IF") => {
				self.pop_peek();
				self.parse_if_stmt().map(Statement::Ifelse)
			}
			t!("INFO") => {
				self.pop_peek();
				self.parse_info_stmt().map(Statement::Info)
			}
			t!("INSERT") => {
				self.pop_peek();
				self.parse_insert_stmt().map(Statement::Insert)
			}
			t!("KILL") => {
				self.pop_peek();
				self.parse_kill_stmt().map(Statement::Kill)
			}
			t!("LIVE") => {
				self.pop_peek();
				self.parse_live_stmt().map(Statement::Live)
			}
			t!("OPTION") => {
				self.pop_peek();
				self.parse_option_stmt().map(Statement::Option)
			}
			t!("RETURN") => {
				self.pop_peek();
				self.parse_return_stmt().map(Statement::Output)
			}
			t!("RELATE") => {
				self.pop_peek();
				self.parse_relate_stmt().map(Statement::Relate)
			}
			t!("REMOVE") => {
				self.pop_peek();
				self.parse_remove_stmt().map(Statement::Remove)
			}
			t!("SELECT") => {
				self.pop_peek();
				self.parse_select_stmt().map(Statement::Select)
			}
			t!("LET") => {
				self.pop_peek();
				self.parse_let_stmt().map(Statement::Set)
			}
			t!("SHOW") => {
				self.pop_peek();
				self.parse_show_stmt().map(Statement::Show)
			}
			t!("SLEEP") => {
				self.pop_peek();
				self.parse_sleep_stmt().map(Statement::Sleep)
			}
			t!("THROW") => {
				self.pop_peek();
				self.parse_throw_stmt().map(Statement::Throw)
			}
			t!("UPDATE") => {
				self.pop_peek();
				self.parse_update_stmt().map(Statement::Update)
			}
			t!("USE") => {
				self.pop_peek();
				self.parse_use_stmt().map(Statement::Use)
			}
			_ => {
				// TODO: Provide information about keywords.
				let value = self.parse_value()?;
				Ok(Self::refine_stmt_value(value))
			}
		}
	}

	/// Turns [Param] `=` [Value] into a set statment.
	fn refine_stmt_value(value: Value) -> Statement {
		match value {
			Value::Expression(x) => {
				if let Expression::Binary {
					l: Value::Param(x),
					o: Operator::Equal,
					r,
				} = *x
				{
					return Statement::Set(crate::sql::statements::SetStatement {
						name: x.0 .0,
						what: r,
					});
				}
				Statement::Value(Value::Expression(x))
			}
			_ => Statement::Value(value),
		}
	}

	/// Parsers a analyze statement.
	fn parse_analyze(&mut self) -> ParseResult<AnalyzeStatement> {
		expected!(self, "INDEX");

		let index = self.parse_ident()?;
		expected!(self, "ON");
		let table = self.parse_ident()?;

		Ok(AnalyzeStatement::Idx(index, table))
	}

	fn parse_begin(&mut self) -> ParseResult<BeginStatement> {
		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(BeginStatement)
	}

	fn parse_cancel(&mut self) -> ParseResult<CancelStatement> {
		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(CancelStatement)
	}

	fn parse_commit(&mut self) -> ParseResult<CommitStatement> {
		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(CommitStatement)
	}

	fn parse_use_stmt(&mut self) -> ParseResult<UseStatement> {
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
		Ok(res)
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
				let ident = self.parse_ident()?;
				InfoStatement::Tb(ident)
			}
			t!("USER") => {
				let ident = self.parse_ident()?;
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

	pub(crate) fn parse_let_stmt(&mut self) -> ParseResult<SetStatement> {
		let name = self.parse_param()?.0 .0;
		expected!(self, "=");
		let what = self.parse_value()?;
		Ok(SetStatement {
			name,
			what,
		})
	}

	pub(crate) fn parse_show_stmt(&mut self) -> ParseResult<ShowStatement> {
		expected!(self, "CHANGES");
		expected!(self, "FOR");
		let table = match self.next().kind {
			t!("TABLE") => {
				let table = self.parse_raw_ident()?;
				Some(Table(table))
			}
			t!("DATABASE") => None,
			x => unexpected!(self, x, "`TABLE` or `DATABASE`"),
		};
		expected!(self, "SINCE");
		let since = match self.peek_kind() {
			TokenKind::Number => ShowSince::Versionstamp(self.parse_u64()?),
			// TODO: date time
			x => unexpected!(self, x, "a version stamp of date-time"),
		};

		let limit = self.eat(t!("LIMIT")).then(|| self.parse_u32()).transpose()?;

		Ok(ShowStatement {
			table,
			since,
			limit,
		})
	}

	pub(crate) fn parse_sleep_stmt(&mut self) -> ParseResult<SleepStatement> {
		let duration = self.parse_duration()?;
		Ok(SleepStatement {
			duration,
		})
	}

	pub(crate) fn parse_throw_stmt(&mut self) -> ParseResult<ThrowStatement> {
		let error = self.parse_value()?;
		Ok(ThrowStatement {
			error,
		})
	}
}
