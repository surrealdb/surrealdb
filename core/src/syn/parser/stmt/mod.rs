use reblessive::Stk;

use crate::cnf::EXPERIMENTAL_BEARER_ACCESS;
use crate::sql::block::Entry;
use crate::sql::statements::rebuild::{RebuildIndexStatement, RebuildStatement};
use crate::sql::statements::show::{ShowSince, ShowStatement};
use crate::sql::statements::sleep::SleepStatement;
use crate::sql::statements::{
	access::{
		AccessStatement, AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke,
		AccessStatementShow, Subject,
	},
	KillStatement, LiveStatement, OptionStatement, SetStatement, ThrowStatement,
};
use crate::sql::{Duration, Fields, Ident, Param};
use crate::syn::lexer::compound;
use crate::syn::parser::enter_query_recursion;
use crate::syn::token::{t, Glued, TokenKind};
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

use super::{mac::expected, ParseResult, Parser};

mod alter;
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
mod upsert;

impl Parser<'_> {
	pub(super) async fn parse_stmt_list(&mut self, ctx: &mut Stk) -> ParseResult<Statements> {
		let mut res = Vec::new();
		loop {
			match self.peek_kind() {
				// consume any possible empty statements.
				t!(";") => {
					self.pop_peek();
					continue;
				}
				t!("eof") => break,
				_ => {
					let stmt = ctx.run(|ctx| self.parse_stmt(ctx)).await?;
					res.push(stmt);
					if !self.eat(t!(";")) {
						if self.eat(t!("eof")) {
							break;
						}

						let token = self.peek();
						if Self::kind_starts_statement(token.kind) {
							// user likely forgot a semicolon.
							unexpected!(self,token,"the query to end", => "maybe forgot a semicolon  after the previous statement?");
						}

						expected!(self, t!("eof"));
					}
				}
			}
		}
		Ok(Statements(res))
	}

	pub(super) async fn parse_stmt(&mut self, ctx: &mut Stk) -> ParseResult<Statement> {
		enter_query_recursion!(this = self => {
			this.parse_stmt_inner(ctx).await
		})
	}

	async fn parse_stmt_inner(&mut self, ctx: &mut Stk) -> ParseResult<Statement> {
		let token = self.peek();
		match token.kind {
			t!("ACCESS") => {
				// TODO(gguillemas): Remove this once bearer access is no longer experimental.
				if !*EXPERIMENTAL_BEARER_ACCESS {
					unexpected!(
						self,
						token,
						"the experimental bearer access feature to be enabled"
					);
				}
				self.pop_peek();
				ctx.run(|ctx| self.parse_access(ctx)).await.map(Statement::Access)
			}
			t!("ALTER") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_alter_stmt(ctx)).await.map(Statement::Alter)
			}
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
				ctx.run(|ctx| self.parse_create_stmt(ctx)).await.map(Statement::Create)
			}
			t!("DEFINE") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_define_stmt(ctx)).await.map(Statement::Define)
			}
			t!("DELETE") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_delete_stmt(ctx)).await.map(Statement::Delete)
			}
			t!("FOR") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_for_stmt(ctx)).await.map(Statement::Foreach)
			}
			t!("IF") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_if_stmt(ctx)).await.map(Statement::Ifelse)
			}
			t!("INFO") => {
				self.pop_peek();
				self.parse_info_stmt().map(Statement::Info)
			}
			t!("INSERT") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_insert_stmt(ctx)).await.map(Statement::Insert)
			}
			t!("KILL") => {
				self.pop_peek();
				self.parse_kill_stmt().map(Statement::Kill)
			}
			t!("LIVE") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_live_stmt(ctx)).await.map(Statement::Live)
			}
			t!("OPTION") => {
				self.pop_peek();
				self.parse_option_stmt().map(Statement::Option)
			}
			t!("REBUILD") => {
				self.pop_peek();
				self.parse_rebuild_stmt().map(Statement::Rebuild)
			}
			t!("RETURN") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_return_stmt(ctx)).await.map(Statement::Output)
			}
			t!("RELATE") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_relate_stmt(ctx)).await.map(Statement::Relate)
			}
			t!("REMOVE") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_remove_stmt(ctx)).await.map(Statement::Remove)
			}
			t!("SELECT") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_select_stmt(ctx)).await.map(Statement::Select)
			}
			t!("LET") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_let_stmt(ctx)).await.map(Statement::Set)
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
				ctx.run(|ctx| self.parse_throw_stmt(ctx)).await.map(Statement::Throw)
			}
			t!("UPDATE") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_update_stmt(ctx)).await.map(Statement::Update)
			}
			t!("UPSERT") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_upsert_stmt(ctx)).await.map(Statement::Upsert)
			}
			t!("USE") => {
				self.pop_peek();
				self.parse_use_stmt().map(Statement::Use)
			}
			_ => {
				// TODO: Provide information about keywords.
				let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				Ok(Self::refine_stmt_value(value))
			}
		}
	}

	pub(super) async fn parse_entry(&mut self, ctx: &mut Stk) -> ParseResult<Entry> {
		enter_query_recursion!(this = self => {
			this.parse_entry_inner(ctx).await
		})
	}

	async fn parse_entry_inner(&mut self, ctx: &mut Stk) -> ParseResult<Entry> {
		let token = self.peek();
		match token.kind {
			t!("ALTER") => {
				self.pop_peek();
				self.parse_alter_stmt(ctx).await.map(Entry::Alter)
			}
			t!("BREAK") => {
				self.pop_peek();
				Ok(Entry::Break(BreakStatement))
			}
			t!("CONTINUE") => {
				self.pop_peek();
				Ok(Entry::Continue(ContinueStatement))
			}
			t!("CREATE") => {
				self.pop_peek();
				self.parse_create_stmt(ctx).await.map(Entry::Create)
			}
			t!("DEFINE") => {
				self.pop_peek();
				self.parse_define_stmt(ctx).await.map(Entry::Define)
			}
			t!("DELETE") => {
				self.pop_peek();
				self.parse_delete_stmt(ctx).await.map(Entry::Delete)
			}
			t!("FOR") => {
				self.pop_peek();
				self.parse_for_stmt(ctx).await.map(Entry::Foreach)
			}
			t!("IF") => {
				self.pop_peek();
				self.parse_if_stmt(ctx).await.map(Entry::Ifelse)
			}
			t!("INSERT") => {
				self.pop_peek();
				self.parse_insert_stmt(ctx).await.map(Entry::Insert)
			}
			t!("REBUILD") => {
				self.pop_peek();
				self.parse_rebuild_stmt().map(Entry::Rebuild)
			}
			t!("RETURN") => {
				self.pop_peek();
				self.parse_return_stmt(ctx).await.map(Entry::Output)
			}
			t!("RELATE") => {
				self.pop_peek();
				self.parse_relate_stmt(ctx).await.map(Entry::Relate)
			}
			t!("REMOVE") => {
				self.pop_peek();
				self.parse_remove_stmt(ctx).await.map(Entry::Remove)
			}
			t!("SELECT") => {
				self.pop_peek();
				self.parse_select_stmt(ctx).await.map(Entry::Select)
			}
			t!("LET") => {
				self.pop_peek();
				self.parse_let_stmt(ctx).await.map(Entry::Set)
			}
			t!("THROW") => {
				self.pop_peek();
				self.parse_throw_stmt(ctx).await.map(Entry::Throw)
			}
			t!("UPDATE") => {
				self.pop_peek();
				self.parse_update_stmt(ctx).await.map(Entry::Update)
			}
			t!("UPSERT") => {
				self.pop_peek();
				self.parse_upsert_stmt(ctx).await.map(Entry::Upsert)
			}
			_ => {
				// TODO: Provide information about keywords.
				let v = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
				Ok(Self::refine_entry_value(v))
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
						kind: None,
					});
				}
				Statement::Value(Value::Expression(x))
			}
			_ => Statement::Value(value),
		}
	}

	fn refine_entry_value(value: Value) -> Entry {
		match value {
			Value::Expression(x) => {
				if let Expression::Binary {
					l: Value::Param(x),
					o: Operator::Equal,
					r,
				} = *x
				{
					return Entry::Set(crate::sql::statements::SetStatement {
						name: x.0 .0,
						what: r,
						kind: None,
					});
				}
				Entry::Value(Value::Expression(x))
			}
			_ => Entry::Value(value),
		}
	}

	/// Parsers an access statement.
	async fn parse_access(&mut self, ctx: &mut Stk) -> ParseResult<AccessStatement> {
		let ac = self.next_token_value()?;
		let base = self.eat(t!("ON")).then(|| self.parse_base(false)).transpose()?;
		let peek = self.peek();
		match peek.kind {
			t!("GRANT") => {
				self.pop_peek();
				expected!(self, t!("FOR"));
				match self.peek_kind() {
					t!("USER") => {
						self.pop_peek();
						let user = self.next_token_value()?;
						Ok(AccessStatement::Grant(AccessStatementGrant {
							ac,
							base,
							subject: Some(Subject::User(user)),
						}))
					}
					t!("RECORD") => {
						self.pop_peek();
						let rid = ctx.run(|ctx| self.parse_thing(ctx)).await?;
						Ok(AccessStatement::Grant(AccessStatementGrant {
							ac,
							base,
							subject: Some(Subject::Record(rid)),
						}))
					}
					_ => unexpected!(self, peek, "either USER or RECORD"),
				}
			}
			t!("SHOW") => {
				self.pop_peek();

				if self.eat(t!("GRANT")) {
					return Ok(AccessStatement::Show(AccessStatementShow {
						ac,
						base,
						id: Some(self.next_token_value()?),
						..Default::default()
					}));
				}

				let mut show = AccessStatementShow {
					ac,
					base,
					..Default::default()
				};

				match self.peek_kind() {
					t!("ALL") => {
						self.pop_peek();
						show.active = true;
						show.expired = true;
						show.revoked = true;
					}
					t!("EXPIRED") => {
						self.pop_peek();
						show.expired = true;
						if self.eat(t!("SINCE")) {
							show.expired_since = Some(self.next_token_value()?);
						}
					}
					t!("REVOKED") => {
						self.pop_peek();
						show.revoked = true;
						if self.eat(t!("SINCE")) {
							show.revoked_since = Some(self.next_token_value()?);
						}
					}
					// By default, show only active grants.
					_ => {
						show.active = true;
					}
				};

				loop {
					match self.peek_kind() {
						t!("FOR") => {
							self.pop_peek();
							match self.peek_kind() {
								t!("USER") => {
									self.pop_peek();
									let user = self.next_token_value()?;
									show.subject = Some(Subject::User(user));
								}
								t!("RECORD") => {
									self.pop_peek();
									let rid = ctx.run(|ctx| self.parse_thing(ctx)).await?;
									show.subject = Some(Subject::Record(rid));
								}
								_ => unexpected!(self, peek, "either USER or RECORD"),
							}
						}
						t!("AFTER") => {
							self.pop_peek();
							show.created_after = Some(self.next_token_value()?);
						}
						t!("BEFORE") => {
							self.pop_peek();
							show.created_before = Some(self.next_token_value()?);
						}
						_ => break,
					}
				}

				Ok(AccessStatement::Show(show))
			}
			t!("REVOKE") => {
				self.pop_peek();
				let gr = match self.peek_kind() {
					t!("GRANT") => {
						self.pop_peek();
						Some(self.next_token_value()?)
					}
					t!("ALL") => {
						self.pop_peek();
						None
					}
					_ => unexpected!(self, peek, "either GRANT or ALL"),
				};

				Ok(AccessStatement::Revoke(AccessStatementRevoke {
					ac,
					base,
					gr,
				}))
			}
			t!("PURGE") => {
				self.pop_peek();
				let (expired, revoked) = match self.peek_kind() {
					t!("EXPIRED") => {
						self.pop_peek();
						(true, false)
					}
					t!("REVOKED") => {
						self.pop_peek();
						(false, true)
					}
					// By default, purge all inactive grants.
					_ => (true, true),
				};
				let grace = if self.eat(t!("FOR")) {
					self.next_token_value()?
				} else {
					Duration::default()
				};
				Ok(AccessStatement::Purge(AccessStatementPurge {
					ac,
					base,
					expired,
					revoked,
					grace,
				}))
			}
			_ => unexpected!(self, peek, "one of GRANT, LIST, REVOKE or PURGE"),
		}
	}

	/// Parsers a analyze statement.
	fn parse_analyze(&mut self) -> ParseResult<AnalyzeStatement> {
		expected!(self, t!("INDEX"));

		let index = self.next_token_value()?;
		expected!(self, t!("ON"));
		let table = self.next_token_value()?;

		Ok(AnalyzeStatement::Idx(table, index))
	}

	/// Parsers a begin statement.
	///
	/// # Parser State
	/// Expects `BEGIN` to already be consumed.
	fn parse_begin(&mut self) -> ParseResult<BeginStatement> {
		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(BeginStatement)
	}

	/// Parsers a cancel statement.
	///
	/// # Parser State
	/// Expects `CANCEL` to already be consumed.
	fn parse_cancel(&mut self) -> ParseResult<CancelStatement> {
		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(CancelStatement)
	}

	/// Parsers a commit statement.
	///
	/// # Parser State
	/// Expects `COMMIT` to already be consumed.
	fn parse_commit(&mut self) -> ParseResult<CommitStatement> {
		if let t!("TRANSACTION") = self.peek().kind {
			self.next();
		}
		Ok(CommitStatement)
	}

	/// Parsers a USE statement.
	///
	/// # Parser State
	/// Expects `USE` to already be consumed.
	fn parse_use_stmt(&mut self) -> ParseResult<UseStatement> {
		let peek = self.peek();
		let (ns, db) = match peek.kind {
			t!("NAMESPACE") => {
				self.pop_peek();
				let ns = self.next_token_value::<Ident>()?.0;
				let db = self
					.eat(t!("DATABASE"))
					.then(|| self.next_token_value::<Ident>())
					.transpose()?
					.map(|x| x.0);
				(Some(ns), db)
			}
			t!("DATABASE") => {
				self.pop_peek();
				let db = self.next_token_value::<Ident>()?;
				(None, Some(db.0))
			}
			_ => unexpected!(self, peek, "either DATABASE or NAMESPACE"),
		};

		Ok(UseStatement {
			ns,
			db,
		})
	}

	/// Parsers a FOR statement.
	///
	/// # Parser State
	/// Expects `FOR` to already be consumed.
	pub(super) async fn parse_for_stmt(&mut self, stk: &mut Stk) -> ParseResult<ForeachStatement> {
		let param = self.next_token_value()?;
		expected!(self, t!("IN"));
		let range = stk.run(|stk| self.parse_value_inherit(stk)).await?;

		let span = expected!(self, t!("{")).span;
		let block = self.parse_block(stk, span).await?;
		Ok(ForeachStatement {
			param,
			range,
			block,
		})
	}

	/// Parsers a INFO statement.
	///
	/// # Parser State
	/// Expects `INFO` to already be consumed.
	pub(super) fn parse_info_stmt(&mut self) -> ParseResult<InfoStatement> {
		expected!(self, t!("FOR"));
		let next = self.next();
		let mut stmt = match next.kind {
			t!("ROOT") => InfoStatement::Root(false),
			t!("NAMESPACE") => InfoStatement::Ns(false),
			t!("DATABASE") => InfoStatement::Db(false, None),
			t!("TABLE") => {
				let ident = self.next_token_value()?;
				InfoStatement::Tb(ident, false, None)
			}
			t!("USER") => {
				let ident = self.next_token_value()?;
				let base = self.eat(t!("ON")).then(|| self.parse_base(false)).transpose()?;
				InfoStatement::User(ident, base, false)
			}
			t!("INDEX") => {
				let index = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;
				InfoStatement::Index(index, table, false)
			}
			_ => unexpected!(self, next, "an info target"),
		};

		if let Some(version) = self.try_parse_version()? {
			stmt = stmt.versionize(version);
		}

		if self.peek_kind() == t!("STRUCTURE") {
			self.pop_peek();
			stmt = stmt.structurize();
		};
		Ok(stmt)
	}

	/// Parsers a KILL statement.
	///
	/// # Parser State
	/// Expects `KILL` to already be consumed.
	pub(super) fn parse_kill_stmt(&mut self) -> ParseResult<KillStatement> {
		let peek = self.peek();
		let id = match peek.kind {
			t!("u\"") | t!("u'") | TokenKind::Glued(Glued::Uuid) => {
				self.next_token_value().map(Value::Uuid)?
			}
			t!("$param") => self.next_token_value().map(Value::Param)?,
			_ => unexpected!(self, peek, "a UUID or a parameter"),
		};
		Ok(KillStatement {
			id,
		})
	}

	/// Parsers a LIVE statement.
	///
	/// # Parser State
	/// Expects `LIVE` to already be consumed.
	pub(super) async fn parse_live_stmt(&mut self, stk: &mut Stk) -> ParseResult<LiveStatement> {
		expected!(self, t!("SELECT"));

		let expr = match self.peek_kind() {
			t!("DIFF") => {
				self.pop_peek();
				Fields::default()
			}
			_ => self.parse_fields(stk).await?,
		};
		expected!(self, t!("FROM"));
		let what = match self.peek().kind {
			t!("$param") => Value::Param(self.next_token_value()?),
			_ => Value::Table(self.next_token_value()?),
		};
		let cond = self.try_parse_condition(stk).await?;
		let fetch = self.try_parse_fetch(stk).await?;

		Ok(LiveStatement::from_source_parts(expr, what, cond, fetch))
	}

	/// Parsers a OPTION statement.
	///
	/// # Parser State
	/// Expects `OPTION` to already be consumed.
	pub(super) fn parse_option_stmt(&mut self) -> ParseResult<OptionStatement> {
		let name = self.next_token_value()?;
		let what = if self.eat(t!("=")) {
			let next = self.next();
			match next.kind {
				t!("true") => true,
				t!("false") => false,
				_ => unexpected!(self, next, "either 'true' or 'false'"),
			}
		} else {
			true
		};
		Ok(OptionStatement {
			name,
			what,
		})
	}

	pub(super) fn parse_rebuild_stmt(&mut self) -> ParseResult<RebuildStatement> {
		let next = self.next();
		let res = match next.kind {
			t!("INDEX") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = self.next_token_value()?;

				RebuildStatement::Index(RebuildIndexStatement {
					what,
					name,
					if_exists,
				})
			}
			_ => unexpected!(self, next, "a rebuild statement keyword"),
		};
		Ok(res)
	}

	/// Parsers a RETURN statement.
	///
	/// # Parser State
	/// Expects `RETURN` to already be consumed.
	pub(super) async fn parse_return_stmt(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<OutputStatement> {
		let what = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
		let fetch = self.try_parse_fetch(ctx).await?;
		Ok(OutputStatement {
			what,
			fetch,
		})
	}

	/// Parsers a LET statement.
	///
	/// SurrealQL has support for `LET` less let statements.
	/// These are not parsed here but after a statement is fully parsed.
	/// A expression statement which matches a let-less let statement is then refined into a let
	/// statement.
	///
	/// # Parser State
	/// Expects `LET` to already be consumed.
	pub(super) async fn parse_let_stmt(&mut self, ctx: &mut Stk) -> ParseResult<SetStatement> {
		let name = self.next_token_value::<Param>()?.0 .0;
		let kind = if self.eat(t!(":")) {
			Some(self.parse_inner_kind(ctx).await?)
		} else {
			None
		};
		expected!(self, t!("="));
		let what = self.parse_value_inherit(ctx).await?;
		Ok(SetStatement {
			name,
			what,
			kind,
		})
	}

	/// Parsers a SHOW statement
	///
	/// # Parser State
	/// Expects `SHOW` to already be consumed.
	pub(super) fn parse_show_stmt(&mut self) -> ParseResult<ShowStatement> {
		expected!(self, t!("CHANGES"));
		expected!(self, t!("FOR"));

		let next = self.next();
		let table = match next.kind {
			t!("TABLE") => {
				let table = self.next_token_value()?;
				Some(table)
			}
			t!("DATABASE") => None,
			_ => unexpected!(self, next, "`TABLE` or `DATABASE`"),
		};

		expected!(self, t!("SINCE"));

		let next = self.peek();
		let since = match next.kind {
			TokenKind::Digits => {
				self.pop_peek();
				let int = self.lexer.lex_compound(next, compound::integer)?.value;
				ShowSince::Versionstamp(int)
			}
			t!("d\"") | t!("d'") => ShowSince::Timestamp(self.next_token_value()?),
			TokenKind::Glued(_) => {
				// This panic can be upheld within this function, just make sure you don't call
				// glue here and the `next()` before this peek should eat any glued value.
				panic!("A glued number token would truncate the timestamp so no gluing is allowed before this production.");
			}
			_ => unexpected!(self, next, "a version stamp or a date-time"),
		};

		let limit = self.eat(t!("LIMIT")).then(|| self.next_token_value()).transpose()?;

		Ok(ShowStatement {
			table,
			since,
			limit,
		})
	}

	/// Parsers a SLEEP statement
	///
	/// # Parser State
	/// Expects `SLEEP` to already be consumed.
	pub(super) fn parse_sleep_stmt(&mut self) -> ParseResult<SleepStatement> {
		let duration = self.next_token_value()?;
		Ok(SleepStatement {
			duration,
		})
	}

	/// Parsers a THROW statement
	///
	/// # Parser State
	/// Expects `THROW` to already be consumed.
	pub(super) async fn parse_throw_stmt(&mut self, ctx: &mut Stk) -> ParseResult<ThrowStatement> {
		let error = self.parse_value_inherit(ctx).await?;
		Ok(ThrowStatement {
			error,
		})
	}
}
