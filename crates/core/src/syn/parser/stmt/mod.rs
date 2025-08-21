use reblessive::Stk;

use super::mac::expected;
use super::{ParseResult, Parser};
use crate::sql::data::Assignment;
use crate::sql::statements::access::{
	AccessStatement, AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke,
	AccessStatementShow, Subject,
};
use crate::sql::statements::analyze::AnalyzeStatement;
use crate::sql::statements::rebuild::RebuildIndexStatement;
use crate::sql::statements::show::ShowSince;
use crate::sql::statements::{
	ForeachStatement, InfoStatement, KillStatement, LiveStatement, OptionStatement,
	OutputStatement, RebuildStatement, SetStatement, ShowStatement, SleepStatement, UseStatement,
};
use crate::sql::{AssignOperator, Expr, Fields, Ident, Literal, Param, TopLevelExpr};
use crate::syn::lexer::compound;
use crate::syn::parser::mac::unexpected;
use crate::syn::token::{Glued, TokenKind, t};
use crate::val::Duration;

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
	pub(super) async fn parse_stmt_list(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<Vec<TopLevelExpr>> {
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
					let stmt = stk.run(|ctx| self.parse_top_level_expr(ctx)).await?;
					res.push(stmt);
					if !self.eat(t!(";")) {
						if self.eat(t!("eof")) {
							break;
						}

						let token = self.peek();
						if Self::kind_starts_statement(token.kind) {
							// consume token for streaming
							self.pop_peek();
							// user likely forgot a semicolon.
							unexpected!(self,token,"the query to end", => "maybe forgot a semicolon after the previous statement?");
						}

						expected!(self, t!("eof"));
					}
				}
			}
		}
		Ok(res)
	}

	pub(super) async fn parse_top_level_expr(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<TopLevelExpr> {
		let token = self.peek();
		match token.kind {
			t!("BEGIN") => {
				self.pop_peek();
				self.parse_begin()
			}
			t!("CANCEL") => {
				self.pop_peek();
				self.parse_cancel()
			}
			t!("COMMIT") => {
				self.pop_peek();
				self.parse_commit()
			}
			t!("KILL") => {
				self.pop_peek();
				self.parse_kill_stmt().map(TopLevelExpr::Kill)
			}
			t!("LIVE") => {
				self.pop_peek();
				self.parse_live_stmt(stk).await.map(|x| TopLevelExpr::Live(Box::new(x)))
			}
			t!("OPTION") => {
				self.pop_peek();
				self.parse_option_stmt().map(TopLevelExpr::Option)
			}
			t!("USE") => {
				self.pop_peek();
				self.parse_use_stmt().map(TopLevelExpr::Use)
			}
			t!("ACCESS") => {
				self.pop_peek();
				self.parse_access(stk).await.map(|x| TopLevelExpr::Access(Box::new(x)))
			}
			t!("ANALYZE") => {
				self.pop_peek();
				self.parse_analyze().map(TopLevelExpr::Analyze)
			}
			t!("SHOW") => {
				self.pop_peek();
				self.parse_show_stmt().map(TopLevelExpr::Show)
			}
			_ => self.parse_expr_start(stk).await.map(TopLevelExpr::Expr),
		}
	}

	/// Parsers an access statement.
	async fn parse_access(&mut self, stk: &mut Stk) -> ParseResult<AccessStatement> {
		let ac = self.next_token_value()?;
		let base = self.eat(t!("ON")).then(|| self.parse_base()).transpose()?;
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
							subject: Subject::User(user),
						}))
					}
					t!("RECORD") => {
						self.pop_peek();
						let rid = stk.run(|ctx| self.parse_record_id(ctx)).await?;
						Ok(AccessStatement::Grant(AccessStatementGrant {
							ac,
							base,
							subject: Subject::Record(rid),
						}))
					}
					_ => unexpected!(self, peek, "either USER or RECORD"),
				}
			}
			t!("SHOW") => {
				self.pop_peek();
				match self.peek_kind() {
					t!("ALL") => {
						self.pop_peek();
						Ok(AccessStatement::Show(AccessStatementShow {
							ac,
							base,
							..Default::default()
						}))
					}
					t!("GRANT") => {
						self.pop_peek();
						let gr = Some(self.next_token_value()?);
						Ok(AccessStatement::Show(AccessStatementShow {
							ac,
							base,
							gr,
							..Default::default()
						}))
					}
					t!("WHERE") => {
						let cond = self.try_parse_condition(stk).await?;
						Ok(AccessStatement::Show(AccessStatementShow {
							ac,
							base,
							cond,
							..Default::default()
						}))
					}
					_ => unexpected!(self, peek, "one of ALL, GRANT or WHERE"),
				}
			}
			t!("REVOKE") => {
				self.pop_peek();
				match self.peek_kind() {
					t!("ALL") => {
						self.pop_peek();
						Ok(AccessStatement::Revoke(AccessStatementRevoke {
							ac,
							base,
							..Default::default()
						}))
					}
					t!("GRANT") => {
						self.pop_peek();
						let gr = Some(self.next_token_value()?);
						Ok(AccessStatement::Revoke(AccessStatementRevoke {
							ac,
							base,
							gr,
							..Default::default()
						}))
					}
					t!("WHERE") => {
						let cond = self.try_parse_condition(stk).await?;
						Ok(AccessStatement::Revoke(AccessStatementRevoke {
							ac,
							base,
							cond,
							..Default::default()
						}))
					}
					_ => unexpected!(self, peek, "one of ALL, GRANT or WHERE"),
				}
			}
			t!("PURGE") => {
				self.pop_peek();
				let mut expired = false;
				let mut revoked = false;
				loop {
					match self.peek_kind() {
						t!("EXPIRED") => {
							self.pop_peek();
							expired = true;
						}
						t!("REVOKED") => {
							self.pop_peek();
							revoked = true;
						}
						_ => {
							if !expired && !revoked {
								unexpected!(self, peek, "EXPIRED, REVOKED or both");
							}
							break;
						}
					}
					self.eat(t!(","));
				}
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
			_ => unexpected!(self, peek, "one of GRANT, SHOW, REVOKE or PURGE"),
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
	fn parse_begin(&mut self) -> ParseResult<TopLevelExpr> {
		self.eat(t!("TRANSACTION"));
		Ok(TopLevelExpr::Begin)
	}

	/// Parsers a cancel statement.
	///
	/// # Parser State
	/// Expects `CANCEL` to already be consumed.
	fn parse_cancel(&mut self) -> ParseResult<TopLevelExpr> {
		self.eat(t!("TRANSACTION"));
		Ok(TopLevelExpr::Cancel)
	}

	/// Parsers a commit statement.
	///
	/// # Parser State
	/// Expects `COMMIT` to already be consumed.
	fn parse_commit(&mut self) -> ParseResult<TopLevelExpr> {
		self.eat(t!("TRANSACTION"));
		Ok(TopLevelExpr::Commit)
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
				let ns = self.next_token_value::<Ident>()?;
				let db = self
					.eat(t!("DATABASE"))
					.then(|| self.next_token_value::<Ident>())
					.transpose()?;
				(Some(ns), db)
			}
			t!("DATABASE") => {
				self.pop_peek();
				let db = self.next_token_value::<Ident>()?;
				(None, Some(db))
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
		let range = stk.run(|stk| self.parse_expr_inherit(stk)).await?;

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
	pub(super) async fn parse_info_stmt(&mut self, stk: &mut Stk) -> ParseResult<InfoStatement> {
		expected!(self, t!("FOR"));
		let next = self.next();
		let stmt = match next.kind {
			t!("ROOT") => {
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Root(structure)
			}
			t!("NAMESPACE") => {
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Ns(structure)
			}
			t!("DATABASE") => {
				let version = if self.eat(t!("VERSION")) {
					Some(stk.run(|stk| self.parse_expr_inherit(stk)).await?)
				} else {
					None
				};
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Db(structure, version)
			}
			t!("TABLE") => {
				let ident = self.next_token_value()?;
				let version = if self.eat(t!("VERSION")) {
					Some(stk.run(|stk| self.parse_expr_inherit(stk)).await?)
				} else {
					None
				};
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Tb(ident, structure, version)
			}
			t!("USER") => {
				let ident = self.next_token_value()?;
				let base = self.eat(t!("ON")).then(|| self.parse_base()).transpose()?;
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::User(ident, base, structure)
			}
			t!("INDEX") => {
				let index = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Index(index, table, structure)
			}
			_ => unexpected!(self, next, "an info target"),
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
				self.next_token_value().map(|u| Expr::Literal(Literal::Uuid(u)))?
			}
			t!("$param") => self.next_token_value().map(Expr::Param)?,
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
				Fields::all()
			}
			_ => self.parse_fields(stk).await?,
		};
		expected!(self, t!("FROM"));
		let what = match self.peek().kind {
			t!("$param") => Expr::Param(self.next_token_value()?),
			_ => self.parse_expr_table(stk).await?,
		};
		let cond = self.try_parse_condition(stk).await?;
		let fetch = self.try_parse_fetch(stk).await?;

		Ok(LiveStatement {
			fields: expr,
			what,
			cond,
			fetch,
		})
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
		stk: &mut Stk,
	) -> ParseResult<OutputStatement> {
		let what = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
		let fetch = self.try_parse_fetch(stk).await?;
		Ok(OutputStatement {
			what,
			fetch,
		})
	}

	/// Parsers a LET statement.
	///
	/// SurrealQL has support for `LET` less let statements.
	/// These are not parsed here but after a statement is fully parsed.
	/// A expression statement which matches a let-less let statement is then
	/// refined into a let statement.
	///
	/// # Parser State
	/// Expects `LET` to already be consumed.
	pub(super) async fn parse_let_stmt(&mut self, stk: &mut Stk) -> ParseResult<SetStatement> {
		let name = self.next_token_value::<Param>()?.ident();
		let kind = if self.eat(t!(":")) {
			Some(self.parse_inner_kind(stk).await?)
		} else {
			None
		};
		expected!(self, t!("="));
		let what = stk.run(|stk| self.parse_expr_inherit(stk)).await?;
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
				panic!(
					"A glued number token would truncate the timestamp so no gluing is allowed before this production."
				);
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

	pub(super) async fn parse_assignment(&mut self, stk: &mut Stk) -> ParseResult<Assignment> {
		let place = self.parse_plain_idiom(stk).await?;
		let token = self.next();
		let operator = match token.kind {
			t!("=") => AssignOperator::Assign,
			t!("+=") => AssignOperator::Add,
			t!("-=") => AssignOperator::Subtract,
			t!("+?=") => AssignOperator::Extend,
			_ => unexpected!(self, token, "an assign operator"),
		};
		let value = stk.run(|stk| self.parse_expr_field(stk)).await?;
		Ok(Assignment {
			place,
			operator,
			value,
		})
	}
}
