use surrealdb_sql::data::Assignment;
use surrealdb_sql::statements::access::{
	AccessStatement, AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke,
	AccessStatementShow, PurgeKind, Subject,
};
use surrealdb_sql::statements::live::LiveFields;
use surrealdb_sql::statements::rebuild::RebuildIndexStatement;
use surrealdb_sql::statements::show::ShowSince;
use surrealdb_sql::statements::{
	ForeachStatement, InfoStatement, KillStatement, LiveStatement, OptionStatement,
	OutputStatement, RebuildStatement, SetStatement, ShowStatement, SleepStatement, UseStatement,
};
use surrealdb_sql::{AssignOperator, ExplainFormat, Expr, Literal, Param, TopLevelExpr};

use super::mac::expected;
use super::{ParseResult, Parser};
use crate::lexer::compound;
use crate::parser::mac::unexpected;
use crate::token::{TokenKind, t};

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
	pub(super) fn parse_stmt_list(&mut self) -> ParseResult<Vec<TopLevelExpr>> {
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
					let stmt = self.parse_top_level_expr()?;
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

	pub(super) fn parse_top_level_expr(&mut self) -> ParseResult<TopLevelExpr> {
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
				self.parse_live_stmt().map(|x| TopLevelExpr::Live(Box::new(x)))
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
				self.parse_access().map(|x| TopLevelExpr::Access(Box::new(x)))
			}
			t!("SHOW") => {
				self.pop_peek();
				self.parse_show_stmt().map(TopLevelExpr::Show)
			}
			_ => {
				let covered = self.peek_kind() == t!("(");
				let expr = self.parse_expr_start()?;
				let span = token.span.covers(self.last_span);
				if !covered {
					Self::reject_letless_let(&expr, span)?;
				}
				Ok(TopLevelExpr::Expr(expr))
			}
		}
	}

	/// Parsers an access statement.
	fn parse_access(&mut self) -> ParseResult<AccessStatement> {
		let ac = self.parse_ident()?;
		let base = self.eat(t!("ON")).then(|| self.parse_base()).transpose()?;
		let peek = self.peek();
		match peek.kind {
			t!("GRANT") => {
				self.pop_peek();
				expected!(self, t!("FOR"));
				match self.peek_kind() {
					t!("USER") => {
						self.pop_peek();
						let user = self.parse_ident()?;
						Ok(AccessStatement::Grant(AccessStatementGrant {
							ac,
							base,
							subject: Subject::User(user),
						}))
					}
					t!("RECORD") => {
						self.pop_peek();
						let rid = self.parse_record_id()?;
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
						let gr = Some(self.parse_ident()?);
						Ok(AccessStatement::Show(AccessStatementShow {
							ac,
							base,
							gr,
							..Default::default()
						}))
					}
					t!("WHERE") => {
						let cond = self.try_parse_condition()?;
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
						let gr = Some(self.parse_ident()?);
						Ok(AccessStatement::Revoke(AccessStatementRevoke {
							ac,
							base,
							gr,
							..Default::default()
						}))
					}
					t!("WHERE") => {
						let cond = self.try_parse_condition()?;
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
				let mut kind = None;
				let kind = loop {
					match self.peek_kind() {
						t!("EXPIRED") => {
							self.pop_peek();
							match kind {
								None => kind = Some(PurgeKind::Expired),
								Some(PurgeKind::Revoked) => kind = Some(PurgeKind::Both),
								_ => unexpected!(self, peek, "ACCESS PURGE statement to end"),
							}
						}
						t!("REVOKED") => {
							self.pop_peek();
							match kind {
								None => kind = Some(PurgeKind::Revoked),
								Some(PurgeKind::Expired) => kind = Some(PurgeKind::Both),
								_ => unexpected!(self, peek, "ACCESS PURGE statement to end"),
							}
						}
						_ => {
							let Some(kind) = kind else {
								unexpected!(self, peek, "EXPIRED, or REVOKED")
							};
							break kind;
						}
					}
					//TODO: This is kind of bad syntax, we should either choose to have a `,`
					//between keywords or not, not allow both and just ignore it.
					self.eat(t!(","));
				};
				let grace = if self.eat(t!("FOR")) {
					self.next_token_value()?
				} else {
					std::time::Duration::default()
				};
				Ok(AccessStatement::Purge(AccessStatementPurge {
					ac,
					base,
					kind,
					grace,
				}))
			}
			_ => unexpected!(self, peek, "one of GRANT, SHOW, REVOKE or PURGE"),
		}
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

	/// Parses an EXPLAIN expression.
	///
	/// # Parser State
	/// Expects `EXPLAIN` to already be consumed.
	pub(super) fn parse_explain_expr(&mut self) -> ParseResult<Expr> {
		// Check for optional ANALYZE keyword
		// ANALYZE is not a reserved keyword, so we need to check if it's an identifier
		let analyze = {
			let peek = self.peek();
			if matches!(peek.kind, TokenKind::Identifier) {
				let ident_str = self.lexer.span_str(peek.span);
				if ident_str.eq_ignore_ascii_case("ANALYZE") {
					self.pop_peek();
					true
				} else {
					false
				}
			} else {
				false
			}
		};

		// Check for optional FORMAT keyword
		let format = {
			let peek = self.peek();
			if matches!(peek.kind, TokenKind::Identifier) {
				let ident_str = self.lexer.span_str(peek.span);
				if ident_str.eq_ignore_ascii_case("FORMAT") {
					self.pop_peek();
					// Now expect TEXT or JSON
					let format_peek = self.peek();
					if matches!(format_peek.kind, TokenKind::Identifier) {
						let format_str = self.lexer.span_str(format_peek.span);
						if format_str.eq_ignore_ascii_case("TEXT") {
							self.pop_peek();
							ExplainFormat::Text
						} else if format_str.eq_ignore_ascii_case("JSON") {
							self.pop_peek();
							ExplainFormat::Json
						} else {
							unexpected!(self, format_peek, "TEXT or JSON")
						}
					} else {
						unexpected!(self, format_peek, "TEXT or JSON")
					}
				} else {
					ExplainFormat::Text // Default to TEXT
				}
			} else {
				ExplainFormat::Text // Default to TEXT
			}
		};

		// Parse the inner statement as an expression
		let statement = self.parse_expr_start()?;

		Ok(Expr::Explain {
			format,
			analyze,
			statement: Box::new(statement),
		})
	}

	/// Parsers a USE statement.
	///
	/// # Parser State
	/// Expects `USE` to already be consumed.
	fn parse_use_stmt(&mut self) -> ParseResult<UseStatement> {
		let peek = self.peek();
		let stmt = match peek.kind {
			t!("NAMESPACE") => {
				self.pop_peek();
				let ns = self.parse_expr_field()?;
				if self.eat(t!("DATABASE")) {
					let db = self.parse_expr_field()?;
					UseStatement::NsDb(ns, db)
				} else {
					UseStatement::Ns(ns)
				}
			}
			t!("DATABASE") => {
				self.pop_peek();
				let db = self.parse_expr_field()?;
				UseStatement::Db(db)
			}
			t!("DEFAULT") => {
				self.pop_peek();
				UseStatement::Default
			}
			_ => unexpected!(self, peek, "either DATABASE or NAMESPACE"),
		};

		Ok(stmt)
	}

	/// Parsers a FOR statement.
	///
	/// # Parser State
	/// Expects `FOR` to already be consumed.
	pub(super) fn parse_for_stmt(&mut self) -> ParseResult<ForeachStatement> {
		let param = self.next_token_value()?;
		expected!(self, t!("IN"));
		let range = self.parse_expr_inherit()?;

		let span = expected!(self, t!("{")).span;
		let block = self.parse_block(span)?;
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
		let stmt = match next.kind {
			t!("ROOT") => {
				let version = if self.eat(t!("VERSION")) {
					Some(self.parse_expr_inherit()?)
				} else {
					None
				};
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Root(structure, version)
			}
			t!("NAMESPACE") => {
				let version = if self.eat(t!("VERSION")) {
					Some(self.parse_expr_inherit()?)
				} else {
					None
				};
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Ns(structure, version)
			}
			t!("DATABASE") => {
				let version = if self.eat(t!("VERSION")) {
					Some(self.parse_expr_inherit()?)
				} else {
					None
				};
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Db(structure, version)
			}
			t!("TABLE") | t!("TB") => {
				let ident = self.parse_expr_table()?;
				let version = if self.eat(t!("VERSION")) {
					Some(self.parse_expr_inherit()?)
				} else {
					None
				};
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::Tb(ident, structure, version)
			}
			t!("USER") => {
				let ident = self.parse_expr_inherit()?;
				let base = self.eat(t!("ON")).then(|| self.parse_base()).transpose()?;
				let structure = self.eat(t!("STRUCTURE"));
				InfoStatement::User(ident, base, structure)
			}
			t!("INDEX") => {
				let index = self.parse_expr_field()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.parse_expr_table()?;
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
			t!("u\"") | t!("u'") => {
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
	pub(super) fn parse_live_stmt(&mut self) -> ParseResult<LiveStatement> {
		expected!(self, t!("SELECT"));

		let fields = match self.peek_kind() {
			t!("DIFF") => {
				self.pop_peek();
				LiveFields::Diff
			}
			_ => LiveFields::Select(self.parse_fields()?),
		};
		expected!(self, t!("FROM"));
		let what = self.parse_expr_table()?;
		let cond = self.try_parse_condition()?;
		let fetch = self.try_parse_fetch()?;

		Ok(LiveStatement {
			fields,
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
		let name = self.parse_ident()?;
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
				let name = self.parse_ident()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what: surrealdb_sql::TableName = self.parse_ident_str()?.into();
				let concurrently = self.eat(t!("CONCURRENTLY"));
				RebuildStatement::Index(RebuildIndexStatement {
					what,
					name,
					if_exists,
					concurrently,
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
	pub(super) fn parse_return_stmt(&mut self) -> ParseResult<OutputStatement> {
		let what = self.parse_expr_inherit()?;
		let fetch = self.try_parse_fetch()?;
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
	pub(super) fn parse_let_stmt(&mut self) -> ParseResult<SetStatement> {
		let name = self.next_token_value::<Param>()?.into_strand();
		let kind = if self.eat(t!(":")) {
			Some(self.parse_inner_kind()?)
		} else {
			None
		};
		expected!(self, t!("="));
		let what = self.parse_expr_inherit()?;
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
			t!("TABLE") | t!("TB") => {
				let table: surrealdb_sql::TableName = self.parse_ident_str()?.into();
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
				let int = self.lex_compound(next, compound::integer)?.value;
				ShowSince::Versionstamp(int)
			}
			t!("d\"") | t!("d'") => ShowSince::Timestamp(self.next_token_value()?),
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

	pub(super) fn parse_assignment(&mut self) -> ParseResult<Assignment> {
		let place = self.parse_plain_idiom()?;
		let token = self.next();
		let operator = match token.kind {
			t!("=") => AssignOperator::Assign,
			t!("+=") => AssignOperator::Add,
			t!("-=") => AssignOperator::Subtract,
			t!("+?=") => AssignOperator::Extend,
			_ => unexpected!(self, token, "an assign operator"),
		};
		let value = self.parse_expr_field()?;
		Ok(Assignment {
			place,
			operator,
			value,
		})
	}
}
