use std::fmt::Display;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use futures::{Stream, StreamExt, stream};
use reblessive::TreeStack;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tracing::{instrument, warn};
use trice::Instant;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

use crate::ctx::Context;
use crate::ctx::reason::Reason;
use crate::dbs::response::Response;
use crate::dbs::{Force, Options, QueryType};
use crate::err::Error;
use crate::expr::paths::{DB, NS};
use crate::expr::plan::LogicalPlan;
use crate::expr::statements::OptionStatement;
use crate::expr::{Base, ControlFlow, Expr, FlowResult, TopLevelExpr};
use crate::iam::{Action, ResourceKind};
use crate::kvs::{Datastore, LockType, Transaction, TransactionType};
use crate::sql::{self, Ast};
use crate::val::Value;
use crate::{err, expr};

const TARGET: &str = "surrealdb::core::dbs";

pub struct Executor {
	stack: TreeStack,
	results: Vec<Response>,
	opt: Options,
	ctx: Context,
}

impl Executor {
	pub fn new(ctx: Context, opt: Options) -> Self {
		Executor {
			stack: TreeStack::new(),
			results: Vec::new(),
			opt,
			ctx,
		}
	}

	fn execute_option_statement(&mut self, stmt: OptionStatement) -> Result<()> {
		// Allowed to run?
		self.opt.is_allowed(Action::Edit, ResourceKind::Option, &Base::Db)?;

		if stmt.name.eq_ignore_ascii_case("IMPORT") {
			self.opt.set_import(stmt.what);
		} else if stmt.name.eq_ignore_ascii_case("FORCE") {
			let force = if stmt.what {
				Force::All
			} else {
				Force::None
			};
			self.opt.force = force;
		}

		Ok(())
	}

	fn check_slow_log(&self, start: &Instant, stm: &impl Display) {
		if let Some(threshold) = self.ctx.slow_log_threshold() {
			let elapsed = start.elapsed();
			if elapsed > threshold {
				warn!("Slow query detected - time: {elapsed:#?} - query: {stm}")
			}
		}
	}

	/// Executes a statement which needs a transaction with the supplied
	/// transaction.
	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	async fn execute_plan_in_transaction(
		&mut self,
		txn: Arc<Transaction>,
		start: &Instant,
		plan: TopLevelExpr,
	) -> FlowResult<Value> {
		let res = match plan {
			TopLevelExpr::Use(stmt) => {
				// Avoid moving in and out of the context via Arc::get_mut
				let ctx = Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						Error::unreachable("Tried to unfreeze a Context with multiple references")
					})
					.map_err(anyhow::Error::new)?;

				if let Some(ns) = stmt.ns {
					txn.get_or_add_ns(&ns, self.opt.strict).await?;

					let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
					self.opt.set_ns(Some(ns.as_str().into()));
					session.put(NS.as_ref(), ns.into_strand().into());
					ctx.add_value("session", session.into());
				}
				if let Some(db) = stmt.db {
					let Some(ns) = &self.opt.ns else {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"Cannot use database without namespace"
						)));
					};

					txn.ensure_ns_db(ns, &db, self.opt.strict).await?;

					let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
					self.opt.set_db(Some(db.as_str().into()));
					session.put(DB.as_ref(), db.into_strand().into());
					ctx.add_value("session", session.into());
				}
				Ok(Value::None)
			}
			TopLevelExpr::Option(_) => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::unreachable(
					"TopLevelExpr::Option should have been handled by a calling function",
				))));
			}

			TopLevelExpr::Expr(Expr::Let(stm)) => {
				// Avoid moving in and out of the context via Arc::get_mut
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						Error::unreachable("Tried to unfreeze a Context with multiple references")
					})
					.map_err(anyhow::Error::new)?
					.set_transaction(txn);

				// Run the statement
				let res = self
					.stack
					.enter(|stk| stm.what.compute(stk, &self.ctx, &self.opt, None))
					.finish()
					.await;

				// Check if we dump the slow log
				self.check_slow_log(start, &stm);

				let res = res?;
				let result = match &stm.kind {
					Some(kind) => res
						.coerce_to_kind(kind)
						.map_err(|e| Error::SetCoerce {
							name: stm.name.to_string(),
							error: Box::new(e),
						})
						.map_err(anyhow::Error::new)?,
					None => res,
				};

				if stm.is_protected_set() {
					return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidParam {
						name: stm.name.clone().into_string(),
					})));
				}
				// Set the parameter
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						Error::unreachable("Tried to unfreeze a Context with multiple references")
					})
					.map_err(anyhow::Error::new)?
					.add_value(stm.name.into_string(), result.into());
				// Finalise transaction, returning nothing unless it couldn't commit
				Ok(Value::None)
			}
			TopLevelExpr::Begin => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatement(
					"Cannot BEGIN a transaction within a transaction".to_string(),
				))));
			}
			TopLevelExpr::Commit => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatement(
					"Cannot COMMIT without starting a transaction".to_string(),
				))));
			}
			TopLevelExpr::Cancel => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatement(
					"Cannot CANCEL without starting a transaction".to_string(),
				))));
			}
			TopLevelExpr::Kill(s) => {
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						err::Error::unreachable(
							"Tried to unfreeze a Context with multiple references",
						)
					})
					.map_err(anyhow::Error::new)?
					.set_transaction(txn);
				self.stack
					.enter(|stk| s.compute(stk, &self.ctx, &self.opt, None))
					.finish()
					.await
					.map_err(ControlFlow::Err)
			}
			TopLevelExpr::Live(s) => {
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						err::Error::unreachable(
							"Tried to unfreeze a Context with multiple references",
						)
					})
					.map_err(anyhow::Error::new)?
					.set_transaction(txn);
				self.stack
					.enter(|stk| s.compute(stk, &self.ctx, &self.opt, None))
					.finish()
					.await
					.map_err(ControlFlow::Err)
			}
			TopLevelExpr::Show(s) => {
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						err::Error::unreachable(
							"Tried to unfreeze a Context with multiple references",
						)
					})
					.map_err(anyhow::Error::new)?
					.set_transaction(txn);
				s.compute(&self.ctx, &self.opt, None).await.map_err(ControlFlow::Err)
			}
			TopLevelExpr::Analyze(s) => {
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						err::Error::unreachable(
							"Tried to unfreeze a Context with multiple references",
						)
					})
					.map_err(anyhow::Error::new)?
					.set_transaction(txn);
				s.compute(&self.ctx, &self.opt).await.map_err(ControlFlow::Err)
			}
			TopLevelExpr::Access(s) => {
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						err::Error::unreachable(
							"Tried to unfreeze a Context with multiple references",
						)
					})
					.map_err(anyhow::Error::new)?
					.set_transaction(txn);
				self.stack.enter(|stk| s.compute(stk, &self.ctx, &self.opt, None)).finish().await
			}
			// Process all other normal statements
			TopLevelExpr::Expr(e) => {
				// The transaction began successfully
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						Error::unreachable("Tried to unfreeze a Context with multiple references")
					})
					.map_err(anyhow::Error::new)?
					.set_transaction(txn);
				// Process the statement
				let res = self
					.stack
					.enter(|stk| e.compute(stk, &self.ctx, &self.opt, None))
					.finish()
					.await;
				self.check_slow_log(start, &e);
				res
			}
		};

		// Catch cancellation during running.
		match self.ctx.done(true)? {
			None => res,
			Some(Reason::Timedout) => Err(ControlFlow::from(anyhow::anyhow!(Error::QueryTimedout))),
			Some(Reason::Canceled) => {
				Err(ControlFlow::from(anyhow::anyhow!(Error::QueryCancelled)))
			}
		}
	}

	/// Execute a query not wrapped in a transaction block.
	async fn execute_bare_statement(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		stmt: TopLevelExpr,
	) -> Result<Value> {
		// Don't even try to run if the query should already be finished.
		match self.ctx.done(true)? {
			None => {}
			Some(Reason::Timedout) => {
				bail!(Error::QueryTimedout);
			}
			Some(Reason::Canceled) => {
				bail!(Error::QueryCancelled);
			}
		}

		self.execute_plan_impl(kvs, start, stmt).await
	}

	async fn execute_plan_impl(
		&mut self,
		kvs: &Datastore,
		start: &Instant,
		plan: TopLevelExpr,
	) -> Result<Value> {
		let transaction_type = if plan.read_only() {
			TransactionType::Read
		} else {
			TransactionType::Write
		};
		let txn = Arc::new(kvs.transaction(transaction_type, LockType::Optimistic).await?);
		let receiver = self.ctx.has_notifications().then(|| {
			let (send, recv) = async_channel::unbounded();
			self.opt.sender = Some(send);
			recv
		});

		match self.execute_plan_in_transaction(txn.clone(), start, plan).await {
			Ok(value) | Err(ControlFlow::Return(value)) => {
				let mut lock = txn.lock().await;

				// non-writable transactions might return an error on commit.
				// So cancel them instead. This is fine since a non-writable transaction
				// has nothing to commit anyway.
				if let TransactionType::Read = transaction_type {
					let _ = lock.cancel().await;
					return Ok(value);
				}

				if let Err(e) = lock.complete_changes(false).await {
					let _ = lock.cancel().await;

					bail!(Error::QueryNotExecutedDetail {
						message: e.to_string(),
					});
				}

				if let Err(e) = lock.commit().await {
					bail!(Error::QueryNotExecutedDetail {
						message: e.to_string(),
					});
				}

				// flush notifications.
				if let Some(recv) = receiver {
					self.opt.sender = None;
					if let Some(sink) = self.ctx.notifications() {
						spawn(async move {
							while let Ok(x) = recv.recv().await {
								if sink.send(x).await.is_err() {
									break;
								}
							}
						});
					}
				}

				Ok(value)
			}
			Err(ControlFlow::Continue) | Err(ControlFlow::Break) => {
				bail!(Error::InvalidControlFlow)
			}
			Err(ControlFlow::Err(e)) => {
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}

	/// Execute the begin statement and all statements after which are within a
	/// transaction block.
	async fn execute_begin_statement<S>(
		&mut self,
		kvs: &Datastore,
		mut stream: Pin<&mut S>,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelExpr>>,
	{
		let Ok(txn) = kvs.transaction(TransactionType::Write, LockType::Optimistic).await else {
			// couldn't create a transaction.
			// Fast forward until we hit CANCEL or COMMIT
			while let Some(stmt) = stream.next().await {
				yield_now!();
				let stmt = stmt?;
				if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
					return Ok(());
				}

				self.results.push(Response {
					time: Duration::ZERO,
					result: Err(anyhow!(Error::QueryNotExecuted)),
					query_type: QueryType::Other,
				});
			}

			// Ran out of statements but still didn't hit a COMMIT or CANCEL
			// Just break as we can't do anything else since the query is already
			// effectively canceled.
			return Ok(());
		};

		// Create a sender for this transaction only if the context allows for
		// notifications.
		let receiver = self.ctx.has_notifications().then(|| {
			let (send, recv) = async_channel::unbounded();
			self.opt.sender = Some(send);
			recv
		});

		let txn = Arc::new(txn);
		let start_results = self.results.len();
		let mut skip_remaining = false;

		// loop over the statements until we hit a cancel or a commit statement.
		while let Some(stmt) = stream.next().await {
			yield_now!();
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					// make sure the transaction is properly canceled.
					let _ = txn.cancel().await;
					return Err(e);
				}
			};

			// check for timeout and cancellation.
			if let Some(done) = self.ctx.done(true)? {
				// A cancellation happened. Cancel the transaction, fast-forward the remaining
				// results and then return.
				let _ = txn.cancel().await;

				for res in &mut self.results[start_results..] {
					res.query_type = QueryType::Other;
					res.result = Err(anyhow!(Error::QueryCancelled));
				}

				while let Some(stmt) = stream.next().await {
					yield_now!();
					let stmt = stmt?;
					if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
						return Ok(());
					}

					self.results.push(Response {
						time: Duration::ZERO,
						result: Err(match done {
							Reason::Timedout => anyhow!(Error::QueryTimedout),
							Reason::Canceled => anyhow!(Error::QueryCancelled),
						}),
						query_type: QueryType::Other,
					});
				}

				// Missing CANCEL/COMMIT statement, statement already canceled so nothing todo.
				return Ok(());
			}

			if skip_remaining && !matches!(stmt, TopLevelExpr::Cancel | TopLevelExpr::Commit) {
				continue;
			}

			trace!(target: TARGET, statement = %stmt, "Executing statement");

			let query_type = match stmt {
				TopLevelExpr::Live(_) => QueryType::Live,
				TopLevelExpr::Kill(_) => QueryType::Kill,
				_ => QueryType::Other,
			};

			let before = Instant::now();
			let value = match stmt {
				TopLevelExpr::Begin => {
					let _ = txn.cancel().await;
					// tried to begin a transaction within a transaction.

					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(anyhow!(Error::QueryNotExecuted));
					}

					self.results.push(Response {
						time: Duration::ZERO,
						result: Err(anyhow!(Error::QueryNotExecutedDetail {
							message:
								"Tried to start a transaction while another transaction was open"
									.to_string(),
						})),
						query_type: QueryType::Other,
					});

					self.opt.sender = None;

					while let Some(stmt) = stream.next().await {
						yield_now!();
						let stmt = stmt?;
						if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
							return Ok(());
						}

						self.results.push(Response {
							time: Duration::ZERO,
							result: Err(anyhow!(Error::QueryNotExecuted)),
							query_type: QueryType::Other,
						});
					}

					// Missing CANCEL/COMMIT statement, statement already canceled so nothing todo.
					return Ok(());
				}
				TopLevelExpr::Cancel => {
					let _ = txn.cancel().await;

					// update the results indicating cancelation.
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(anyhow!(Error::QueryCancelled));
					}

					self.opt.sender = None;

					return Ok(());
				}
				TopLevelExpr::Commit => {
					let mut lock = txn.lock().await;

					// complete_changes and then commit.
					// If either error undo results.
					let e = if let Err(e) = lock.complete_changes(false).await {
						let _ = lock.cancel().await;
						e
					} else if let Err(e) = lock.commit().await {
						e
					} else {
						// Successfully commited. everything is fine.

						// flush notifications.
						if let Some(recv) = receiver {
							self.opt.sender = None;
							if let Some(sink) = self.ctx.notifications() {
								spawn(async move {
									while let Ok(x) = recv.recv().await {
										if sink.send(x).await.is_err() {
											break;
										}
									}
								});
							}
						}

						return Ok(());
					};

					// failed to commit
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(anyhow!(Error::QueryNotExecutedDetail {
							message: e.to_string(),
						}));
					}

					self.opt.sender = None;

					return Ok(());
				}
				TopLevelExpr::Option(stmt) => match self.execute_option_statement(stmt) {
					Ok(_) => {
						// skip adding the value as executing an option statement doesn't produce
						// results
						continue;
					}
					Err(e) => Err(e),
				},
				stmt => {
					skip_remaining = matches!(stmt, TopLevelExpr::Expr(Expr::Return(_)));

					// reintroduce planner later.
					let plan = stmt;

					let r = match self.execute_plan_in_transaction(txn.clone(), &before, plan).await
					{
						Ok(x) => Ok(x),
						Err(ControlFlow::Return(value)) => {
							skip_remaining = true;
							Ok(value)
						}
						Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
							Err(anyhow!(Error::InvalidControlFlow))
						}
						Err(ControlFlow::Err(e)) => {
							for res in &mut self.results[start_results..] {
								res.query_type = QueryType::Other;
								res.result = Err(anyhow!(Error::QueryNotExecuted));
							}

							// statement return an error. Consume all the other statement until we
							// hit a cancel or commit.
							self.results.push(Response {
								time: before.elapsed(),
								result: Err(e),
								query_type,
							});

							let _ = txn.cancel().await;

							self.opt.sender = None;

							while let Some(stmt) = stream.next().await {
								yield_now!();
								let stmt = stmt?;
								if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
									return Ok(());
								}

								self.results.push(Response {
									time: Duration::ZERO,
									result: Err(anyhow!(Error::QueryNotExecuted)),
									query_type: QueryType::Other,
								});
							}

							// ran out of statements before the transaction ended.
							// Just break as we have nothing else we can do.
							return Ok(());
						}
					};

					if skip_remaining {
						// If we skip the next values due to return then we need to clear the other
						// results.
						self.results.truncate(start_results)
					}

					r
				}
			};

			self.results.push(Response {
				time: before.elapsed(),
				result: value,
				query_type,
			});
		}

		// we ran out of query but we still have an open transaction.
		// Be conservative and treat this essentially as a CANCEL statement.
		let _ = txn.cancel().await;

		for res in &mut self.results[start_results..] {
			res.query_type = QueryType::Other;
			res.result = Err(anyhow!(Error::QueryNotExecutedDetail {
				message: "Missing COMMIT statement".to_string(),
			}));
		}

		self.opt.sender = None;

		Ok(())
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute(
		kvs: &Datastore,
		ctx: Context,
		opt: Options,
		qry: Ast,
	) -> Result<Vec<Response>> {
		let stream = futures::stream::iter(qry.expressions.into_iter().map(Ok));
		Self::execute_stream(kvs, ctx, opt, false, stream).await
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute_plan(
		kvs: &Datastore,
		ctx: Context,
		opt: Options,
		qry: LogicalPlan,
	) -> Result<Vec<Response>> {
		let stream = futures::stream::iter(qry.expressions.into_iter().map(Ok));
		Self::execute_expr_stream(kvs, ctx, opt, false, stream).await
	}

	pub async fn execute_expr(
		kvs: &Datastore,
		ctx: Context,
		opt: Options,
		plan: TopLevelExpr,
	) -> Result<Vec<Response>> {
		Self::execute_expr_stream(kvs, ctx, opt, false, stream::once(async { Ok(plan) })).await
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute_stream<S>(
		kvs: &Datastore,
		ctx: Context,
		opt: Options,
		skip_success_results: bool,
		stream: S,
	) -> Result<Vec<Response>>
	where
		S: Stream<Item = Result<sql::TopLevelExpr>>,
	{
		Self::execute_expr_stream(
			kvs,
			ctx,
			opt,
			skip_success_results,
			stream.map(|x| x.map(expr::TopLevelExpr::from)),
		)
		.await
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub async fn execute_expr_stream<S>(
		kvs: &Datastore,
		ctx: Context,
		opt: Options,
		skip_success_results: bool,
		stream: S,
	) -> Result<Vec<Response>>
	where
		S: Stream<Item = Result<TopLevelExpr>>,
	{
		let mut this = Executor::new(ctx, opt);
		let mut stream = pin!(stream);

		while let Some(stmt) = stream.next().await {
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					this.results.push(Response {
						time: Duration::ZERO,
						result: Err(e),
						query_type: QueryType::Other,
					});

					return Ok(this.results);
				}
			};

			match stmt {
				TopLevelExpr::Option(stmt) => this.execute_option_statement(stmt)?,
				// handle option here because it doesn't produce a result.
				TopLevelExpr::Begin => {
					if let Err(e) = this.execute_begin_statement(kvs, stream.as_mut()).await {
						this.results.push(Response {
							time: Duration::ZERO,
							result: Err(e),
							query_type: QueryType::Other,
						});

						return Ok(this.results);
					}
				}
				stmt => {
					let query_type: QueryType = QueryType::for_toplevel_expr(&stmt);

					let now = Instant::now();
					let result = this.execute_bare_statement(kvs, &now, stmt).await;
					if !skip_success_results || result.is_err() {
						this.results.push(Response {
							time: now.elapsed(),
							result,
							query_type,
						});
					}
				}
			}
			yield_now!();
		}
		Ok(this.results)
	}
}

#[cfg(test)]
mod tests {
	use crate::dbs::Session;
	use crate::iam::{Level, Role};
	use crate::kvs::Datastore;

	#[tokio::test]
	async fn check_execute_option_permissions() {
		let tests = vec![
			// Root level
			(
				Session::for_level(Level::Root, Role::Owner).with_ns("NS").with_db("DB"),
				true,
				"owner at root level should be able to set options",
			),
			(
				Session::for_level(Level::Root, Role::Editor).with_ns("NS").with_db("DB"),
				true,
				"editor at root level should be able to set options",
			),
			(
				Session::for_level(Level::Root, Role::Viewer).with_ns("NS").with_db("DB"),
				false,
				"viewer at root level should not be able to set options",
			),
			// Namespace level
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Owner)
					.with_ns("NS")
					.with_db("DB"),
				true,
				"owner at namespace level should be able to set options on its namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Owner)
					.with_ns("OTHER_NS")
					.with_db("DB"),
				false,
				"owner at namespace level should not be able to set options on another namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Editor)
					.with_ns("NS")
					.with_db("DB"),
				true,
				"editor at namespace level should be able to set options on its namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Editor)
					.with_ns("OTHER_NS")
					.with_db("DB"),
				false,
				"editor at namespace level should not be able to set options on another namespace",
			),
			(
				Session::for_level(Level::Namespace("NS".to_string()), Role::Viewer)
					.with_ns("NS")
					.with_db("DB"),
				false,
				"viewer at namespace level should not be able to set options on its namespace",
			),
			// Database level
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Owner,
				)
				.with_ns("NS")
				.with_db("DB"),
				true,
				"owner at database level should be able to set options on its database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Owner,
				)
				.with_ns("NS")
				.with_db("OTHER_DB"),
				false,
				"owner at database level should not be able to set options on another database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Owner,
				)
				.with_ns("OTHER_NS")
				.with_db("DB"),
				false,
				"owner at database level should not be able to set options on another namespace even if the database name matches",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Editor,
				)
				.with_ns("NS")
				.with_db("DB"),
				true,
				"editor at database level should be able to set options on its database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Editor,
				)
				.with_ns("NS")
				.with_db("OTHER_DB"),
				false,
				"editor at database level should not be able to set options on another database",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Editor,
				)
				.with_ns("OTHER_NS")
				.with_db("DB"),
				false,
				"editor at database level should not be able to set options on another namespace even if the database name matches",
			),
			(
				Session::for_level(
					Level::Database("NS".to_string(), "DB".to_string()),
					Role::Viewer,
				)
				.with_ns("NS")
				.with_db("DB"),
				false,
				"viewer at database level should not be able to set options on its database",
			),
		];
		let statement = "OPTION IMPORT = false";

		for test in tests.iter() {
			let (session, should_succeed, msg) = test;

			{
				let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(true);

				let res = ds.execute(statement, session, None).await;

				if *should_succeed {
					assert!(res.is_ok(), "{}: {:?}", msg, res);
				} else {
					let err = res.unwrap_err().to_string();
					assert!(
						err.contains("Not enough permissions to perform this action"),
						"{}: {}",
						msg,
						err
					)
				}
			}
		}

		// Anonymous with auth enabled
		{
			let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(true);

			let res =
				ds.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None).await;

			let err = res.unwrap_err().to_string();
			assert!(
				err.contains("Not enough permissions to perform this action"),
				"anonymous user should not be able to set options: {}",
				err
			)
		}

		// Anonymous with auth disabled
		{
			let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(false);

			let res =
				ds.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None).await;

			assert!(
				res.is_ok(),
				"anonymous user should be able to set options when auth is disabled: {:?}",
				res
			)
		}
	}

	#[tokio::test]
	async fn check_execute_timeout() {
		// With small timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 2s";
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with small timeout: {:?}", res);
		}
		// With large timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 31540000s"; // 1 year
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with large timeout: {:?}", res);
		}
		// With very large timeout
		{
			let ds = Datastore::new("memory").await.unwrap();
			let stmt = "UPDATE test TIMEOUT 9460800000000000000s"; // 300 billion years
			let res = ds.execute(stmt, &Session::default().with_ns("NS").with_db("DB"), None).await;
			assert!(res.is_ok(), "Failed to execute statement with very large timeout: {:?}", res);
			let err = res.unwrap()[0].result.as_ref().unwrap_err().to_string();
			assert!(
				err.contains("Invalid timeout"),
				"Expected to find invalid timeout error: {:?}",
				err
			);
		}
	}
}
