use std::pin::{Pin, pin};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use futures::{Stream, StreamExt};
use reblessive::TreeStack;
use surrealdb_types::ToSql;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tracing::instrument;
use trice::Instant;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

use crate::catalog::providers::{CatalogProvider, NamespaceProvider, RootProvider};
use crate::ctx::FrozenContext;
use crate::ctx::reason::Reason;
use crate::dbs::response::QueryResult;
use crate::dbs::{Force, Options, QueryType};
use crate::doc::DefaultBroker;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::paths::{DB, NS};
use crate::expr::plan::LogicalPlan;
use crate::expr::statements::{OptionStatement, UseStatement};
use crate::expr::{Base, ControlFlow, Expr, FlowResult, TopLevelExpr};
use crate::iam::{Action, ResourceKind};
use crate::kvs::slowlog::SlowLogVisit;
use crate::kvs::{Datastore, LockType, Transaction, TransactionType};
use crate::rpc::DbResultError;
use crate::types::PublicNotification;
use crate::val::{Array, Value, convert_value_to_public_value};
use crate::{err, expr, sql};

const TARGET: &str = "surrealdb::core::dbs";

/// An executor which relies on the `compute` methods of the logical expressions.
pub struct ComputeExecutor {
	stack: TreeStack,
	results: Vec<QueryResult>,
	opt: Options,
	ctx: FrozenContext,
}

impl ComputeExecutor {
	fn prepare_broker(&mut self) -> Option<async_channel::Receiver<PublicNotification>> {
		if !self.ctx.has_notifications() {
			return None;
		}
		// If a broker is already provided by a higher layer, don't override it here.
		if self.opt.broker.is_some() {
			return None;
		}
		let (send, recv) = async_channel::unbounded();
		self.opt.broker = Some(DefaultBroker::new(send));
		Some(recv)
	}
}

impl ComputeExecutor {
	pub fn new(ctx: FrozenContext, opt: Options) -> Self {
		ComputeExecutor {
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

	/// If slow logging is configured in the current context, evaluate whether the
	/// statement exceeded the slow threshold and emit a log entry if so.
	///
	/// Generic over `S` to accept both concrete statements and wrappers that
	/// implement `Display` and `VisitExpression`.
	fn check_slow_log<S: SlowLogVisit + ToSql>(&self, start: &Instant, stm: &S) {
		if let Some(slow_log) = self.ctx.slow_log() {
			slow_log.check_log(&self.ctx, start, stm);
		}
	}

	/// Extract session information from the FrozenContext.
	///
	/// The session is stored as a Value object in the context with keys like
	/// "ns", "db", "id", "ip", "or", "ac", "rd", "tk".
	fn extract_session_info(&self) -> Option<std::sync::Arc<crate::exec::context::SessionInfo>> {
		use crate::exec::context::SessionInfo;
		use crate::expr::paths::{AC, DB, ID, IP, NS, OR, RD, TK};

		let session_value = self.ctx.value("session")?;

		// Extract fields from the session Value
		let ns = match session_value.pick(NS.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let db = match session_value.pick(DB.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let id = match session_value.pick(ID.as_ref()) {
			Value::Uuid(u) => Some(*u.as_ref()),
			_ => None,
		};

		let ip = match session_value.pick(IP.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let origin = match session_value.pick(OR.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let ac = match session_value.pick(AC.as_ref()) {
			Value::String(s) => Some(s),
			_ => None,
		};

		let rd = match session_value.pick(RD.as_ref()) {
			Value::None => None,
			v => Some(v),
		};

		let token = match session_value.pick(TK.as_ref()) {
			Value::None => None,
			v => Some(v),
		};

		// Note: exp is not in the session object, it's in the Session struct
		// For now, we leave it as None
		let exp = None;

		Some(std::sync::Arc::new(SessionInfo {
			ns,
			db,
			id,
			ip,
			origin,
			ac,
			rd,
			token,
			exp,
		}))
	}

	/// Execute an OperatorPlan and collect results into a Value.
	///
	/// This builds an ExecutionContext from the current session state and executes
	/// the streaming operator plan, collecting all results into an array.
	async fn execute_operator_plan(
		&self,
		plan: Arc<dyn crate::exec::ExecOperator>,
		txn: Arc<Transaction>,
	) -> FlowResult<Value> {
		use tokio_util::sync::CancellationToken;

		use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
		use crate::exec::context::{
			DatabaseContext, ExecutionContext, NamespaceContext, Parameters, RootContext,
		};

		// Build parameters from the context values
		// This collects all query parameters from the FrozenContext chain,
		// excluding protected/system parameters (access, auth, token, session).
		let params: Arc<Parameters> = Arc::new(self.ctx.collect_params());

		// Extract session info from the FrozenContext
		let session = self.extract_session_info();

		// Get capabilities from the FrozenContext
		let capabilities = Some(self.ctx.get_capabilities());

		// Build the root context
		let root_ctx = RootContext {
			datastore: None,
			params,
			cancellation: CancellationToken::new(),
			auth: self.opt.auth.clone(),
			auth_enabled: self.opt.auth_enabled,
			txn: txn.clone(),
			session,
			capabilities,
			// Include Options for fallback to legacy compute path
			options: Some(self.opt.clone()),
			// Include the FrozenContext for operators that need to call legacy compute methods
			ctx: self.ctx.clone(),
		};

		// Check what level of context we need
		let required_level = plan.required_context();

		let exec_ctx = match required_level {
			crate::exec::context::ContextLevel::Root => ExecutionContext::Root(root_ctx),
			crate::exec::context::ContextLevel::Namespace => {
				// Get namespace definition
				let ns_name = self.opt.ns()?;
				let ns_def = txn.get_or_add_ns(None, ns_name).await?;
				ExecutionContext::Namespace(NamespaceContext {
					root: root_ctx,
					ns: ns_def,
				})
			}
			crate::exec::context::ContextLevel::Database => {
				// Get namespace and database definitions
				let ns_name = self.opt.ns()?;
				let db_name = self.opt.db()?;
				let ns_def = txn.get_or_add_ns(None, ns_name).await?;
				let db_def = txn.get_or_add_db_upwards(None, ns_name, db_name, true).await?;
				ExecutionContext::Database(DatabaseContext {
					ns_ctx: NamespaceContext {
						root: root_ctx,
						ns: ns_def,
					},
					db: db_def,
				})
			}
		};

		// Execute the plan
		// Handle control flow signals from execute()
		let stream = match plan.execute(&exec_ctx) {
			Ok(s) => s,
			Err(crate::expr::ControlFlow::Return(v)) => {
				// RETURN - propagate as control flow signal
				return Err(ControlFlow::Return(v));
			}
			Err(crate::expr::ControlFlow::Break) => {
				return Err(ControlFlow::Break);
			}
			Err(crate::expr::ControlFlow::Continue) => {
				return Err(ControlFlow::Continue);
			}
			Err(crate::expr::ControlFlow::Err(e)) => {
				return Err(ControlFlow::Err(e));
			}
		};

		// Collect all results
		let mut results = Vec::new();
		futures::pin_mut!(stream);
		while let Some(batch_result) = stream.next().await {
			match batch_result {
				Ok(batch) => {
					results.extend(batch.values);
				}
				Err(crate::expr::ControlFlow::Err(e)) => {
					return Err(ControlFlow::Err(e));
				}
				Err(crate::expr::ControlFlow::Return(v)) => {
					// RETURN - propagate as control flow signal
					return Err(ControlFlow::Return(v));
				}
				Err(crate::expr::ControlFlow::Break) => {
					return Err(ControlFlow::Break);
				}
				Err(crate::expr::ControlFlow::Continue) => {
					return Err(ControlFlow::Continue);
				}
			}
		}

		// Return results as an array if it's a query, or the scalar value if it's a scalar plan
		if plan.is_scalar() && results.len() == 1 {
			Ok(results.pop().unwrap())
		} else {
			Ok(Value::Array(Array::from(results)))
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
		/// Helper method to get mutable access to the context
		macro_rules! ctx_mut {
			() => {
				Arc::get_mut(&mut self.ctx)
					.ok_or_else(|| {
						Error::unreachable("Tried to unfreeze a Context with multiple references")
					})
					.map_err(anyhow::Error::new)?
			};
		}
		let res = match plan {
			TopLevelExpr::Use(stmt) => {
				let opt_ref = self.opt.clone();

				let (use_ns, use_db) = match stmt {
					UseStatement::Default => {
						if let Some(x) = txn.get_default_config().await? {
							(x.namespace.clone(), x.database.clone())
						} else {
							(None, None)
						}
					}
					UseStatement::Ns(ns) => {
						let ns = self
							.stack
							.enter(|stk| {
								expr_to_ident(stk, &self.ctx, &opt_ref, None, &ns, "namespace")
							})
							.finish()
							.await?;

						(Some(ns), None)
					}
					UseStatement::Db(db) => {
						let db = self
							.stack
							.enter(|stk| {
								expr_to_ident(stk, &self.ctx, &opt_ref, None, &db, "database")
							})
							.finish()
							.await?;

						(None, Some(db))
					}
					UseStatement::NsDb(ns, db) => {
						let ns = self
							.stack
							.enter(|stk| {
								expr_to_ident(stk, &self.ctx, &opt_ref, None, &ns, "namespace")
							})
							.finish()
							.await?;

						let db = self
							.stack
							.enter(|stk| {
								expr_to_ident(stk, &self.ctx, &opt_ref, None, &db, "database")
							})
							.finish()
							.await?;

						(Some(ns), Some(db))
					}
				};

				let ctx = ctx_mut!();

				// Apply new namespace
				if let Some(ns) = use_ns {
					txn.get_or_add_ns(Some(ctx), &ns).await?;

					let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
					self.opt.set_ns(Some(ns.as_str().into()));
					session.put(NS.as_ref(), ns.into());
					ctx.add_value("session", session.into());
				}

				// Apply new database
				if let Some(db) = use_db {
					let Some(ns) = &self.opt.ns else {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"Cannot use database without namespace"
						)));
					};

					txn.ensure_ns_db(Some(ctx), ns, &db).await?;

					let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
					self.opt.set_db(Some(db.as_str().into()));
					session.put(DB.as_ref(), db.into());
					ctx.add_value("session", session.into());
				}

				// Return the current namespace and database
				Ok(Value::from(map! {
					"namespace".to_string() => self.opt.ns.clone().map(|x| Value::String(x.to_string())).unwrap_or(Value::None),
					"database".to_string() => self.opt.db.clone().map(|x| Value::String(x.to_string())).unwrap_or(Value::None),
				}))
			}
			TopLevelExpr::Option(_) => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::unreachable(
					"TopLevelExpr::Option should have been handled by a calling function",
				))));
			}

			TopLevelExpr::Expr(Expr::Let(stm)) => {
				// Avoid moving in and out of the context via Arc::get_mut
				ctx_mut!().set_transaction(txn);

				// Run the statement
				let res = self
					.stack
					.enter(|stk| stm.what.compute(stk, &self.ctx, &self.opt, None))
					.finish()
					.await;

				let res = res?;
				let result = match &stm.kind {
					Some(kind) => res
						.coerce_to_kind(kind)
						.map_err(|e| Error::SetCoerce {
							name: stm.name.clone(),
							error: Box::new(e),
						})
						.map_err(anyhow::Error::new)?,
					None => res,
				};

				if stm.is_protected_set() {
					return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidParam {
						name: stm.name.clone(),
					})));
				}
				// Set the parameter
				ctx_mut!().add_value(stm.name.clone(), result.into());

				// Check if we dump the slow log
				self.check_slow_log(start, stm.as_ref());
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
				ctx_mut!().set_transaction(txn);
				self.stack
					.enter(|stk| s.compute(stk, &self.ctx, &self.opt, None))
					.finish()
					.await
					.map_err(ControlFlow::Err)
			}
			TopLevelExpr::Show(s) => {
				ctx_mut!().set_transaction(txn);
				s.compute(&self.ctx, &self.opt, None).await.map_err(ControlFlow::Err)
			}
			TopLevelExpr::Access(s) => {
				ctx_mut!().set_transaction(txn);
				self.stack.enter(|stk| s.compute(stk, &self.ctx, &self.opt, None)).finish().await
			}
			// Process all other normal statements
			TopLevelExpr::Expr(e) => {
				// Try the new streaming execution path first
				match crate::exec::planner::try_plan_expr(e.clone(), &self.ctx) {
					Ok(plan) => {
						// Set the transaction on the context
						ctx_mut!().set_transaction(txn.clone());

						// Build execution context and execute the plan
						let exec_result = self.execute_operator_plan(plan, txn.clone()).await;

						self.check_slow_log(start, &e);

						// exec_result is now FlowResult<Value>, propagate directly
						exec_result
					}
					Err(Error::Unimplemented(_)) => {
						// Fallback to existing compute path
						ctx_mut!().set_transaction(txn);
						let res = self
							.stack
							.enter(|stk| e.compute(stk, &self.ctx, &self.opt, None))
							.finish()
							.await;
						self.check_slow_log(start, &e);
						res
					}
					Err(e) => Err(ControlFlow::Err(anyhow::Error::new(e))),
				}
			}
		};

		// Catch cancellation during running.
		match self.ctx.done(true)? {
			None => res,
			Some(Reason::Timedout(d)) => {
				Err(ControlFlow::from(anyhow::anyhow!(Error::QueryTimedout(d))))
			}
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
			Some(Reason::Timedout(d)) => {
				bail!(Error::QueryTimedout(d));
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
		let receiver = self.prepare_broker();

		match self.execute_plan_in_transaction(txn.clone(), start, plan).await {
			Ok(value) | Err(ControlFlow::Return(value)) => {
				// non-writable transactions might return an error on commit.
				// So cancel them instead. This is fine since a non-writable transaction
				// has nothing to commit anyway.
				if let TransactionType::Read = transaction_type {
					let _ = txn.cancel().await;
					return Ok(value);
				}

				if let Err(e) = txn.commit().await {
					bail!(Error::QueryNotExecuted {
						message: e.to_string(),
					});
				}

				// flush notifications.
				if let Some(recv) = receiver {
					self.opt.broker = None;
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

				self.results.push(QueryResult {
					time: Duration::ZERO,
					result: Err(DbResultError::QueryNotExecuted(
						"Tried to start a transaction while another transaction was open"
							.to_string(),
					)),
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
		let receiver = self.prepare_broker();

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
					res.result = Err(DbResultError::QueryCancelled);
				}

				while let Some(stmt) = stream.next().await {
					yield_now!();
					let stmt = stmt?;
					if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
						return Ok(());
					}

					self.results.push(QueryResult {
						time: Duration::ZERO,
						result: Err(match done {
							Reason::Timedout(d) => {
								DbResultError::QueryTimedout(format!("Timed out: {d}"))
							}
							Reason::Canceled => DbResultError::QueryCancelled,
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

			trace!(target: TARGET, statement = %stmt.to_sql(), "Executing statement");

			let query_type = match stmt {
				TopLevelExpr::Live(_) => QueryType::Live,
				TopLevelExpr::Kill(_) => QueryType::Kill,
				_ => QueryType::Other,
			};

			let before = Instant::now();
			let result = match stmt {
				TopLevelExpr::Begin => {
					let _ = txn.cancel().await;
					// tried to begin a transaction within a transaction.

					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(DbResultError::QueryNotExecuted(format!(
							"The query was not executed due to a failed transaction: {}",
							stmt.to_sql()
						)));
					}

					self.results.push(QueryResult {
						time: Duration::ZERO,
						result: Err(DbResultError::InternalError(
							"Tried to start a transaction while another transaction was open"
								.to_string(),
						)),
						query_type: QueryType::Other,
					});

					self.opt.broker = None;

					while let Some(stmt) = stream.next().await {
						yield_now!();
						let stmt = stmt?;
						if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
							return Ok(());
						}

						self.results.push(QueryResult {
							time: Duration::ZERO,
							result: Err(DbResultError::QueryNotExecuted(format!(
								"The query was not executed due to a failed transaction: {}",
								stmt.to_sql()
							))),
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
						res.result = Err(DbResultError::QueryCancelled);
					}

					self.opt.broker = None;

					// CANCEL returns NONE
					self.results.push(QueryResult {
						time: before.elapsed(),
						result: Ok(convert_value_to_public_value(Value::None)?),
						query_type: QueryType::Other,
					});

					return Ok(());
				}
				TopLevelExpr::Commit => {
					// Commit the transaction.
					// If error undo results.
					let e = if let Err(e) = txn.commit().await {
						e
					} else {
						// Successfully commited. everything is fine.

						// flush notifications.
						if let Some(recv) = receiver {
							self.opt.broker = None;
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

						// COMMIT returns NONE
						self.results.push(QueryResult {
							time: before.elapsed(),
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});

						return Ok(());
					};

					// failed to commit
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result =
							Err(DbResultError::InternalError(format!("Query not executed: {}", e)));
					}

					self.opt.broker = None;

					return Ok(());
				}
				TopLevelExpr::Option(stmt) => match self.execute_option_statement(stmt) {
					Ok(_) => {
						// OPTION returns NONE
						self.results.push(QueryResult {
							time: before.elapsed(),
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});
						continue;
					}
					Err(e) => Err(DbResultError::InternalError(e.to_string())),
				},
				stmt => {
					// reintroduce planner later.
					let plan = stmt;

					let r =
						match self.execute_plan_in_transaction(txn.clone(), &before, plan).await {
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
									res.result = Err(DbResultError::QueryNotExecuted(
										"The query was not executed due to a failed transaction"
											.to_string(),
									));
								}

								// statement return an error. Consume all the other statement until
								// we hit a cancel or commit.
								self.results.push(QueryResult {
									time: before.elapsed(),
									result: Err(DbResultError::InternalError(e.to_string())),
									query_type,
								});

								let _ = txn.cancel().await;

								self.opt.broker = None;

								while let Some(stmt) = stream.next().await {
									yield_now!();
									let stmt = stmt?;
									if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
										return Ok(());
									}

									self.results.push(QueryResult {
										time: Duration::ZERO,
										result: Err(DbResultError::QueryNotExecuted(
												"The query was not executed due to a cancelled transaction".to_string(),
										)),
										query_type: QueryType::Other,
									});
								}

								// ran out of statements before the transaction ended.
								// Just break as we have nothing else we can do.
								return Ok(());
							}
						};

					match r {
						Ok(value) => Ok(convert_value_to_public_value(value)?),
						Err(err) => Err(DbResultError::InternalError(err.to_string())),
					}
				}
			};

			self.results.push(QueryResult {
				time: before.elapsed(),
				result,
				query_type,
			});
		}

		// we ran out of query but we still have an open transaction.
		// Be conservative and treat this essentially as a CANCEL statement.
		let _ = txn.cancel().await;

		for res in &mut self.results[start_results..] {
			res.query_type = QueryType::Other;
			res.result = Err(DbResultError::InternalError("Missing COMMIT statement".to_string()));
		}

		self.opt.broker = None;

		Ok(())
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub(crate) async fn execute_plan(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		plan: LogicalPlan,
	) -> Result<Vec<QueryResult>> {
		let stream = futures::stream::iter(plan.expressions.into_iter().map(Ok));
		Self::execute_expr_stream(kvs, ctx, opt, false, stream).await
	}

	/// Execute a logical plan with an existing transaction
	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub(crate) async fn execute_plan_with_transaction(
		ctx: FrozenContext,
		opt: Options,
		plan: LogicalPlan,
	) -> Result<Vec<QueryResult>> {
		// The transaction is already set in the context
		// Execute each expression with the transaction
		let tx = ctx.tx();
		let mut executor = Self::new(ctx, opt);
		let mut results = Vec::new();

		for expr in plan.expressions {
			let start = Instant::now();
			let result = executor.execute_plan_in_transaction(tx.clone(), &start, expr).await;

			let time = start.elapsed();
			let query_result = match result {
				Ok(value) | Err(ControlFlow::Return(value)) => QueryResult {
					time,
					result: crate::val::convert_value_to_public_value(value)
						.map_err(|e| crate::rpc::DbResultError::InternalError(e.to_string())),
					query_type: QueryType::Other,
				},
				Err(ControlFlow::Err(e)) => QueryResult {
					time,
					result: Err(DbResultError::InternalError(e.to_string())),
					query_type: QueryType::Other,
				},
				Err(ControlFlow::Continue) | Err(ControlFlow::Break) => QueryResult {
					time,
					result: Err(DbResultError::InternalError("Invalid control flow".to_string())),
					query_type: QueryType::Other,
				},
			};
			results.push(query_result);
		}

		Ok(results)
	}

	#[instrument(level = "debug", name = "executor", target = "surrealdb::core::dbs", skip_all)]
	pub(crate) async fn execute_stream<S>(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		skip_success_results: bool,
		stream: S,
	) -> Result<Vec<QueryResult>>
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
	pub(crate) async fn execute_expr_stream<S>(
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		skip_success_results: bool,
		stream: S,
	) -> Result<Vec<QueryResult>>
	where
		S: Stream<Item = Result<TopLevelExpr>>,
	{
		let mut this = ComputeExecutor::new(ctx, opt);
		let mut stream = pin!(stream);

		while let Some(stmt) = stream.next().await {
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					this.results.push(QueryResult {
						time: Duration::ZERO,
						result: Err(DbResultError::InternalError(e.to_string())),
						query_type: QueryType::Other,
					});

					return Ok(this.results);
				}
			};

			match stmt {
				TopLevelExpr::Option(stmt) => {
					this.execute_option_statement(stmt)?;
					// OPTION returns NONE
					this.results.push(QueryResult {
						time: Duration::ZERO,
						result: Ok(convert_value_to_public_value(Value::None)?),
						query_type: QueryType::Other,
					});
				}
				TopLevelExpr::Begin => {
					// BEGIN returns NONE
					this.results.push(QueryResult {
						time: Duration::ZERO,
						result: Ok(convert_value_to_public_value(Value::None)?),
						query_type: QueryType::Other,
					});

					if let Err(e) = this.execute_begin_statement(kvs, stream.as_mut()).await {
						this.results.push(QueryResult {
							time: Duration::ZERO,
							result: Err(DbResultError::InternalError(e.to_string())),
							query_type: QueryType::Other,
						});

						return Ok(this.results);
					}
				}
				stmt => {
					let query_type: QueryType = QueryType::for_toplevel_expr(&stmt);

					let now = Instant::now();
					let result = this.execute_bare_statement(kvs, &now, stmt).await;
					let result = match result {
						Ok(value) => Ok(convert_value_to_public_value(value)?),
						Err(err) => Err(DbResultError::InternalError(err.to_string())),
					};
					if !skip_success_results || result.is_err() {
						this.results.push(QueryResult {
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
