use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use futures::{Stream, StreamExt};
use reblessive::TreeStack;
use surrealdb_types::{Error as TypesError, QueryError, ToSql};
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tracing::instrument;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;
use web_time::Instant;

use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, RootProvider,
};
use crate::ctx::reason::Reason;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::response::QueryResult;
use crate::dbs::{Force, MessageBroker, Options, QueryType, RoutedNotification, StatementCounters};
use crate::doc::DefaultBroker;
use crate::err::Error;
use crate::exec::planner::try_plan_expr;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::paths::{DB, NS};
use crate::expr::plan::LogicalPlan;
use crate::expr::statements::{OptionStatement, UseStatement};
use crate::expr::{Base, ControlFlow, Expr, FlowResult, TopLevelExpr};
use crate::iam::{Action, ResourceKind};
use crate::kvs::slowlog::SlowLogVisit;
use crate::kvs::{Datastore, LockType, Transaction, TransactionType};
use crate::observe::{
	Outcome, QueryCounters, QueryEvent, QueryEventSafe, StatementEvent, StatementEventCtx,
	StatementEventSafe, StatementType,
};
use crate::rpc::types_error_from_anyhow;
use crate::val::{Array, Value, convert_value_to_public_value};
use crate::{err, expr, sql};

const TARGET: &str = "surrealdb::core::dbs";

struct PreparedBroker {
	receiver: async_channel::Receiver<RoutedNotification>,
	delivery: Arc<dyn MessageBroker>,
}

/// An executor which relies on the `compute` methods of the logical expressions.
pub struct Executor {
	stack: TreeStack,
	results: Vec<QueryResult>,
	opt: Options,
	ctx: FrozenContext,
	/// Cached session info to avoid re-extracting from context on every query.
	/// Session values don't change between statements in the same executor batch.
	cached_session: Option<Arc<crate::exec::context::SessionInfo>>,
	/// Set when [`Executor::prepare_broker`] installs a [`DefaultBroker`]. Left false when a
	/// broker was already present (higher layer) or this statement skipped installation.
	/// Drives conditional [`clear_broker`] so we never remove an externally supplied broker.
	broker_owned_by_executor: bool,
}

impl Executor {
	/// Install a per-statement notification broker when `writable` is true and the session has
	/// notifications enabled. Read-only bare statements skip installation — they cannot emit
	/// LIVE/KILL notifications — avoiding allocation and stale-broker leaks on the hot path.
	#[inline]
	fn prepare_broker(
		&mut self,
		writable: bool,
		delivery: Option<Arc<dyn MessageBroker>>,
	) -> Option<PreparedBroker> {
		if !writable {
			return None;
		}
		let delivery = delivery?;
		// If a broker is already provided by a higher layer, don't override it here.
		if self.ctx.broker().is_some() {
			return None;
		}
		let (send, recv) = async_channel::unbounded();
		let Some(ctx) = Arc::get_mut(&mut self.ctx) else {
			debug_assert!(false, "prepare_broker: ctx Arc was contended at statement boundary");
			static WARNED: AtomicBool = AtomicBool::new(false);
			if !WARNED.swap(true, Ordering::Relaxed) {
				tracing::warn!(
					target: TARGET,
					"prepare_broker: ctx Arc contended at statement boundary; \
					 LIVE notifications for this batch will be skipped. This is a bug — \
					 future occurrences will not log."
				);
			}
			return None;
		};
		ctx.set_broker(Some(DefaultBroker::new(send, Arc::clone(&delivery))));
		self.broker_owned_by_executor = true;
		Some(PreparedBroker {
			receiver: recv,
			delivery,
		})
	}

	fn flush_live_query_notifications(prepared: PreparedBroker) {
		let PreparedBroker {
			receiver,
			delivery,
		} = prepared;
		spawn(async move {
			while let Ok(item) = receiver.recv().await {
				delivery.send(item).await;
			}
		});
	}

	#[inline]
	fn clear_broker(&mut self) {
		let Some(ctx) = Arc::get_mut(&mut self.ctx) else {
			debug_assert!(false, "clear_broker: ctx Arc was contended at statement boundary");
			static WARNED: AtomicBool = AtomicBool::new(false);
			if !WARNED.swap(true, Ordering::Relaxed) {
				tracing::warn!(
					target: TARGET,
					"clear_broker: ctx Arc contended at statement boundary; \
					 broker leaked for this batch and may affect notification \
					 semantics for the next batch. This is a bug — \
					 future occurrences will not log."
				);
			}
			return;
		};
		ctx.set_broker(None);
		self.broker_owned_by_executor = false;
	}
}

impl Executor {
	pub fn new(ctx: FrozenContext, opt: Options) -> Self {
		Executor {
			stack: TreeStack::new(),
			results: Vec::new(),
			opt,
			ctx,
			cached_session: None,
			broker_owned_by_executor: false,
		}
	}

	/// Install a fresh [`StatementCounters`] on the executor's context for
	/// the next statement and return a clone of the handle so the caller
	/// can read the counts after the statement returns. The iterator
	/// (deep in the execution path) inherits the same handle through the
	/// child contexts it derives from `self.ctx`.
	///
	/// Between statements the executor holds the only strong reference to
	/// `self.ctx`, so `Arc::get_mut` succeeds. If it ever fails — i.e.
	/// some child kept a strong reference past statement completion —
	/// the iterator would record into the previous statement's
	/// counters (or `None`) while `count_result_rows` reads zero from
	/// the new one, silently underreporting `RETURN NONE` DML row
	/// counts (the very case `StatementCounters` exists to fix).
	///
	/// We treat that as a programmer error: in debug builds it
	/// panics so tests fail loud; in release builds it logs a `warn`
	/// (deduped per process) so production telemetry surfaces the bug
	/// without turning a row-count discrepancy into an outage.
	fn install_statement_counters(&mut self) -> Arc<StatementCounters> {
		let counters = StatementCounters::new();
		if let Some(ctx) = Arc::get_mut(&mut self.ctx) {
			ctx.set_statement_counters(Some(Arc::clone(&counters)));
		} else {
			debug_assert!(
				false,
				"install_statement_counters: ctx Arc was contended at statement boundary"
			);
			static WARNED: std::sync::atomic::AtomicBool =
				std::sync::atomic::AtomicBool::new(false);
			if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
				tracing::warn!(
					target: "surrealdb::core::dbs",
					"install_statement_counters: ctx Arc contended at statement boundary; \
					 result_rows on RETURN NONE DML may underreport. This is a bug — \
					 future occurrences will not log."
				);
			}
		}
		counters
	}

	fn execute_option_statement(&mut self, stmt: &OptionStatement) -> Result<()> {
		// Allowed to run?
		self.ctx.is_allowed(&self.opt, Action::Edit, ResourceKind::Option, Base::Db)?;

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

	/// Dispatch a [`QueryEvent`] to the datastore's installed observer at
	/// the boundary of a query batch.
	///
	/// Aggregates per-statement outcomes captured by the executor while
	/// running the batch. Counters are derived from `results`: any
	/// `QueryResult` whose `result` is `Err` is classified as a statement
	/// error, and the batch outcome is [`Outcome::Error`] if any
	/// statement errored, otherwise [`Outcome::Success`]. Callers pass
	/// the slice of [`QueryResult`]s produced by the current batch
	/// (excluding any from outer batches).
	fn emit_query_event_for_results(
		&self,
		kvs: &Datastore,
		batch_start: Instant,
		results: &[QueryResult],
	) {
		let total = results.len() as u32;
		let err = results.iter().filter(|r| r.result.is_err()).count() as u32;
		let ok = total - err;
		let outcome = if err > 0 {
			Outcome::Error
		} else {
			Outcome::Success
		};
		// Classify the batch error using the FIRST errored statement's
		// structured `surrealdb_types::Error`. Operators see the
		// dominant error class for the batch on `surrealdb.query.*`;
		// per-statement classification happens via
		// `surrealdb.statement.*` once executor emit sites are wired
		// to populate it (today only the unexecuted-statement paths
		// do so explicitly).
		let error_class = results
			.iter()
			.find_map(|r| r.result.as_ref().err())
			.map(crate::observe::error_class::classify_types_error);
		kvs.observer().on_query_complete(&QueryEvent {
			safe: QueryEventSafe {
				outcome,
				duration: batch_start.elapsed(),
				counters: QueryCounters {
					total,
					ok,
					err,
				},
				error_class,
			},
			ctx: self.ctx.tenant_identity().map(|t| t.to_query_ctx()).unwrap_or_default(),
		});
	}

	/// Dispatch a [`StatementEvent`] to the datastore's installed observer.
	///
	/// Classification fields (`kind`, `read_only`, optional `sql`) are
	/// captured up-front by the caller before the originating expression is
	/// moved into the execution path. This keeps `on_statement_complete`
	/// reachable from every control-flow branch (early `return`, `continue`,
	/// or fall-through) without re-examining the consumed statement.
	///
	/// `result_rows` is the number of rows the statement returned (SELECT)
	/// or affected (CREATE / UPDATE / UPSERT / DELETE / RELATE / INSERT).
	/// Non-DML statements pass `0`. For DML statements with `RETURN NONE`
	/// the post-RETURN value is empty; the actual affected count is
	/// captured by the iterator on a per-statement [`StatementCounters`]
	/// and supplied here by the caller.
	#[allow(clippy::too_many_arguments)]
	fn emit_statement_event_cached(
		&self,
		kvs: &Datastore,
		kind: StatementType,
		read_only: bool,
		sql: Option<String>,
		start: &Instant,
		outcome: Outcome,
		result_rows: u64,
		error_class: Option<&'static str>,
	) {
		kvs.observer().on_statement_complete(&StatementEvent {
			safe: StatementEventSafe {
				kind,
				outcome,
				duration: start.elapsed(),
				read_only,
				result_rows,
				error_class,
			},
			ctx: self.ctx.tenant_identity().map(|t| t.to_statement_ctx(sql.clone())).unwrap_or(
				StatementEventCtx {
					sql,
					..Default::default()
				},
			),
		});
	}

	/// Emit a `StatementEvent` for a statement that was rejected
	/// before execution (transaction-create failure, parent-batch
	/// timeout, ctx cancellation, …). Centralised so every
	/// `QueryResult::Err(NotExecuted | TimedOut | Cancelled)` push
	/// has a matching telemetry event and dashboards do not undercount
	/// `surrealdb_statement_total{outcome="error"}` on these paths.
	///
	/// Duration is `Duration::ZERO` because the statement never ran;
	/// `read_only` is conservatively `false` because we cannot inspect
	/// the consumed expression.
	fn emit_statement_event_unexecuted(
		&self,
		kvs: &Datastore,
		kind: StatementType,
		error_class: &'static str,
	) {
		kvs.observer().on_statement_complete(&StatementEvent {
			safe: StatementEventSafe {
				kind,
				outcome: Outcome::Error,
				duration: Duration::ZERO,
				read_only: false,
				result_rows: 0,
				error_class: Some(error_class),
			},
			ctx: self.ctx.tenant_identity().map(|t| t.to_statement_ctx(None)).unwrap_or_default(),
		});
	}

	/// Compute the row count for a completed statement based on its kind,
	/// the per-statement counter snapshot, and the result the executor
	/// produced.
	///
	/// For DML statements (CREATE / UPDATE / UPSERT / DELETE / RELATE /
	/// INSERT) the iterator-side [`StatementCounters::affected`] is the
	/// source of truth: it correctly reports records mutated even when
	/// `RETURN NONE` (or a mode whose per-document value happens to be
	/// `Value::None`) suppresses the post-RETURN payload. The value
	/// shape is consulted only when no counter snapshot is available,
	/// which never happens on the production hot path but keeps the
	/// helper safe to call.
	///
	/// For non-DML statements the counter is not consulted and the value
	/// shape is interpreted as before:
	///
	/// - `Value::Array` — `array.len()` (SELECT rows after START/LIMIT);
	/// - `Value::None` / `Value::Null` — `0`;
	/// - anything else — `1`;
	/// - `None` (error path with no value to inspect) — `0`.
	fn count_result_rows(
		kind: StatementType,
		counters: Option<&StatementCounters>,
		value: Option<&Value>,
	) -> u64 {
		if kind.is_dml() {
			if let Some(counters) = counters {
				return counters.affected();
			}
			// Fallback to the value-shape heuristic if no counter was
			// installed (legacy / test paths).
			return match value {
				Some(Value::Array(arr)) => arr.len() as u64,
				Some(Value::None) | Some(Value::Null) | None => 0,
				Some(_) => 1,
			};
		}
		match value {
			Some(Value::Array(arr)) => arr.len() as u64,
			Some(Value::None) | Some(Value::Null) | None => 0,
			Some(_) => 1,
		}
	}

	/// Get the cached session info, extracting it on first call.
	///
	/// Session values don't change between statements in the same
	/// executor batch, so we extract once and reuse.
	fn get_session_info(&mut self) -> Option<Arc<crate::exec::context::SessionInfo>> {
		if let Some(ref cached) = self.cached_session {
			return Some(Arc::clone(cached));
		}
		let session = self.extract_session_info();
		self.cached_session.clone_from(&session);
		session
	}

	/// Extract session information from the FrozenContext.
	///
	/// The session is stored as a Value object in the context with keys like
	/// "ns", "db", "id", "ip", "or", "ac", "rd", "tk".
	fn extract_session_info(&self) -> Option<std::sync::Arc<crate::exec::context::SessionInfo>> {
		use crate::exec::context::SessionInfo;
		use crate::expr::paths::{AC, DB, ID, IP, NS, OR, RD, TK};

		let session_value = self.ctx.value("session")?;

		// Extract fields from the session Value. `Value::String` holds a `Strand`, so we just
		// move it into `SessionInfo` without the `Strand -> String -> Strand` round-trip the
		// previous `into_string()` chain incurred on every batch.
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
		&mut self,
		plan: Arc<dyn crate::exec::ExecOperator>,
		txn: Arc<Transaction>,
	) -> FlowResult<Value> {
		use tokio_util::sync::CancellationToken;

		use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
		use crate::exec::context::{
			DatabaseContext, ExecutionContext, NamespaceContext, RootContext,
		};

		/// Guard that aborts a spawned task when dropped, ensuring the
		/// timeout task is cleaned up when execution finishes or errors.
		struct AbortOnDrop(tokio::task::JoinHandle<()>);
		impl Drop for AbortOnDrop {
			fn drop(&mut self) {
				self.0.abort();
			}
		}

		// Derive the streaming-exec cancellation token from the legacy
		// context's awaitable cancel handle when one is installed (the WS
		// RPC layer installs it; embedded callers do not). `child_token()`
		// gives parent-cancels-child semantics, so the same connection
		// disconnect that trips the legacy executor's `Context::done`
		// flag also fires the streaming-exec operators' `select!` against
		// their token (e.g. `SleepPlan`, long-running scans) — without
		// linking the two we would silently lose WS cancellation on the
		// streaming-exec path.
		let cancellation = match self.ctx.cancel_token() {
			Some(parent) => parent.child_token(),
			None => CancellationToken::new(),
		};

		// If a query timeout is configured, spawn a task that cancels the
		// token when the timeout expires. This lets operators that check
		// the cancellation token (e.g. SleepPlan, long-running scans)
		// stop promptly instead of running to completion.
		// AbortOnDrop ensures the task is cleaned up when execution finishes.
		let _timeout_guard = self.ctx.timeout().map(|timeout| {
			let token = cancellation.clone();
			AbortOnDrop(tokio::spawn(async move {
				tokio::time::sleep(timeout).await;
				token.cancel();
			}))
		});

		// Build the root context using cached session info. The context
		// snapshot must be fresh per-query because it contains the
		// transaction reference which changes between statements.
		let root_ctx = RootContext {
			ctx: Context::snapshot(&self.ctx).freeze(),
			options: Some(self.opt.clone()),
			datastore: None,
			cancellation,
			auth: Arc::clone(&self.opt.auth),
			session: self.get_session_info(),
			current_value: None,
			skip_fetch_perms: false,
			version_stamp: None,
		};

		// Check what level of context we need
		let required_level = plan.required_context();

		let exec_ctx = match required_level {
			crate::exec::context::ContextLevel::Root => ExecutionContext::Root(root_ctx),
			crate::exec::context::ContextLevel::Namespace => {
				// Get namespace definition. SECURITY: `USE` no longer
				// implicitly creates namespaces for sessions without
				// `DEFINE NAMESPACE`-equivalent authorization, so the
				// planner must not paper over a missing target by writing
				// a fresh namespace into the catalog on the read path
				// (which would also fail on a read-only transaction).
				// `expect_ns_by_name` surfaces a clean `NsNotFound`
				// instead.
				let ns_name = self.opt.ns()?;
				let ns_def = txn.expect_ns_by_name(ns_name).await?;
				ExecutionContext::Namespace(NamespaceContext {
					root: root_ctx,
					ns: ns_def,
				})
			}
			crate::exec::context::ContextLevel::Database => {
				// Get namespace and database definitions. See the
				// `Namespace` arm above for why this no longer
				// auto-creates the target through the planner.
				let ns_name = self.opt.ns()?;
				let db_name = self.opt.db()?;
				let ns_def = txn.expect_ns_by_name(ns_name).await?;
				let db_def = txn.expect_db_by_name(ns_name, db_name).await?;
				ExecutionContext::Database(DatabaseContext {
					ns_ctx: NamespaceContext {
						root: root_ctx,
						ns: ns_def,
					},
					db: db_def,
					field_state_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
						std::collections::HashMap::new(),
					)),
					table_def_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
						std::collections::HashMap::new(),
					)),
					index_def_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
						std::collections::HashMap::new(),
					)),
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
			Ok(results.pop().expect("results verified non-empty"))
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

				// SECURITY: implicit creation of a namespace or database via
				// `USE` requires the same authorization as the explicit
				// `DEFINE NAMESPACE` / `DEFINE DATABASE` statements
				// (SECURITY_GUIDE section 3). Re-selecting an existing
				// namespace or database is unrestricted; downstream
				// operations against a non-existent resource surface
				// `NsNotFound` / `DbNotFound` naturally rather than a
				// silently auto-created resource the session was never
				// allowed to create.
				let create_ns = if let Some(ns_name) = use_ns.as_deref() {
					if txn.get_ns_by_name(ns_name, None).await?.is_some()
						|| (!self.ctx.auth_enabled() && self.opt.auth.is_anon())
					{
						true
					} else {
						self.opt
							.auth
							.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root())
							.is_ok()
					}
				} else {
					false
				};
				let create_db = if let Some(db_name) = use_db.as_deref() {
					let target_ns = use_ns.as_deref().or(self.opt.ns.as_deref());
					if let Some(ns_name) = target_ns {
						if txn.get_db_by_name(ns_name, db_name, None).await?.is_some()
							|| (!self.ctx.auth_enabled() && self.opt.auth.is_anon())
						{
							true
						} else if txn.get_ns_by_name(ns_name, None).await?.is_none()
							&& self
								.opt
								.auth
								.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root())
								.is_err()
						{
							// SECURITY: `ensure_ns_db` is
							// `get_or_add_db_upwards(..., upwards = true)`
							// and silently `get_or_add_ns`-es a missing
							// parent (see `kvs/tx.rs::get_or_add_db_upwards`).
							// Without this gate, a namespace-level Editor —
							// including one on a stale token whose
							// namespace has since been dropped — could
							// recreate the parent namespace as a side
							// effect of `USE NS dropped DB anything` or
							// `USE DB anything` against a stale
							// session-level ns.
							false
						} else {
							self.opt
								.auth
								.is_allowed(Action::Edit, &ResourceKind::Database.on_ns(ns_name))
								.is_ok()
						}
					} else {
						false
					}
				} else {
					false
				};

				let ctx = ctx_mut!();

				// Apply new namespace
				if let Some(ns) = use_ns {
					if create_ns {
						txn.get_or_add_ns(Some(ctx), &ns).await?;
					}

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

					if create_db {
						txn.ensure_ns_db(Some(ctx), ns, &db).await?;
					}

					let mut session = ctx.value("session").unwrap_or(&Value::None).clone();
					self.opt.set_db(Some(db.as_str().into()));
					session.put(DB.as_ref(), db.into());
					ctx.add_value("session", session.into());
				}

				// Invalidate cached session info since USE changes ns/db
				self.cached_session = None;

				// Return the current namespace and database
				Ok(Value::from(map! {
					"namespace" => self.opt.ns.as_deref().map(|x| Value::String(x.into())).unwrap_or(Value::None),
					"database" => self.opt.db.as_deref().map(|x| Value::String(x.into())).unwrap_or(Value::None),
				}))
			}
			TopLevelExpr::Option(_) => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::unreachable(
					"TopLevelExpr::Option should have been handled by a calling function",
				))));
			}

			TopLevelExpr::Expr(Expr::Let(stm)) => {
				// Reject protected names first, before any work: avoids planning
				// or computing a value we'll throw away.
				if stm.is_protected_set() {
					return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidParam {
						name: stm.name.to_string(),
					})));
				}
				ctx_mut!().set_transaction(Arc::clone(&txn));

				// Plan the RHS through the streaming pipeline first; fall back
				// to the legacy `compute()` path on PlannerUnsupported /
				// PlannerUnimplemented. The streaming pipeline is the same
				// one a bare `SELECT * FROM t` uses, so a `LET $x = SELECT * FROM t`
				// no longer pays the legacy recursive evaluator's cost.
				//
				// We plan `stm.what` (the RHS), not the LET itself: routing
				// through `plan_let_statement` would build a `LetPlan` whose
				// binding lives in `output_context`, which produces a new
				// `ExecutionContext` local to `execute_operator_plan` that
				// never reaches `self.ctx`. Planning the RHS directly keeps
				// the bind-into-session logic in one place — here.
				let res = match try_plan_expr!(
					&stm.what,
					&self.ctx,
					Arc::clone(&txn),
					Some(Arc::clone(&self.opt.auth))
				) {
					Ok(plan) => self.execute_operator_plan(plan, Arc::clone(&txn)).await,
					Err(err @ (Error::PlannerUnsupported(_) | Error::PlannerUnimplemented(_))) => {
						if let Error::PlannerUnimplemented(msg) = &err {
							tracing::warn!("PlannerUnimplemented fallback in top-level LET: {msg}");
						}
						self.stack
							.enter(|stk| stm.what.compute(stk, &self.ctx, &self.opt, None))
							.finish()
							.await
					}
					Err(e) => Err(ControlFlow::Err(anyhow::Error::new(e))),
				};

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
				match try_plan_expr!(
					&e,
					&self.ctx,
					Arc::clone(&txn),
					Some(Arc::clone(&self.opt.auth))
				) {
					Ok(plan) => {
						// Set the transaction on the context
						ctx_mut!().set_transaction(Arc::clone(&txn));

						// Build execution context and execute the plan
						let exec_result = self.execute_operator_plan(plan, Arc::clone(&txn)).await;

						self.check_slow_log(start, &e);

						// exec_result is now FlowResult<Value>, propagate directly
						exec_result
					}
					Err(err @ (Error::PlannerUnsupported(_) | Error::PlannerUnimplemented(_))) => {
						if let Error::PlannerUnimplemented(msg) = &err {
							tracing::warn!("PlannerUnimplemented fallback in executor: {msg}");
						}
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
		self.broker_owned_by_executor = false;
		let result = self.execute_plan_impl_inner(kvs, start, plan).await;
		if self.broker_owned_by_executor {
			self.clear_broker();
		}
		result
	}

	async fn execute_plan_impl_inner(
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
		let txn = Arc::new(
			kvs.transaction(transaction_type, LockType::Optimistic)
				.await?
				.with_tenant_identity(self.ctx.tenant_identity().cloned()),
		);
		let receiver = self.prepare_broker(
			matches!(transaction_type, TransactionType::Write),
			kvs.live_query_broker(),
		);

		let exec_result = match kvs.transaction_timeout() {
			Some(timeout) => {
				match tokio::time::timeout(
					timeout,
					self.execute_plan_in_transaction(Arc::clone(&txn), start, plan),
				)
				.await
				{
					Ok(res) => res,
					Err(_) => {
						let _ = txn.cancel().await;
						bail!(Error::TransactionTimedout(timeout.into()))
					}
				}
			}
			None => self.execute_plan_in_transaction(Arc::clone(&txn), start, plan).await,
		};

		match exec_result {
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

				// Flush buffered notifications only after the write is durable. Failed commits and
				// cancelled transactions drop the receiver without delivery.
				if let Some(prepared) = receiver {
					Self::flush_live_query_notifications(prepared);
				}

				Ok(value)
			}
			Err(ControlFlow::Continue) | Err(ControlFlow::Break) => {
				let _ = txn.cancel().await;
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
		stream: Pin<&mut S>,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelExpr>>,
	{
		self.broker_owned_by_executor = false;
		let result = self.execute_begin_statement_impl(kvs, stream).await;
		if self.broker_owned_by_executor {
			self.clear_broker();
		}
		result
	}

	async fn execute_begin_statement_impl<S>(
		&mut self,
		kvs: &Datastore,
		mut stream: Pin<&mut S>,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelExpr>>,
	{
		let Ok(txn) = kvs
			.transaction(TransactionType::Write, LockType::Optimistic)
			.await
			.map(|tx| tx.with_tenant_identity(self.ctx.tenant_identity().cloned()))
		else {
			// couldn't create a transaction.
			// Fast forward until we hit CANCEL or COMMIT
			while let Some(stmt) = stream.next().await {
				yield_now!();
				let stmt = stmt?;
				if let TopLevelExpr::Cancel | TopLevelExpr::Commit = stmt {
					return Ok(());
				}

				let kind = StatementType::from_top_level(&stmt);
				self.results.push(QueryResult {
					time: Duration::ZERO,
					result: Err(TypesError::query(
						"Tried to start a transaction while another transaction was open"
							.to_string(),
						Some(QueryError::NotExecuted),
					)),
					query_type: QueryType::Other,
				});
				self.emit_statement_event_unexecuted(
					kvs,
					kind,
					crate::observe::error_class::TXN_CREATE_FAILED,
				);
			}

			// Ran out of statements but still didn't hit a COMMIT or CANCEL
			// Just break as we can't do anything else since the query is already
			// effectively canceled.
			return Ok(());
		};
		let txn = Arc::new(txn);

		match kvs.transaction_timeout() {
			Some(timeout) => {
				let start_results = self.results.len();
				match tokio::time::timeout(
					timeout,
					self.execute_begin_statement_inner(kvs, Arc::clone(&txn), stream),
				)
				.await
				{
					Ok(result) => result,
					Err(_) => {
						let _ = txn.cancel().await;
						let timed_out_count = self.results.len().saturating_sub(start_results);
						for res in &mut self.results[start_results..] {
							res.query_type = QueryType::Other;
							res.result = Err(TypesError::query(
								format!(
									"The transaction timed out: {}",
									crate::val::Duration::from(timeout)
								),
								Some(QueryError::TimedOut {
									duration: timeout,
								}),
							));
						}
						// Emit one statement event per result that the
						// timeout retroactively turned into a failure.
						// We don't have the original statement kind any
						// more (the ast was consumed), so collapse to
						// `Other`; consumers care primarily about the
						// `error_class` and the `outcome` here.
						for _ in 0..timed_out_count {
							self.emit_statement_event_unexecuted(
								kvs,
								StatementType::Other,
								crate::observe::error_class::TXN_TIMEOUT,
							);
						}
						bail!(Error::TransactionTimedout(timeout.into()))
					}
				}
			}
			None => self.execute_begin_statement_inner(kvs, txn, stream).await,
		}
	}

	async fn execute_begin_statement_inner<S>(
		&mut self,
		kvs: &Datastore,
		txn: Arc<Transaction>,
		mut stream: Pin<&mut S>,
	) -> Result<()>
	where
		S: Stream<Item = Result<TopLevelExpr>>,
	{
		// Create a sender for this transaction only if the context allows for
		// notifications.
		let receiver = self.prepare_broker(true, kvs.live_query_broker());
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

				let cancelled_count = self.results.len().saturating_sub(start_results);
				for res in &mut self.results[start_results..] {
					res.query_type = QueryType::Other;
					res.result = Err(TypesError::query(
						"The query was not executed due to a cancelled transaction".to_string(),
						Some(QueryError::Cancelled),
					));
				}
				let cancel_class = match done {
					Reason::Timedout(_) => crate::observe::error_class::CTX_TIMEOUT,
					Reason::Canceled => crate::observe::error_class::CTX_CANCELLED,
				};
				for _ in 0..cancelled_count {
					self.emit_statement_event_unexecuted(kvs, StatementType::Other, cancel_class);
				}

				while let Some(stmt) = stream.next().await {
					yield_now!();
					let stmt = stmt?;
					let kind = StatementType::from_top_level(&stmt);
					match stmt {
						TopLevelExpr::Commit => {
							// After timeout/cancel the txn is already gone: COMMIT cannot succeed.
							// Still emit one `QueryResult` for this COMMIT statement so the batch
							// has one row per statement (mirrors successful COMMIT, which
							// pushes Ok(NONE) in the main `TopLevelExpr::Commit` branch
							// below) (#7207).
							self.results.push(QueryResult {
								time: Duration::ZERO,
								result: Err(match done {
									Reason::Timedout(d) => TypesError::query(
										format!("Cannot COMMIT: timed out ({d})"),
										Some(QueryError::TimedOut {
											duration: d.0,
										}),
									),
									Reason::Canceled => TypesError::query(
										"Cannot COMMIT: the transaction was cancelled".to_string(),
										Some(QueryError::Cancelled),
									),
								}),
								query_type: QueryType::Other,
							});
							self.emit_statement_event_unexecuted(kvs, kind, cancel_class);
							return Ok(());
						}
						ref stmt => {
							let result = Err(match done {
								Reason::Timedout(d) => TypesError::query(
									format!("Timed out: {d}"),
									Some(QueryError::TimedOut {
										duration: d.0,
									}),
								),
								Reason::Canceled => TypesError::query(
									"The query was not executed due to a cancelled transaction"
										.to_string(),
									Some(QueryError::Cancelled),
								),
							});
							self.results.push(QueryResult {
								time: Duration::ZERO,
								result,
								query_type: QueryType::Other,
							});
							self.emit_statement_event_unexecuted(kvs, kind, cancel_class);
							if matches!(stmt, TopLevelExpr::Cancel) {
								return Ok(());
							}
						}
					}
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

			// Capture classification up-front so we can emit a
			// `StatementEvent` regardless of which control-flow branch the
			// match below takes. The statement itself is moved into the
			// match, so anything the observer needs must be derived here.
			let statement_type = StatementType::from_top_level(&stmt);
			let statement_read_only = stmt.read_only();
			let sql_text = kvs.observer().needs_statement_text().then(|| stmt.to_sql());

			let before = Instant::now();
			// Row count populated by the `stmt =>` arm below before it
			// consumes the internal `Value`. Every other arm of the
			// match either returns early or contributes a non-DML
			// statement, so 0 is the right default.
			let mut stmt_result_rows: u64 = 0;
			let result = match stmt {
				TopLevelExpr::Begin => {
					let _ = txn.cancel().await;
					// tried to begin a transaction within a transaction.

					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(TypesError::query(
							format!(
								"The query was not executed due to a failed transaction: {}",
								stmt.to_sql()
							),
							Some(QueryError::NotExecuted),
						));
					}

					self.results.push(QueryResult {
						time: Duration::ZERO,
						result: Err(TypesError::internal(
							"Tried to start a transaction while another transaction was open"
								.to_string(),
						)),
						query_type: QueryType::Other,
					});

					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&before,
						Outcome::Error,
						0,
						Some(crate::observe::error_class::INTERNAL),
					);

					while let Some(stmt) = stream.next().await {
						yield_now!();
						let stmt = stmt?;
						match stmt {
							TopLevelExpr::Commit => {
								self.results.push(QueryResult {
									time: Duration::ZERO,
									result: Err(TypesError::query(
										"Cannot COMMIT: the transaction was aborted due to a nested BEGIN"
											.to_string(),
										Some(QueryError::NotExecuted),
									)),
									query_type: QueryType::Other,
								});
								return Ok(());
							}
							ref stmt => {
								self.results.push(QueryResult {
									time: Duration::ZERO,
									result: Err(TypesError::query(
										format!(
											"The query was not executed due to a failed transaction: {}",
											stmt.to_sql()
										),
										Some(QueryError::NotExecuted),
									)),
									query_type: QueryType::Other,
								});
								if matches!(stmt, TopLevelExpr::Cancel) {
									return Ok(());
								}
							}
						}
					}

					// Missing CANCEL/COMMIT statement, statement already canceled so nothing todo.
					return Ok(());
				}
				TopLevelExpr::Cancel => {
					let _ = txn.cancel().await;

					// update the results indicating cancelation.
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(TypesError::query(
							"The query was not executed due to a cancelled transaction".to_string(),
							Some(QueryError::Cancelled),
						));
					}

					// CANCEL returns NONE
					self.results.push(QueryResult {
						time: before.elapsed(),
						result: Ok(convert_value_to_public_value(Value::None)?),
						query_type: QueryType::Other,
					});

					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&before,
						Outcome::Success,
						0,
						None,
					);

					return Ok(());
				}
				TopLevelExpr::Commit => {
					// Commit the transaction.
					// If error undo results.
					let e = if let Err(e) = txn.commit().await {
						e
					} else {
						// Successfully commited. everything is fine.

						// Flush buffered notifications only after COMMIT succeeds. Rollback and
						// failed COMMIT paths drop the receiver without delivery.
						if let Some(prepared) = receiver {
							Self::flush_live_query_notifications(prepared);
						}

						// COMMIT returns NONE
						self.results.push(QueryResult {
							time: before.elapsed(),
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});

						self.emit_statement_event_cached(
							kvs,
							statement_type,
							statement_read_only,
							sql_text,
							&before,
							Outcome::Success,
							0,
							None,
						);

						return Ok(());
					};

					// `txn.commit()` failed (e.g. constraint on commit, or txn already finished).
					// Surface the failure on a dedicated COMMIT result row; mark prior statement
					// slots as not executed so nothing implies a successful commit (#7207).
					for res in &mut self.results[start_results..] {
						res.query_type = QueryType::Other;
						res.result = Err(TypesError::query(
							"The query was not executed due to a failed transaction".to_string(),
							Some(QueryError::NotExecuted),
						));
					}

					self.results.push(QueryResult {
						time: before.elapsed(),
						result: Err(TypesError::query(
							format!("Cannot COMMIT: {e}"),
							Some(QueryError::NotExecuted),
						)),
						query_type: QueryType::Other,
					});

					// `Cannot COMMIT` surfaces as a NotExecuted query error on
					// the COMMIT row -- a caller-visible failure ("the
					// transaction your statements ran inside could not
					// commit"), not an internal fault.
					self.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&before,
						Outcome::Error,
						0,
						Some(crate::observe::error_class::CLIENT),
					);

					return Ok(());
				}
				TopLevelExpr::Option(stmt) => match self.execute_option_statement(&stmt) {
					Ok(_) => {
						// OPTION returns NONE
						self.results.push(QueryResult {
							time: before.elapsed(),
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});
						self.emit_statement_event_cached(
							kvs,
							statement_type,
							statement_read_only,
							sql_text,
							&before,
							Outcome::Success,
							0,
							None,
						);
						continue;
					}
					Err(e) => Err(TypesError::internal(e.to_string())),
				},
				stmt => {
					// reintroduce planner later.
					let plan = stmt;

					// Install fresh per-statement counters so DML
					// iterators inside this BEGIN/COMMIT block can
					// surface affected-row counts independently of the
					// post-RETURN value shape.
					let counters = self.install_statement_counters();
					let r: Result<Value> = match self
						.execute_plan_in_transaction(Arc::clone(&txn), &before, plan)
						.await
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
								res.result = Err(TypesError::query(
									"The query was not executed due to a failed transaction"
										.to_string(),
									Some(QueryError::NotExecuted),
								));
							}

							// Convert the anyhow error before pushing so we can both
							// classify it for the metric attribute and store it on the
							// result row in a single move.
							let typed_err = types_error_from_anyhow(e);
							let error_class =
								Some(crate::observe::error_class::classify_types_error(&typed_err));

							// statement return an error. Consume all the other statement until
							// we hit a cancel or commit.
							self.results.push(QueryResult {
								time: before.elapsed(),
								result: Err(typed_err),
								query_type,
							});

							self.emit_statement_event_cached(
								kvs,
								statement_type,
								statement_read_only,
								sql_text,
								&before,
								Outcome::Error,
								0,
								error_class,
							);

							let _ = txn.cancel().await;

							while let Some(stmt) = stream.next().await {
								yield_now!();
								let stmt = stmt?;
								match stmt {
									TopLevelExpr::Commit => {
										// Aborted txn: COMMIT must error (same intent as
										// `txn.commit()` failure above — descriptive
										// `Cannot COMMIT:` prefix) (#7207).
										self.results.push(QueryResult {
												time: Duration::ZERO,
												result: Err(TypesError::query(
													"Cannot COMMIT: the transaction was aborted due to a prior error"
														.to_string(),
													Some(QueryError::NotExecuted),
												)),
												query_type: QueryType::Other,
											});
										return Ok(());
									}
									TopLevelExpr::Cancel => {
										return Ok(());
									}
									_ => {
										self.results.push(QueryResult {
												time: Duration::ZERO,
												result: Err(TypesError::query(
													"The query was not executed due to a cancelled transaction"
														.to_string(),
													Some(QueryError::Cancelled),
												)),
												query_type: QueryType::Other,
											});
									}
								}
							}

							// ran out of statements before the transaction ended.
							// Just break as we have nothing else we can do.
							return Ok(());
						}
					};

					// Count rows from the internal Value before it's
					// consumed by the conversion to PublicValue. Non-DML
					// statement kinds collapse to 0 inside the helper;
					// DML statements consult the iterator-side counter
					// so RETURN NONE / fresh CREATE etc. report
					// accurately.
					let rows = Self::count_result_rows(
						statement_type,
						Some(counters.as_ref()),
						r.as_ref().ok(),
					);
					stmt_result_rows = rows;

					match r {
						Ok(value) => Ok(convert_value_to_public_value(value)?),
						Err(err) => Err(TypesError::internal(err.to_string())),
					}
				}
			};

			let outcome = Outcome::from(&result);
			let error_class =
				result.as_ref().err().map(crate::observe::error_class::classify_types_error);
			self.emit_statement_event_cached(
				kvs,
				statement_type,
				statement_read_only,
				sql_text,
				&before,
				outcome,
				stmt_result_rows,
				error_class,
			);

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
			res.result = Err(TypesError::internal("Missing COMMIT statement".to_string()));
		}

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
		kvs: &Datastore,
		ctx: FrozenContext,
		opt: Options,
		plan: LogicalPlan,
	) -> Result<Vec<QueryResult>> {
		// The transaction is already set in the context
		// Execute each expression with the transaction
		let tx = ctx.tx();
		let batch_start = Instant::now();
		let mut executor = Self::new(ctx, opt);
		let mut results = Vec::new();

		for expr in plan.expressions {
			let start = Instant::now();
			// Capture classification before the expression is moved
			// into `execute_plan_in_transaction` so we can emit a
			// matching `StatementEvent` regardless of which control-flow
			// branch the result lands in (mirrors the cached helper
			// pattern used in `execute_expr_stream`).
			let statement_type = StatementType::from_top_level(&expr);
			let statement_read_only = matches!(expr, TopLevelExpr::Use(_) | TopLevelExpr::Show(_))
				|| matches!(
					&expr,
					TopLevelExpr::Expr(e) if matches!(
						e,
						Expr::Select(_) | Expr::Info(_) | Expr::Explain { .. }
					)
				);
			let counters = executor.install_statement_counters();
			let result = executor.execute_plan_in_transaction(Arc::clone(&tx), &start, expr).await;

			let time = start.elapsed();
			let query_result = match result {
				Ok(value) | Err(ControlFlow::Return(value)) => QueryResult {
					time,
					result: crate::val::convert_value_to_public_value(value)
						.map_err(|e| TypesError::internal(e.to_string())),
					query_type: QueryType::Other,
				},
				Err(ControlFlow::Err(e)) => QueryResult {
					time,
					result: Err(types_error_from_anyhow(e)),
					query_type: QueryType::Other,
				},
				Err(ControlFlow::Continue) | Err(ControlFlow::Break) => QueryResult {
					time,
					result: Err(TypesError::internal("Invalid control flow".to_string())),
					query_type: QueryType::Other,
				},
			};
			let outcome = Outcome::from(&query_result.result);
			let error_class = query_result
				.result
				.as_ref()
				.err()
				.map(crate::observe::error_class::classify_types_error);
			// Counters are accurate for DML (SELECT row counts come
			// from the value shape, which we no longer have post
			// conversion — passing `None` is fine since the helper
			// short-circuits to counter-driven counts on DML and
			// returns 0 on non-DML when value is `None`, matching the
			// previous behaviour for this entry point).
			let stmt_result_rows =
				Self::count_result_rows(statement_type, Some(counters.as_ref()), None);
			executor.emit_statement_event_cached(
				kvs,
				statement_type,
				statement_read_only,
				None,
				&start,
				outcome,
				stmt_result_rows,
				error_class,
			);
			results.push(query_result);
		}

		executor.emit_query_event_for_results(kvs, batch_start, &results);
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

	#[instrument(
		level = "debug",
		name = "executor",
		target = "surrealdb::core::dbs",
		skip_all,
		fields(
			// Pre-declared placeholder fields populated by the
			// enterprise OTEL enrichment observer. Tracing's
			// `Span::record(field, value)` only updates fields that
			// were declared at span-creation time; declaring them as
			// `tracing::field::Empty` here lets the observer fill in
			// tenant attributes without touching every executor
			// internal. Community builds pay zero cost — the
			// enrichment observer never attaches and the placeholders
			// stay empty.
			surrealdb.namespace = tracing::field::Empty,
			surrealdb.database = tracing::field::Empty,
			surrealdb.user = tracing::field::Empty,
			surrealdb.session_id = tracing::field::Empty,
			surrealdb.statement_type = tracing::field::Empty,
		),
	)]
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
		// Capture batch boundaries up-front so the `QueryEvent` emitted
		// at every return path measures the full span of the batch even
		// when an early parse error short-circuits the loop. The slice
		// of `QueryResult`s the executor produced is the source of
		// truth for the per-batch counters.
		let batch_start = Instant::now();
		let mut this = Executor::new(ctx, opt);
		let batch_results_start = this.results.len();
		let mut stream = pin!(stream);

		if skip_success_results {
			// The import path requires OPTION IMPORT as the first statement.
			// This sets opt.import which skips events, live queries, field
			// processing, table views, and result output for performance.
			match stream.as_mut().next().await {
				Some(Ok(TopLevelExpr::Option(ref stmt)))
					if stmt.name.eq_ignore_ascii_case("IMPORT") && stmt.what =>
				{
					this.execute_option_statement(stmt)?;
				}
				Some(Err(e)) => {
					bail!(Error::InvalidStatement(e.to_string()));
				}
				_ => {
					bail!(Error::InvalidStatement(
						"Import requires `OPTION IMPORT;` as the first statement. \
						 This disables events, live queries, field processing, and result \
						 output for optimal import performance. To execute queries with \
						 full side effects, use the /sql endpoint instead."
							.to_string()
					));
				}
			}
		}

		while let Some(stmt) = stream.next().await {
			let stmt = match stmt {
				Ok(x) => x,
				Err(e) => {
					this.results.push(QueryResult {
						time: Duration::ZERO,
						result: Err(TypesError::internal(e.to_string())),
						query_type: QueryType::Other,
					});

					this.emit_query_event_for_results(
						kvs,
						batch_start,
						&this.results[batch_results_start..],
					);
					return Ok(this.results);
				}
			};

			// Capture classification up-front so we can emit a
			// `StatementEvent` from whichever branch of the match consumes
			// `stmt`. None of these helpers retain any data that could leak
			// customer values to an unauthenticated sink -- `StatementType`
			// is a bounded enum and `to_sql` is only invoked when the
			// installed observer explicitly opts in.
			let statement_type = StatementType::from_top_level(&stmt);
			let statement_read_only = stmt.read_only();
			let sql_text = kvs.observer().needs_statement_text().then(|| stmt.to_sql());
			let start = Instant::now();

			match stmt {
				TopLevelExpr::Option(stmt) => {
					if skip_success_results && stmt.name.eq_ignore_ascii_case("IMPORT") {
						bail!(Error::InvalidStatement(
							"Cannot change OPTION IMPORT during an import stream. \
						 Import mode is locked for the duration of the /import request."
								.to_string()
						));
					}
					let result = this.execute_option_statement(&stmt);
					let outcome = Outcome::from(&result);
					// `execute_option_statement` returns `anyhow::Result`; the
					// only failure today is a permission denial via the
					// capability gate (`is_allowed(..., Action::Edit,
					// ResourceKind::Option, ...)`), so the bounded error class
					// is `permission`, not the generic `client` bucket.
					let error_class =
						result.as_ref().err().map(|_| crate::observe::error_class::PERMISSION);
					this.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&start,
						outcome,
						0,
						error_class,
					);
					result?;
					if !skip_success_results {
						this.results.push(QueryResult {
							time: Duration::ZERO,
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});
					}
				}
				TopLevelExpr::Begin => {
					if !skip_success_results {
						this.results.push(QueryResult {
							time: Duration::ZERO,
							result: Ok(convert_value_to_public_value(Value::None)?),
							query_type: QueryType::Other,
						});
					}

					let begin_result = this.execute_begin_statement(kvs, stream.as_mut()).await;
					let outcome = Outcome::from(&begin_result);
					// `execute_begin_statement` returns `anyhow::Result`; on
					// failure the result is wrapped via
					// `types_error_from_anyhow` below (see the `if let
					// Err(e)` arm). We can't classify before that conversion
					// because we'd consume the error twice, so reach for the
					// `txn_create_failed` constant: the only way
					// `execute_begin_statement` errors today is when the
					// transaction creation itself failed.
					let error_class = begin_result
						.as_ref()
						.err()
						.map(|_| crate::observe::error_class::TXN_CREATE_FAILED);
					this.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&start,
						outcome,
						0,
						error_class,
					);

					if let Err(e) = begin_result {
						this.results.push(QueryResult {
							time: Duration::ZERO,
							result: Err(types_error_from_anyhow(e)),
							query_type: QueryType::Other,
						});

						this.emit_query_event_for_results(
							kvs,
							batch_start,
							&this.results[batch_results_start..],
						);
						return Ok(this.results);
					}
				}
				stmt => {
					let query_type: QueryType = QueryType::for_toplevel_expr(&stmt);

					// Install fresh per-statement counters so DML
					// iterators can record affected rows independently of
					// the post-RETURN value shape.
					let counters = this.install_statement_counters();
					let result = this.execute_bare_statement(kvs, &start, stmt).await;
					let outcome = Outcome::from(&result);
					let result_rows = Self::count_result_rows(
						statement_type,
						Some(counters.as_ref()),
						result.as_ref().ok(),
					);
					let error_class = result
						.as_ref()
						.err()
						.map(crate::observe::error_class::classify_anyhow_error);
					this.emit_statement_event_cached(
						kvs,
						statement_type,
						statement_read_only,
						sql_text,
						&start,
						outcome,
						result_rows,
						error_class,
					);

					if skip_success_results {
						if let Err(err) = result {
							this.results.push(QueryResult {
								time: start.elapsed(),
								result: Err(types_error_from_anyhow(err)),
								query_type,
							});
						}
					} else {
						let result = match result {
							Ok(value) => Ok(convert_value_to_public_value(value)?),
							Err(err) => Err(types_error_from_anyhow(err)),
						};
						this.results.push(QueryResult {
							time: start.elapsed(),
							result,
							query_type,
						});
					}
				}
			}
			yield_now!();
		}
		this.emit_query_event_for_results(kvs, batch_start, &this.results[batch_results_start..]);
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
				let ds =
					Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();

				let res = ds.execute(statement, session, None).await;

				if *should_succeed {
					assert!(res.is_ok(), "{}: {:?}", msg, res);
				} else {
					let err = res.unwrap_err();
					assert!(err.is_not_allowed(), "{msg}: expected NotAllowed error, got {err}")
				}
			}
		}

		// Anonymous with auth enabled
		{
			let ds = Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();

			let res =
				ds.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None).await;

			let err = res.unwrap_err();
			assert!(
				err.is_not_allowed(),
				"anonymous user should not be able to set options: {}",
				err
			)
		}

		// Anonymous with auth disabled
		{
			let ds = Datastore::builder().with_auth(false).build_with_path("memory").await.unwrap();

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
			let results = res.unwrap();
			let err = results[0].result.as_ref().unwrap_err();
			assert!(
				err.is_validation()
					|| (err.is_internal() && err.message().contains("Invalid timeout")),
				"Expected to find invalid timeout error: {:?}",
				err
			);
		}
	}

	#[tokio::test]
	async fn import_stream_suppresses_results_but_persists_data() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql =
			"OPTION IMPORT; INSERT INTO person [{ name: 'a' }, { name: 'b' }, { name: 'c' }];";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let results = ds.import_stream(&sess, body).await.unwrap();

		assert!(
			results.is_empty(),
			"import_stream should suppress successful results, got {} results",
			results.len()
		);

		let verify = ds.execute("SELECT * FROM person ORDER BY name", &sess, None).await.unwrap();
		let rows = verify[0].result.as_ref().unwrap();
		assert!(rows.is_array(), "Expected an array of results");
		let arr = rows.as_array().unwrap();
		assert_eq!(arr.len(), 3, "Expected 3 inserted records, got {}", arr.len());
	}

	#[tokio::test]
	async fn import_stream_still_reports_errors() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; INSERT INTO person { name: 'ok' }; BREAK;";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let results = ds.import_stream(&sess, body).await.unwrap();

		assert!(!results.is_empty(), "import_stream should report errors");
		assert!(
			results.iter().any(|r| r.result.is_err()),
			"Expected at least one error result from invalid BREAK statement"
		);

		let verify = ds.execute("SELECT * FROM person", &sess, None).await.unwrap();
		let rows = verify[0].result.as_ref().unwrap();
		let arr = rows.as_array().unwrap();
		assert_eq!(arr.len(), 1, "The successful INSERT before the error should have persisted");
	}

	#[tokio::test]
	async fn import_stream_rejects_without_option_import() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "INSERT INTO person { name: 'a' };";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let result = ds.import_stream(&sess, body).await;

		assert!(result.is_err(), "import_stream should reject input without OPTION IMPORT");
		let err = result.unwrap_err().to_string();
		assert!(err.contains("OPTION IMPORT"), "Error should mention OPTION IMPORT, got: {err}");
	}

	#[tokio::test]
	async fn import_stream_rejects_option_import_change_midstream() {
		use bytes::Bytes;

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::default().with_ns("NS").with_db("DB");

		ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
			.await
			.unwrap();

		let sql = "OPTION IMPORT; OPTION IMPORT = false; INSERT INTO person { name: 'a' };";
		let body = futures::stream::once(async { Ok(Bytes::from(sql)) });
		let result = ds.import_stream(&sess, body).await;

		assert!(result.is_err(), "import_stream should reject OPTION IMPORT changes mid-stream");
		let err = result.unwrap_err().to_string();
		assert!(
			err.contains("Cannot change OPTION IMPORT"),
			"Error should explain that import mode is locked, got: {err}"
		);
	}

	mod result_rows_observer {
		//! Regression coverage for the affected-row contract on
		//! `StatementEventSafe.result_rows` and the per-batch
		//! `QueryEvent` dispatch. The iterator-side counter must report
		//! the actual number of records mutated for DML statements --
		//! including those that suppress the per-document value via
		//! `RETURN NONE` or that lack a "before" value (fresh `CREATE`).
		//! The executor must also emit one `QueryEvent` per query batch
		//! so authenticated `surrealdb_queries_total` /
		//! `surrealdb_query_errors_total` /
		//! `surrealdb_query_duration_seconds` keep advancing under load.

		use std::sync::{Arc, Mutex};

		use crate::dbs::Session;
		use crate::kvs::Datastore;
		use crate::observe::{
			ExecutionObserver, Outcome, QueryEvent, StatementEvent, StatementType,
		};

		#[derive(Default)]
		struct CapturingObserver {
			events: Mutex<Vec<(StatementType, u64)>>,
			queries: Mutex<Vec<(Outcome, u32, u32, u32)>>,
		}

		impl CapturingObserver {
			fn snapshot(&self) -> Vec<(StatementType, u64)> {
				self.events.lock().unwrap().clone()
			}

			fn queries(&self) -> Vec<(Outcome, u32, u32, u32)> {
				self.queries.lock().unwrap().clone()
			}
		}

		impl ExecutionObserver for CapturingObserver {
			fn on_statement_complete(&self, event: &StatementEvent) {
				self.events.lock().unwrap().push((event.safe.kind, event.safe.result_rows));
			}

			fn on_query_complete(&self, event: &QueryEvent) {
				let c = event.safe.counters;
				self.queries.lock().unwrap().push((event.safe.outcome, c.total, c.ok, c.err));
			}
		}

		async fn run(sql: &str) -> Vec<(StatementType, u64)> {
			let (events, _) = run_capturing(sql).await;
			events
		}

		#[allow(clippy::clone_on_ref_ptr)]
		async fn run_capturing(sql: &str) -> (Vec<(StatementType, u64)>, Arc<CapturingObserver>) {
			let observer = Arc::new(CapturingObserver::default());
			let obs: Arc<dyn ExecutionObserver> = observer.clone();
			let ds =
				Datastore::builder().with_observer(obs).build_with_path("memory").await.unwrap();
			let sess = Session::default().with_ns("NS").with_db("DB");
			// The capturing observer also receives the DEFINE bootstrap
			// events; drain them before running the test SQL so the
			// assertions only see the statements they care about.
			ds.execute("DEFINE NAMESPACE NS; USE NS NS; DEFINE DATABASE DB", &sess, None)
				.await
				.unwrap();
			observer.events.lock().unwrap().clear();
			observer.queries.lock().unwrap().clear();
			ds.execute(sql, &sess, None).await.unwrap();
			(observer.snapshot(), observer)
		}

		fn rows_for(events: &[(StatementType, u64)], kind: StatementType) -> Vec<u64> {
			events.iter().filter(|(k, _)| *k == kind).map(|(_, n)| *n).collect()
		}

		#[tokio::test]
		async fn update_return_none_reports_affected_count() {
			let events = run("\
				 CREATE foo:1; CREATE foo:2; CREATE foo:3;\
				 UPDATE foo SET x = 1 RETURN NONE;\
				 ")
			.await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(updates, vec![3], "UPDATE RETURN NONE should report 3 affected rows");
		}

		#[tokio::test]
		async fn create_return_before_reports_one() {
			let events = run("CREATE foo:bar RETURN BEFORE;").await;
			let creates = rows_for(&events, StatementType::Create);
			assert_eq!(creates, vec![1], "CREATE RETURN BEFORE should report 1 affected row");
		}

		#[tokio::test]
		async fn delete_return_none_reports_affected_count() {
			let events = run("\
				 CREATE foo:1; CREATE foo:2;\
				 DELETE foo RETURN NONE;\
				 ")
			.await;
			let deletes = rows_for(&events, StatementType::Delete);
			assert_eq!(deletes, vec![2], "DELETE RETURN NONE should report 2 affected rows");
		}

		#[tokio::test]
		async fn update_where_no_match_reports_zero() {
			// `WHERE` filter that no row satisfies must report zero
			// affected rows, even though the iterator visits every
			// scanned record. Pre-mutation `IgnoreError::Ignore` from
			// `check_where_condition` must not bump the counter.
			let events = run("\
				 CREATE foo:1 SET x = 1; CREATE foo:2 SET x = 2; CREATE foo:3 SET x = 3;\
				 UPDATE foo SET x = 99 WHERE x = 999;\
				 ")
			.await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(
				updates,
				vec![0],
				"UPDATE with WHERE matching no rows should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn update_nonexistent_record_reports_zero() {
			// Targeting a record id that does not exist makes
			// `check_record_exists` return `IgnoreError::Ignore`
			// before any KV write; the counter must stay at zero.
			let events = run("UPDATE foo:does_not_exist SET x = 1;").await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(
				updates,
				vec![0],
				"UPDATE on a non-existent record id should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn delete_where_no_match_reports_zero() {
			// DELETE with a WHERE clause that filters every row out
			// must report zero affected rows even when the iterator
			// visits each record.
			let events = run("\
				 CREATE foo:1 SET x = 1; CREATE foo:2 SET x = 2;\
				 DELETE foo WHERE x = 999;\
				 ")
			.await;
			let deletes = rows_for(&events, StatementType::Delete);
			assert_eq!(
				deletes,
				vec![0],
				"DELETE with WHERE matching no rows should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn update_unchanged_record_reports_zero() {
			// `set_record` is suppressed when `!self.changed()`, so
			// no KV write happens and the counter must stay at zero
			// even though the iterator returned `Ok` from
			// `Document::process`.
			let events = run("\
				 CREATE foo:1 SET x = 1;\
				 UPDATE foo:1 SET x = 1;\
				 ")
			.await;
			let updates = rows_for(&events, StatementType::Update);
			assert_eq!(
				updates,
				vec![0],
				"UPDATE that does not change a record should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn upsert_existing_record_where_no_match_reports_zero() {
			// Baseline coverage for the UPSERT-falls-back-to-update
			// path: `upsert_create` fails with `RecordExists` from
			// `store_record_data` (which returns Err before flipping
			// `doc.mutated`), the dispatch rolls back the empty
			// savepoint, and `upsert_update` returns `Ignore` from
			// `check_where_condition`. No KV write survives, so the
			// affected-row count must be zero.
			let events = run("\
				 CREATE foo:1 SET x = 1;\
				 UPSERT foo:1 SET x = 99 WHERE x = 999;\
				 ")
			.await;
			let upserts = rows_for(&events, StatementType::Upsert);
			assert_eq!(
				upserts,
				vec![0],
				"UPSERT on an existing record with an unmatched WHERE should report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn upsert_index_collision_then_where_no_match_reports_zero() {
			// Direct regression for the rollback-time `mutated`
			// reset: UPSERT on a fresh id passes
			// `upsert_create.store_record_data` (which sets
			// `doc.mutated = true`) and then trips a unique-index
			// collision in `store_index_data` against an existing
			// record. The dispatch rolls back the savepoint and
			// retries as an update against the colliding row, where
			// a WHERE clause filters it out. Without resetting
			// `doc.mutated` after the rollback, the stale flag from
			// the rolled-back create would inflate the affected-row
			// count even though no net KV write survives.
			let events = run("\
				 DEFINE INDEX uniq_x ON foo FIELDS x UNIQUE;\
				 CREATE foo:a SET x = 1;\
				 UPSERT foo:b SET x = 1 WHERE x = 999;\
				 ")
			.await;
			let upserts = rows_for(&events, StatementType::Upsert);
			assert_eq!(
				upserts,
				vec![0],
				"UPSERT that rolls back a unique-index collision and finds no WHERE match must report 0 affected rows"
			);
		}

		#[tokio::test]
		async fn select_unaffected_by_counter_path() {
			let events = run("\
				 CREATE foo:1; CREATE foo:2; CREATE foo:3;\
				 SELECT * FROM foo;\
				 ")
			.await;
			let selects = rows_for(&events, StatementType::Select);
			assert_eq!(selects, vec![3], "SELECT should still report rows from value-shape");
		}

		#[tokio::test]
		async fn query_event_emitted_for_successful_batch() {
			let (_, observer) = run_capturing("CREATE foo:1; SELECT * FROM foo;").await;
			let queries = observer.queries();
			assert_eq!(queries.len(), 1, "expected one QueryEvent per batch");
			let (outcome, total, ok, err) = queries[0];
			assert_eq!(outcome, Outcome::Success);
			assert_eq!(total, 2);
			assert_eq!(ok, 2);
			assert_eq!(err, 0);
		}

		#[tokio::test]
		async fn query_event_records_errors() {
			// Use an invalid runtime statement (THROW) so the parse step
			// still succeeds and the executor reaches the dispatch site.
			let (_, observer) = run_capturing("CREATE foo:1; THROW 'boom';").await;
			let queries = observer.queries();
			assert_eq!(queries.len(), 1, "expected one QueryEvent per batch");
			let (outcome, total, ok, err) = queries[0];
			assert_eq!(outcome, Outcome::Error);
			assert_eq!(total, 2);
			assert_eq!(ok, 1);
			assert_eq!(err, 1);
		}
	}
}
