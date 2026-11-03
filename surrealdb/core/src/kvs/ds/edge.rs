//! The edge entry: SurrealQL data statements, served without the schema runtime.
//!
//! [`Datastore::execute_edge`] parses with the SurrealQL parser alone, checks every statement
//! against the families it serves, and runs each statement in a transaction of its own through the
//! planner and the VM. It starts every query under the [`DML`] surface, so a statement of any other
//! family that is reached at run time (a function body, a closure or an event that holds one) is
//! refused where it stands. It names no driver of requests: the statement loop, the
//! transaction-block driver, import, export and the session and authentication layers are
//! `Datastore::execute`'s alone.

use std::ops::ControlFlow as Flow;
use std::sync::Arc;

use anyhow::{Result, bail};
use common::time::timeout;
use futures::StreamExt;
use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_kvs::TransactionType;
use surrealdb_types::Error as TypesError;
use tokio_util::sync::CancellationToken;
use web_time::Instant;

use crate::ctx::FrozenContext;
use crate::ctx::reason::Reason;
use crate::dbs::executor::{
	Carried, CarryBound, admit_session_let, commit_statement, context_mut,
	flush_live_query_notifications, install_broker, install_statement_state, lower_statement,
	planning_failed, spawn_timeout, statement_result,
};
use crate::dbs::{Options, QueryResult, QueryType, Session, TopLevelStatement};
use crate::err::EngineError;
use crate::exec::context::{
	ContextLevel, SessionEffect, SessionInfo, SessionSink, shape_statement_value, statement_context,
};
use crate::exec::planner::plan_statement_inner;
use crate::exec::surface::DML;
use crate::exec::{AccessMode, Error as ExecError, OutputShape};
use crate::expr::logical::{LogicalLet, LogicalOp};
use crate::expr::{ControlFlow, FlowResult};
use crate::kvs::{Datastore, DatastoreError, Transaction};
use crate::types::PublicVariables;
use crate::val::Value;

/// A statement the gate admitted: the plan to run and the mode of the transaction it runs in, or
/// the error its own lowering failed with.
enum Admitted {
	Run {
		op: Box<LogicalOp>,
		read_only: bool,
	},
	Failed(TypesError),
}

/// The refusal of a statement the edge entry does not serve, naming the family.
fn not_served(what: &str) -> TypesError {
	TypesError::validation(format!("The edge entry does not serve {what} statements"), None)
}

/// The family of the first node of `op`, or of a plan it holds, that the edge entry does not
/// serve. A closure body is not entered: it is judged where it is lowered.
fn unserved_in(op: &LogicalOp) -> Option<&'static str> {
	let found = op.visit_ops_deep(&mut |node| match node {
		LogicalOp::Ddl(_) => Flow::Break("schema"),
		LogicalOp::Info(_) => Flow::Break("INFO"),
		LogicalOp::Sleep(_) => Flow::Break("SLEEP"),
		LogicalOp::Explain(_) => Flow::Break("EXPLAIN"),
		LogicalOp::Match(_) => Flow::Break("MATCH"),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_) => Flow::Continue(()),
	});
	match found {
		Flow::Break(what) => Some(what),
		Flow::Continue(()) => None,
	}
}

/// Admits `statement`, or refuses the whole call.
///
/// The gate is a syntactic walk of the parsed statement and reads no catalog. A statement that is
/// not a data statement, and a plan that holds a schema statement, `INFO`, `SLEEP`, `EXPLAIN` or
/// `MATCH` anywhere below its root, is refused.
fn admit(statement: TopLevelStatement) -> Result<Admitted, TypesError> {
	let read_only = statement.read_only();
	let op = match statement {
		TopLevelStatement::Surreal(statement) => statement.plan,
		TopLevelStatement::Ast(TopLevelExpr::Expr(expr)) => match lower_statement(expr) {
			Ok(op) => op,
			Err(error) => {
				return Ok(Admitted::Failed(crate::err::anyhow_to_types_error(
					anyhow::Error::new(error),
				)));
			}
		},
		TopLevelStatement::Ast(TopLevelExpr::Begin(_)) => return Err(not_served("BEGIN")),
		TopLevelStatement::Ast(TopLevelExpr::Commit) => return Err(not_served("COMMIT")),
		TopLevelStatement::Ast(TopLevelExpr::Cancel) => return Err(not_served("CANCEL")),
		TopLevelStatement::Ast(TopLevelExpr::Access(_)) => return Err(not_served("ACCESS")),
		TopLevelStatement::Ast(TopLevelExpr::Kill(_)) => return Err(not_served("KILL")),
		TopLevelStatement::Ast(TopLevelExpr::Live(_)) => return Err(not_served("LIVE")),
		TopLevelStatement::Ast(TopLevelExpr::Option(_)) => return Err(not_served("OPTION")),
		TopLevelStatement::Ast(TopLevelExpr::Use(_)) => return Err(not_served("USE")),
		TopLevelStatement::Ast(TopLevelExpr::Show(_)) => return Err(not_served("SHOW")),
		TopLevelStatement::Plan(_) => return Err(not_served("pre-planned")),
	};
	if let Some(what) = unserved_in(&op) {
		return Err(not_served(what));
	}
	Ok(Admitted::Run {
		op: Box::new(op),
		read_only,
	})
}

/// What the request's top-level `LET`s retain between its statements. The edge entry runs one
/// request in memory, so nothing bounds them, as for `Datastore::execute`.
struct Bindings {
	carry: CarryBound,
	carried: Carried,
}

impl Datastore {
	/// Runs SurrealQL data statements in the session's principal, in a transaction per statement.
	///
	/// It serves `SELECT`, `CREATE`, `UPDATE`, `UPSERT`, `DELETE`, `RELATE`, `INSERT`, `LET`,
	/// `RETURN`, `IF`, `FOR`, blocks, subqueries, and calls of the datastore's builtins and of
	/// stored functions. A datastore built by [`Builder::for_edge`](super::Builder::for_edge)
	/// resolves the edge builtins alone.
	///
	/// Every statement is checked before any runs. If one is not served, the call fails and
	/// nothing runs. Each statement runs in its own transaction, planned and run in that
	/// transaction, and a failing statement does not stop the next. The session's checks, its
	/// principal and the table and field permissions are those of [`Datastore::execute`]. Stored
	/// code reached at run time that holds a statement of a family that is not served fails at
	/// that statement with the not-served error.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::edge", skip_all)
	)]
	pub async fn execute_edge(
		&self,
		txt: &str,
		sess: &Session,
		vars: Option<PublicVariables>,
	) -> std::result::Result<Vec<QueryResult>, TypesError> {
		let statements = self.parse_surrealql(txt)?;
		let (opt, mut ctx) =
			self.setup_query_with(&DML, &self.edge_compiled_units, sess, vars, None, None)?;
		let admitted = statements.into_iter().map(admit).collect::<Result<Vec<_>, _>>()?;
		let sink = Arc::<SessionSink>::default();
		let mut bindings = Bindings {
			carry: CarryBound::UNBOUNDED,
			carried: Carried::default(),
		};
		let mut results = Vec::with_capacity(admitted.len());
		for statement in admitted {
			let start = Instant::now();
			let (shape, outcome) = match statement {
				Admitted::Failed(error) => (OutputShape::Scalar, Err(error)),
				Admitted::Run {
					op,
					read_only,
				} => {
					let (shape, outcome) = self
						.run_edge_statement(&mut ctx, &opt, &sink, &mut bindings, *op, read_only)
						.await;
					(shape, outcome.map_err(crate::err::anyhow_to_types_error))
				}
			};
			let result = match outcome {
				Ok(value) => {
					statement_result(shape, value).map_err(crate::err::anyhow_to_types_error)
				}
				Err(error) => Err(error),
			};
			results.push(QueryResult {
				time: start.elapsed(),
				result,
				query_type: QueryType::Other,
			});
		}
		Ok(results)
	}

	/// Runs the plan `op` in a transaction of its own, and answers the shape of its value and the
	/// value. A transaction whose statement completes is committed when it writes and cancelled
	/// otherwise; any other is cancelled.
	async fn run_edge_statement(
		&self,
		ctx: &mut FrozenContext,
		opt: &Options,
		sink: &Arc<SessionSink>,
		bindings: &mut Bindings,
		op: LogicalOp,
		read_only: bool,
	) -> (OutputShape, Result<Value>) {
		let mut shape = OutputShape::Scalar;
		let outcome = self.run_edge_plan(ctx, opt, sink, bindings, op, read_only, &mut shape).await;
		(shape, outcome)
	}

	#[expect(
		clippy::too_many_arguments,
		reason = "the request's state, the statement, and where its shape is reported"
	)]
	async fn run_edge_plan(
		&self,
		ctx: &mut FrozenContext,
		opt: &Options,
		sink: &Arc<SessionSink>,
		bindings: &mut Bindings,
		op: LogicalOp,
		read_only: bool,
		shape: &mut OutputShape,
	) -> Result<Value> {
		bail_if_done(ctx)?;
		let transaction_type = if read_only {
			TransactionType::Read
		} else {
			TransactionType::Write
		};
		let txn = Arc::new(
			self.transaction(transaction_type)
				.await?
				.with_tenant_identity(ctx.tenant_identity().cloned())
				.with_write_keys_limit(self.transaction_max_write_keys()),
		);
		let receiver = match transaction_type {
			TransactionType::Write => self.live_query_broker().and_then(|delivery| {
				let source = crate::dbs::live_recheck::RecheckSource {
					tf: self.transaction_factory().clone(),
					sequences: self.sequences().clone(),
				};
				install_broker(ctx, delivery, source)
			}),
			TransactionType::Read => None,
		};
		let owns_broker = receiver.is_some();
		install_statement_state(ctx);

		let running = self.run_in_transaction(ctx, opt, sink, bindings, &txn, op, shape);
		let outcome = match self.transaction_timeout() {
			Some(left) => match timeout(left, running).await {
				Ok(outcome) => outcome,
				Err(_) => Err(ControlFlow::Err(anyhow::Error::new(
					DatastoreError::TransactionTimedout(left.into()),
				))),
			},
			None => running.await,
		};
		let outcome = outcome.and_then(|value| match bail_if_done(ctx) {
			Ok(()) => Ok(value),
			Err(done) => Err(ControlFlow::Err(done)),
		});
		if owns_broker && let Ok(ctx) = context_mut(ctx) {
			ctx.set_broker(None);
		}
		// The statement's transaction has ended, committed or cancelled.
		let ended = |ctx: &FrozenContext| {
			if let Some(memory) = ctx.memory() {
				memory.transaction_ended();
			}
		};
		match outcome {
			Ok(value) | Err(ControlFlow::Return(value)) => {
				// A read-only transaction has nothing to commit, and might return an error on
				// commit.
				if let TransactionType::Read = transaction_type {
					let _ = txn.cancel().await;
					ended(ctx);
					return Ok(value);
				}
				let committed = commit_statement(&txn).await;
				ended(ctx);
				committed?;
				// Notifications go out only once the write is durable. A failed commit and a
				// cancelled transaction drop the receiver without delivery.
				if let Some(prepared) = receiver {
					flush_live_query_notifications(prepared);
				}
				Ok(value)
			}
			Err(ControlFlow::Continue) | Err(ControlFlow::Break) => {
				let _ = txn.cancel().await;
				ended(ctx);
				bail!(ExecError::InvalidControlFlow)
			}
			Err(ControlFlow::Err(error)) => {
				let _ = txn.cancel().await;
				ended(ctx);
				Err(error)
			}
		}
	}

	/// Plans and runs `op` over `txn`, and applies the changes it made to the request's session.
	///
	/// The planner binds under the statement's transaction and the session's principal, and the
	/// surface of `ctx` decides which statement families it lowers.
	#[expect(
		clippy::too_many_arguments,
		reason = "the request's state, the statement's transaction, and where its shape is reported"
	)]
	async fn run_in_transaction(
		&self,
		ctx: &mut FrozenContext,
		opt: &Options,
		sink: &Arc<SessionSink>,
		bindings: &mut Bindings,
		txn: &Arc<Transaction>,
		op: LogicalOp,
		shape: &mut OutputShape,
	) -> FlowResult<Value> {
		context_mut(ctx)?.set_transaction(Arc::clone(txn));
		// A top-level `LET` binds into the request, which no program of a statement does: its value
		// runs as the statement and the entry binds what it yields.
		let (op, binding) = match op {
			LogicalOp::Let(binding) => {
				let LogicalLet {
					name,
					kind,
					value,
					..
				} = binding;
				// SECURITY: a protected name is refused before the value is evaluated, so a refused
				// `LET` has no side effects.
				if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
					return Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidParam {
						name,
					})));
				}
				let bare_return = matches!(*value, LogicalOp::Return(_));
				(*value, Some((name, kind, bare_return)))
			}
			op @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Window(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)) => (op, None),
		};
		let planned = plan_statement_inner(
			op,
			ctx,
			ctx.function_registry(),
			Arc::clone(txn),
			Some(Arc::clone(&opt.auth)),
			0,
			Some(Arc::new(opt.clone())),
		)
		.await;
		let plan = match planned {
			Ok(plan) => plan,
			Err(error) => return Err(ControlFlow::Err(planning_failed(opt, txn, error).await)),
		};

		// The engine's token follows the connection's, when one is installed, and a configured
		// query timeout cancels it. The guard stops the timer when the statement ends.
		let cancellation = match ctx.cancel_token() {
			Some(parent) => parent.child_token(),
			None => CancellationToken::new(),
		};
		let _timer = ctx.timeout().map(|left| spawn_timeout(left, cancellation.clone()));
		let session = SessionInfo::from_context(ctx).map(Arc::new);
		let exec_ctx = statement_context(
			ctx,
			opt,
			session.clone(),
			txn,
			&cancellation,
			plan.access_mode() != AccessMode::ReadOnly,
			plan.required_context(),
			Some(Arc::clone(sink)),
		)
		.await?;

		let ran = async {
			let mut stream = plan.execute(&exec_ctx)?;
			*shape = plan.output_shape();
			let mut values = Vec::new();
			while let Some(batch) = stream.next().await {
				values.extend(batch?.into_values());
			}
			shape_statement_value(*shape, values).map_err(ControlFlow::Err)
		}
		.await;
		drop(exec_ctx);
		drop(plan);
		let ran = settle_held_creates(txn, ran).await;

		let Some((name, kind, bare_return)) = binding else {
			apply_session_effects(ctx, sink, bindings)?;
			return ran;
		};
		// The value is a value scope: a `RETURN` in it ends the value with what it returns, which
		// the name is bound to. A value that is itself a `RETURN` stands in no value scope, so its
		// `RETURN` ends the statement with its value and binds nothing.
		let value = match ran {
			Err(ControlFlow::Return(value)) if !bare_return => value,
			ran => ran?,
		};
		let value = match kind {
			Some(kind) => {
				let set_coerce = |error| {
					ControlFlow::Err(anyhow::Error::new(ExecError::SetCoerce {
						name: name.clone(),
						error: Box::new(error),
					}))
				};
				let value = value.coerce_to_kind(&kind).map_err(set_coerce)?;
				if kind.has_named_type() {
					// The named types the kind holds are judged against the catalog of the
					// session's database, as a block-scoped `LET` judges them.
					let exec_ctx = statement_context(
						ctx,
						opt,
						session,
						txn,
						&cancellation,
						false,
						ContextLevel::Root,
						None,
					)
					.await?;
					crate::exec::vm::schema_host::check_types_held(&exec_ctx, &kind, value)
						.await
						.map_err(set_coerce)?
				} else {
					value
				}
			}
			None => value,
		};
		// SECURITY: a top-level `LET` binds into the request context, which is not reset between
		// statements, so a chain of them would grow the bound value's nesting without bound.
		crate::val::check_value_depth(&value)?;
		let value = Arc::new(value);
		match admit_session_let(&bindings.carry, &mut bindings.carried, &name, &value) {
			Ok(()) => {}
			Err(refusal) => return Err(ControlFlow::Err(anyhow::Error::new(refusal.into_error()))),
		}
		context_mut(ctx)?.add_value(name, value);
		*shape = OutputShape::Scalar;
		Ok(Value::None)
	}
}

/// The result of a statement once the creates it held are checked ([`Transaction::put_held`]):
/// `result` when every held key was absent, and otherwise the duplicate the earliest failing
/// create would have raised had its check been immediate, whatever the statement went on to do.
async fn settle_held_creates(txn: &Transaction, result: FlowResult<Value>) -> FlowResult<Value> {
	match txn.take_absence_violation().await {
		Ok(None) => result,
		Ok(Some(key)) => Err(ControlFlow::Err(crate::dbs::executor::held_create_exists(&key))),
		Err(e) => match result {
			Ok(_) => Err(ControlFlow::Err(e.into())),
			Err(prior) => Err(prior),
		},
	}
}

/// Fails with the query's timeout or cancellation if it has ended.
fn bail_if_done(ctx: &FrozenContext) -> Result<()> {
	match ctx.done(true)? {
		None => Ok(()),
		Some(Reason::Timedout(d)) => bail!(EngineError::QueryTimedout(d.0)),
		Some(Reason::Canceled) => bail!(EngineError::QueryCancelled),
	}
}

/// Takes the changes the statement that just ran recorded in the session sink. The gate leaves no
/// statement that selects a namespace or database, sets an option or binds a parameter through a
/// program, so any effect is an internal error.
fn apply_session_effects(
	_ctx: &mut FrozenContext,
	sink: &SessionSink,
	_bindings: &mut Bindings,
) -> FlowResult<()> {
	match sink.take().into_iter().next() {
		None => Ok(()),
		Some(
			SessionEffect::Let {
				..
			}
			| SessionEffect::Use {
				..
			}
			| SessionEffect::Option {
				..
			},
		) => Err(ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"a statement of the edge entry recorded a change to the session",
		)))),
	}
}
