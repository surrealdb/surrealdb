//! `eval::surql` / `eval::gql` for the streaming executor.
//!
//! `eval` re-enters the executor at a statement boundary of its own: the query
//! text is parsed into a statement, and the engine is chosen for that statement
//! the way the executor chooses for a top-level one — lowering, then binding
//! and planning, then the legacy engine when either declines. A statement with
//! a logical form runs natively: lowered, bound and planned, then evaluated
//! against an isolated child of the current `ExecutionContext` (only the
//! explicit bindings visible — never the call site's scope). A statement
//! without one — a write, a schema statement — runs whole on the legacy engine
//! through [`legacy_compute`], in the same isolated context and transaction.
//! The plan holding the call never hands part of itself over: the nested
//! statement is decided and run as a unit. Running on the streaming engine is
//! required for `eval::gql`, since GQL `MATCH` only executes there.
//!
//! Capability gating, parsing and statement validation live in
//! `crate::fnc::eval::prepare`, shared with the legacy `compute` path.
//!
//! Nested-`eval` recursion is bounded by *depth*, exactly like the legacy
//! executor: the planner counts expression-nesting depth against
//! `max_computation_depth`, and records that depth onto each `eval` node
//! ([`EvalContext::plan_depth`]). When `eval` plans its query string here, it
//! seeds the fresh planner with that recorded depth so the count *continues*
//! across the re-entry instead of resetting — so a self-referential or
//! deeply-nested `eval` reaches the limit and errors cleanly rather than growing
//! the native stack without bound.

use std::sync::Arc;

use anyhow::Result;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::Context;
use crate::dbs::{Force, Variables};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::FlowResultExt as _;
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::operators::collect_shaped_value;
use crate::exec::operators::legacy_bridge::{legacy_compute, legacy_handles};
use crate::exec::physical_expr::EvalContext;
use crate::exec::planner::{plan_logical_inner, plan_statement_inner};
use crate::exec::{Error as ExecError, ExecOperator, ExecutionContext};
use crate::expr::Kind;
use crate::expr::logical::{EffectSet, lower};
use crate::fnc::args::{FromArgs, Optional};
use crate::fnc::eval::{Prepared, prepare};
use crate::kvs::Dialect;
use crate::val::{Object, Value};

/// Evaluate a prepared eval query, isolated from the call site's scope and
/// bound to the current transaction.
///
/// `prepare` yields either the SurrealQL statements (at most one) or, for GQL,
/// the plan its lowering produced. A statement is lowered and goes through
/// [`plan_statement_inner`], which binds it for the transaction and compiles
/// it; when lowering declines, or planning answers `PlannerUnsupported`, the
/// statement runs on the legacy engine instead ([`evaluate_legacy`]). A plan
/// goes through [`plan_logical_inner`] and has
/// no other engine. The planner is seeded with `ctx.plan_depth + 1` so the
/// nesting count continues toward `max_computation_depth`.
async fn evaluate_streaming(
	ctx: &EvalContext<'_>,
	dialect: Dialect,
	query: String,
	bindings: Option<Object>,
) -> Result<Value> {
	let exec_ctx = ctx.exec_ctx;
	let caps = exec_ctx.capabilities();

	// Depth at which this `eval` node was planned (recorded by the planner and
	// surfaced via `BuiltinFunctionExec`). The nested query is one re-entry
	// deeper, so it is compiled at `plan_depth + 1`; that continuation is what
	// bounds nested-`eval` recursion against `max_computation_depth`.
	let depth = ctx.plan_depth + 1;

	// Gate, parse and validate (shared with the legacy compute path).
	let prepared = prepare(&caps, exec_ctx.auth(), dialect, &query)?;

	// Build an isolated child context: the evaluated query sees the transaction,
	// capabilities and the explicit bindings, but not the caller's scope. The
	// `ExecutionContext` is rebuilt around it, preserving auth / ns-db level.
	let mut isolated = Context::new_isolated(exec_ctx.ctx());
	if let Some(bindings) = bindings {
		isolated.attach_variables(Variables::from(bindings))?;
	}
	let isolated = isolated.freeze();
	let mut eval_ctx = exec_ctx.with_new_ctx(isolated);

	// Carry the permission-predicate no-write signal onto the root of the
	// re-entered context. Inside a `PERMISSIONS` predicate the flag lives on
	// the caller's `EvalContext` (set in `exec::permission`), not on the root
	// this builtin rebuilds from; without this, a write reached through
	// `eval` from a predicate body would bypass the frame the legacy bridge
	// derives from the root (GHSA-66r2-5gwj-gxm2). The COMPUTED-field signal
	// is already a root flag, so it needs no mirror.
	eval_ctx = eval_ctx.with_skip_fetch_perms(ctx.skip_fetch_perms);

	// Promote to Database level when a database is selected, so nested table /
	// GQL MATCH queries have the context they require. `eval`'s `required_context`
	// stays Root so db-less queries (e.g. `eval::surql("RETURN 1")`) still work;
	// the streaming executor would otherwise apply this promotion itself.
	if let Some(session) = exec_ctx.session()
		&& let (Some(ns), Some(db)) = (session.ns.clone(), session.db.clone())
	{
		let txn = exec_ctx.txn();
		let ns_def = txn.expect_ns_by_name(ns.as_str()).await?;
		let db_def = txn.expect_db_by_name(ns.as_str(), db.as_str()).await?;
		eval_ctx = eval_ctx.with_database(ns_def, db_def);
	}

	// Planning resolves table and index definitions through the transaction,
	// so a context without one cannot plan at all.
	let txn = eval_ctx.try_txn().ok_or_else(|| {
		Error::unreachable("eval requires a transaction, but the context has none")
	})?;
	let auth = Some(Arc::clone(&eval_ctx.root().auth));

	match prepared {
		Prepared::Statements(statements) => {
			let mut result = Value::None;
			for stmt in statements {
				let planned = match lower(stmt.clone()) {
					Ok(op) => plan_statement_inner(
						op,
						eval_ctx.ctx(),
						eval_ctx.function_registry(),
						Arc::clone(&txn),
						auth.clone(),
						depth,
					)
					.await
					.map(Some),
					Err(_) => Ok(None),
				};
				result = match planned {
					Ok(Some(plan)) => run_plan(plan, &eval_ctx).await.catch_return()?,
					Ok(None) | Err(Error::Exec(ExecError::PlannerUnsupported(_))) => {
						evaluate_legacy(&eval_ctx, ctx, stmt).await?
					}
					Err(e) => return Err(e.into()),
				};
			}
			Ok(result)
		}
		// A GQL query lowers to a plan, and this is the only engine that runs
		// one.
		Prepared::Plan(op) => {
			let plan = plan_logical_inner(
				*op,
				eval_ctx.ctx(),
				eval_ctx.function_registry(),
				txn,
				auth,
				depth,
			)
			.await?;
			Ok(run_plan(plan, &eval_ctx).await.catch_return()?)
		}
	}
}

/// Evaluate a statement the streaming engine declined on the legacy engine,
/// in the same isolated context and transaction.
///
/// The options are the ones a legacy evaluation inside `eval_ctx` runs with
/// ([`legacy_handles`]), so the permission-predicate and computed-field frames
/// the call site is in still forbid a write, with the call's nesting depth
/// consumed from the dive budget so the count continues across the engine
/// boundary as it does when the statement is planned. Force is reset so the
/// nested statement does not inherit a table re-run. The document is the call
/// site's, as the legacy `eval` passes its own caller's.
async fn evaluate_legacy(
	eval_ctx: &ExecutionContext,
	ctx: &EvalContext<'_>,
	stmt: crate::sql::Expr,
) -> Result<Value> {
	let (opt, frozen) = legacy_handles(eval_ctx)?;
	let opt = opt.new_with_force(Force::None).with_dive_consumed(ctx.plan_depth + 1);
	let doc =
		ctx.document_root.or(ctx.current_value).map(|v| CursorDoc::new(None, None, v.clone()));
	let expr = crate::expr::Expr::from(stmt);
	legacy_compute(&expr, &frozen, &opt, doc.as_ref()).await.catch_return()
}

/// Run a compiled plan to the single value it evaluates to.
async fn run_plan(
	plan: Arc<dyn ExecOperator>,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<Value> {
	let shape = plan.output_shape();
	let stream = plan.execute(ctx)?;
	collect_shaped_value(stream, shape).await
}

fn eval_signature() -> Signature {
	Signature::new()
		.arg("query", Kind::String)
		.optional("bindings", Kind::Object)
		.returns(Kind::Any)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EvalSurql;

impl ScalarFunction for EvalSurql {
	fn name(&self) -> &'static str {
		"eval::surql"
	}

	fn effects(&self) -> EffectSet {
		EffectSet::OPAQUE_BODY | EffectSet::WRITES_DATA
	}

	fn signature(&self) -> Signature {
		eval_signature()
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let (query, Optional(bindings)) = FromArgs::from_args("eval::surql", args)?;
			evaluate_streaming(ctx, Dialect::SurrealQl, query, bindings).await
		})
	}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EvalGql;

impl ScalarFunction for EvalGql {
	fn name(&self) -> &'static str {
		"eval::gql"
	}

	fn effects(&self) -> EffectSet {
		EffectSet::OPAQUE_BODY | EffectSet::WRITES_DATA
	}

	fn signature(&self) -> Signature {
		eval_signature()
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let (query, Optional(bindings)) = FromArgs::from_args("eval::gql", args)?;
			evaluate_streaming(ctx, Dialect::Gql, query, bindings).await
		})
	}
}

// Both run whatever statement they are handed, so neither may be duplicated,
// dropped or folded. `is_writer_builtin` already holds them out of every
// purity decision; the flag says the same thing at the registration.
pub fn register(registry: &mut FunctionRegistry) {
	registry.register(EvalSurql);
	registry.register(EvalGql);
}
