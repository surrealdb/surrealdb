//! `eval::surql` / `eval::gql` for the streaming executor.
//!
//! These run the nested query *natively on the streaming engine*: the validated
//! block is planned with `expr_to_physical_expr` and evaluated against an
//! isolated child of the current `ExecutionContext` (only the explicit bindings
//! visible — never the call site's scope). Running on the streaming engine is
//! required for `eval::gql`, since GQL `MATCH` only executes there, and it
//! avoids bridging back to the legacy `compute` path (no reblessive `TreeStack`).
//!
//! Capability gating, parsing and statement validation live in
//! `crate::fnc::eval::prepare`, shared with the legacy `compute` path.
//!
//! Nested-`eval` recursion is bounded by *depth*, exactly like the legacy
//! executor: the planner counts expression-nesting depth against
//! `max_computation_depth`, and records that depth onto each `eval` node
//! ([`EvalContext::plan_depth`]). When `eval` re-plans its query string here, it
//! seeds the fresh planner with that recorded depth so the count *continues*
//! across the re-entry instead of resetting — so a self-referential or
//! deeply-nested `eval` reaches the limit and errors cleanly rather than growing
//! the native stack without bound (which, unlike the legacy heap-`TreeStack`
//! path, the streaming engine would otherwise do).

use anyhow::Result;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::Context;
use crate::dbs::Variables;
use crate::exe::FlowResultExt as _;
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::exec::plan_or_compute::evaluate_expr_at_depth;
use crate::expr::Kind;
use crate::fnc::args::{FromArgs, Optional};
use crate::fnc::eval::{Dialect, prepare};
use crate::val::{Object, Value};

/// Evaluate a prepared eval query on the streaming engine, isolated from the
/// call site's scope and bound to the current transaction.
///
/// `prepare` yields a single statement (SurrealQL is restricted to one; GQL
/// lowers to a single `Expr::Match`). Each statement is run through
/// [`evaluate_expr_at_depth`], the streaming engine's plan-or-compute bridge: it
/// plans the statement into operators — including the `Expr::Match` operator that
/// GQL needs — and only falls back to the engine's single sanctioned `compute`
/// bridge for statement shapes the planner does not yet support. The re-plan is
/// seeded with `ctx.plan_depth` (the depth this `eval` node was planned at) so
/// the nesting count continues toward `max_computation_depth`.
async fn evaluate_streaming(
	ctx: &EvalContext<'_>,
	dialect: Dialect,
	query: String,
	bindings: Option<Object>,
) -> Result<Value> {
	let exec_ctx = ctx.exec_ctx;
	let caps = exec_ctx.capabilities();

	// Depth at which this `eval` node was planned (recorded by the planner and
	// surfaced via `BuiltinFunctionExec`). The re-planned query is one re-entry
	// deeper, so it is compiled at `plan_depth + 1`; that continuation is what
	// bounds nested-`eval` recursion against `max_computation_depth`.
	let depth = ctx.plan_depth + 1;

	// Gate, parse and validate (shared with the legacy compute path).
	let block = prepare(&caps, exec_ctx.auth(), dialect, &query)?;

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
	// `eval` from a predicate body would bypass the frame that
	// `plan_or_compute` derives from the root (GHSA-66r2-5gwj-gxm2). The
	// COMPUTED-field signal is already a root flag, so it needs no mirror.
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

	// Evaluate on the streaming engine, returning the final statement's value.
	// Seeding the planner with `depth` continues the nesting count across this
	// re-entry, so a runaway recursion hits `max_computation_depth` and errors.
	let mut result = Value::None;
	for stmt in block.iter() {
		result = evaluate_expr_at_depth(stmt, &eval_ctx, depth).await.catch_return()?;
	}
	Ok(result)
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
			evaluate_streaming(ctx, Dialect::Surql, query, bindings).await
		})
	}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EvalGql;

impl ScalarFunction for EvalGql {
	fn name(&self) -> &'static str {
		"eval::gql"
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

pub fn register(registry: &mut FunctionRegistry) {
	registry.register(EvalSurql);
	registry.register(EvalGql);
}
