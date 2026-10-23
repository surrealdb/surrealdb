//! JavaScript function expression - embedded script functions.

#[cfg(feature = "scripting")]
use std::sync::Arc;

use crate::exec::EvalContext;
use crate::expr::FlowResult;
use crate::val::Value;

/// Refuse a script call `ctx` cannot make, before its arguments are
/// evaluated: a context with no `Options` to run it under, a session whose
/// capabilities deny scripting, and every call in a build without the script
/// runtime.
pub(crate) fn check_script_allowed(ctx: &EvalContext<'_>) -> FlowResult<()> {
	#[cfg(feature = "scripting")]
	{
		ctx.exec_ctx
			.options()
			.ok_or_else(|| anyhow::anyhow!("Script functions require Options context"))?;
		ctx.exec_ctx.ctx().check_allowed_scripting()?;
		Ok(())
	}
	#[cfg(not(feature = "scripting"))]
	{
		let _ = ctx;
		Err(script_runtime_missing())
	}
}

/// Run script `source` with `args` in `ctx`, its current value as `this`: the
/// call [`check_script_allowed`] admitted. A script that re-enters SurrealQL
/// continues the computation-depth count from `plan_depth`, the depth the
/// call was planned at.
pub(crate) async fn run_script(
	source: &str,
	plan_depth: u32,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	#[cfg(feature = "scripting")]
	{
		use crate::doc::CursorDoc;
		use crate::fnc::script;

		let frozen_ctx = Arc::clone(ctx.exec_ctx.ctx());
		let opt = ctx
			.exec_ctx
			.options()
			.ok_or_else(|| anyhow::anyhow!("Script functions require Options context"))?
			.clone();
		// Continue the depth count across the JS boundary: a script that
		// re-enters SurrealQL (via `surrealdb.query`) keeps counting from the depth the planner
		// already reached, rather than getting a fresh budget.
		let opt = opt.with_dive_consumed(plan_depth);
		// The script receives the value in scope as its document (`this`);
		// where that is a KNN query vector's row being scanned, the call is
		// refused before it runs, whether or not the script reads it.
		let doc = ctx.value_in_scope().read()?.map(|v| CursorDoc::new(None, v.clone()));
		// A script may reach outside the transaction (`fetch`), so the statement is not run
		// again after a lost conflict.
		ctx.exec_ctx.txn().note_external_effect();
		Ok(script::run(&frozen_ctx, &opt, doc.as_ref(), source, args).await?)
	}
	#[cfg(not(feature = "scripting"))]
	{
		let _ = (source, plan_depth, args, ctx);
		Err(script_runtime_missing())
	}
}

/// The error a script call raises in a build without the script runtime.
#[cfg(not(feature = "scripting"))]
fn script_runtime_missing() -> crate::expr::ControlFlow {
	crate::exec::Error::InvalidScript {
		message: String::from("Embedded functions are not enabled."),
	}
	.into()
}
