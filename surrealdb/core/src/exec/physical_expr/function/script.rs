//! JavaScript function expression - embedded script functions.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, evaluate_args, fmt_call_args};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut};
use crate::expr::{FlowResult, Script};
use crate::val::Value;

/// JavaScript function expression - embedded script functions.
#[derive(Debug, Clone)]
pub struct JsFunctionExec {
	pub(crate) script: Script,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// Expression-nesting depth recorded when this node was planned. A script
	/// can re-enter SurrealQL (via `surrealdb.query`), so the remaining
	/// computation-depth budget handed to it is reduced by this much, keeping
	/// the limit continuous across the boundary.
	pub(crate) plan_depth: u32,
}
impl PhysicalExpr for JsFunctionExec {
	fn name(&self) -> &'static str {
		"JsFunction"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Script functions access database context through the frozen context
		// when needed, so they can operate at root level. Requiring Database
		// here would cause failures when no namespace/database is selected.
		crate::exec::ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			check_script_allowed(&ctx)?;
			let args = evaluate_args(&self.arguments, ctx.clone()).await?;
			run_script(&self.script.0, self.plan_depth, args, &ctx).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		// Script functions are always potentially read-write
		AccessMode::ReadWrite.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for JsFunctionExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("function");
		fmt_call_args(f, fmt, &self.arguments);
	}
}

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
		use reblessive::TreeStack;

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
		let doc = ctx.current_value.map(|v| CursorDoc::new(None, v.clone()));
		// JavaScript can call back into SurrealDB functions through
		// `surrealdb.functions.*`, which needs a TreeStack for recursive
		// computation.
		let mut stack = TreeStack::new();
		Ok(stack
			.enter(|_stk| async {
				script::run(&frozen_ctx, &opt, doc.as_ref(), source, args).await
			})
			.finish()
			.await?)
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
