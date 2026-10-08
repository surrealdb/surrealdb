//! Shared helper functions for function physical expressions.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use super::closure::CompiledClosure;
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, PlannedGuard};
use crate::exec::{AccessMode, CombineAccessModes, Error as ExecError};
use crate::expr::logical::LogicalPermissionRef;
use crate::expr::{Error as ExprError, FlowResult, Kind};
use crate::val::{Closure, Value};

/// Evaluate all argument expressions to values.
pub(crate) async fn evaluate_args(
	args: &[Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'_>,
) -> FlowResult<Vec<Value>> {
	let mut values = Vec::with_capacity(args.len());
	for arg_expr in args {
		values.push(arg_expr.evaluate(ctx.clone()).await?);
	}
	Ok(values)
}

/// Check a function's `PERMISSIONS` clause for the caller.
///
/// A conditional permission is judged on every call and evaluated in the
/// permission frame: reads inside it are not themselves permission-checked,
/// which is what stops a predicate that follows a record link from recursing
/// into itself. `guards` holds the calling node's memo of the compilation,
/// which [`CompiledGuard`] reuses only while `definition` is the definition
/// the call found; a predicate that fails to compile or to evaluate denies.
///
/// `guards` also holds the predicate compiled with the plan: where the
/// definition the call found holds the clause it was compiled from, under the
/// `AUTH LIMIT` `limit`, it is judged, once per statement where its verdict
/// stands that long ([`crate::exec::guard_verdicts`]), and nothing is compiled
/// while the call runs. `kind` names the definition the verdict is kept for.
pub(crate) async fn check_permission<D>(
	kind: crate::exec::guard_verdicts::GuardKind,
	(guard, planned): (&CompiledGuard<D>, &PlannedGuard<D>),
	permission: LogicalPermissionRef<'_>,
	limit: Option<&crate::catalog::auth::AuthLimit>,
	definition: &Arc<D>,
	func_name: &str,
	ctx: &EvalContext<'_>,
) -> FlowResult<()> {
	let denied = || ExecError::FunctionPermissions {
		name: func_name.to_string(),
	};
	if let Some(matched) = planned.matching(definition, permission, limit) {
		return crate::exec::guard_verdicts::judged(
			ctx.exec_ctx,
			matched.scope,
			kind,
			func_name,
			matched.id,
			|| crate::exec::permission::judge_compiled_guard(matched.compiled, ctx, denied),
		)
		.await;
	}
	guard.check(permission, definition, ctx, ctx.exec_ctx.try_txn(), denied).await
}

/// Invoke a closure value with `args` from a streaming expression.
///
/// A [`CompiledClosure`] — the value the engine creates — evaluates
/// its compiled body directly; every other form is invoked by
/// [`crate::exec::closure::invoke`]. The body evaluates against the caller's
/// document root — or its current value, when there is no root — so a closure
/// of either form sees the row the statement is evaluating, whichever
/// position of an idiom chain the call sits at (see [`CompiledClosure::invoke`]).
pub(crate) async fn invoke_closure(
	closure: &Closure,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	if let Closure::Builtin(obj) = closure
		&& let Some(compiled) = obj.as_any().downcast_ref::<CompiledClosure>()
	{
		return compiled.invoke(ctx, args).await;
	}
	invoke_uncompiled_closure(closure, args, ctx).await
}

/// [`invoke_closure`] for a closure value that is not a [`CompiledClosure`]:
/// the rest of an API chain, run by its stage, or a closure stored as
/// written, which the instruction handing it over links no unit for: its
/// invocation links the unit itself.
pub(crate) async fn invoke_uncompiled_closure(
	closure: &Closure,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	crate::exec::closure::invoke(closure, args, ctx).await
}

/// Validate argument count against expected signature.
pub(crate) fn validate_arg_count(
	func_name: &str,
	actual: usize,
	expected: &[(String, Kind)],
) -> anyhow::Result<()> {
	let max_args = expected.len();
	// Count minimum required args (non-optional trailing args)
	let min_args = expected.iter().rev().fold(0, |acc, (_, kind)| {
		if kind.can_be_none() && acc == 0 {
			0
		} else {
			acc + 1
		}
	});

	if !(min_args..=max_args).contains(&actual) {
		return Err(ExprError::InvalidFunctionArguments {
			name: func_name.to_string(),
			message: match (min_args, max_args) {
				(1, 1) => "The function expects 1 argument.".to_string(),
				(r, t) if r == t => format!("The function expects {r} arguments."),
				(r, t) => format!("The function expects {r} to {t} arguments."),
			},
		}
		.into());
	}
	Ok(())
}

/// Validate and coerce return value to declared type.
pub(crate) fn validate_return(
	func_name: &str,
	return_kind: Option<&Kind>,
	result: Value,
) -> anyhow::Result<Value> {
	match return_kind {
		Some(kind) => result.coerce_to_kind(kind).map_err(|e| {
			ExecError::ReturnCoerce {
				name: func_name.to_string(),
				error: Box::new(e),
			}
			.into()
		}),
		None => Ok(result),
	}
}

/// Renders a call's argument list, `(a, b, c)`, in the form the plan is
/// printed in.
pub(crate) fn fmt_call_args(f: &mut String, fmt: SqlFormat, args: &[Arc<dyn PhysicalExpr>]) {
	f.push('(');
	for (i, arg) in args.iter().enumerate() {
		if i > 0 {
			f.push_str(", ");
		}
		arg.fmt_sql(f, fmt);
	}
	f.push(')');
}

/// Helper to compute access mode from arguments.
pub(crate) fn args_access_mode(args: &[Arc<dyn PhysicalExpr>]) -> AccessMode {
	args.iter().map(|e| e.access_mode()).combine_all()
}

/// Helper to compute the maximum required context from arguments.
pub(crate) fn args_required_context(args: &[Arc<dyn PhysicalExpr>]) -> crate::exec::ContextLevel {
	args.iter().map(|e| e.required_context()).max().unwrap_or(crate::exec::ContextLevel::Root)
}
