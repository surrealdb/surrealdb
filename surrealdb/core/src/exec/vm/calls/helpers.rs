//! Shared helper functions for function physical expressions.

use super::closure::CompiledClosure;
use crate::exec::{Error as ExecError, EvalContext};
use crate::expr::{Error as ExprError, FlowResult, Kind};
use crate::val::{Closure, Value};

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
