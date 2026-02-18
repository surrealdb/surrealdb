//! Shared helper functions for function physical expressions.

use std::sync::Arc;

use crate::catalog::Permission;
use crate::err::Error;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{AccessMode, CombineAccessModes};
use crate::expr::{FlowResult, Kind};
use crate::val::Value;

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

/// Check function permission.
pub(crate) async fn check_permission(
	permission: &Permission,
	func_name: &str,
	ctx: &EvalContext<'_>,
) -> FlowResult<()> {
	match permission {
		Permission::Full => Ok(()),
		Permission::None => Err(Error::FunctionPermissions {
			name: func_name.to_string(),
		}
		.into()),
		Permission::Specific(expr) => {
			// Plan and evaluate the permission expression
			match expr_to_physical_expr(expr.clone(), ctx.exec_ctx.ctx()).await {
				Ok(phys_expr) => {
					let result = phys_expr.evaluate(ctx.clone()).await?;
					if !result.is_truthy() {
						Err(Error::FunctionPermissions {
							name: func_name.to_string(),
						}
						.into())
					} else {
						Ok(())
					}
				}
				Err(_) => {
					// If we can't plan the expression, deny by default
					Err(Error::FunctionPermissions {
						name: func_name.to_string(),
					}
					.into())
				}
			}
		}
	}
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
		return Err(Error::InvalidFunctionArguments {
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
			Error::ReturnCoerce {
				name: func_name.to_string(),
				error: Box::new(e),
			}
			.into()
		}),
		None => Ok(result),
	}
}

/// Helper to compute access mode from arguments.
pub(crate) fn args_access_mode(args: &[Arc<dyn PhysicalExpr>]) -> AccessMode {
	args.iter().map(|e| e.access_mode()).combine_all()
}

/// Helper to compute the maximum required context from arguments.
pub(crate) fn args_required_context(args: &[Arc<dyn PhysicalExpr>]) -> crate::exec::ContextLevel {
	args.iter().map(|e| e.required_context()).max().unwrap_or(crate::exec::ContextLevel::Root)
}
