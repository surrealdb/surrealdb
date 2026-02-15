//! Closure expressions - closure creation and invocation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, args_required_context, evaluate_args, validate_return};
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{BlockPhysicalExpr, EvalContext, PhysicalExpr};
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

// =============================================================================
// ClosureExec - closure value creation
// =============================================================================

/// Closure expression - |$x| $x * 2
#[derive(Debug, Clone)]
pub struct ClosureExec {
	pub(crate) closure: crate::expr::ClosureExpr,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ClosureExec {
	fn name(&self) -> &'static str {
		"Closure"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Closures are just values, they don't need any special context
		crate::exec::ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::dbs::ParameterCapturePass;
		use crate::val::Closure;

		// Capture parameters from the context that are referenced in the closure body
		let frozen_ctx = ctx.exec_ctx.ctx();
		let captures = ParameterCapturePass::capture(frozen_ctx, &self.closure.body);

		// Create a Value::Closure with the captured variables
		Ok(Value::Closure(Box::new(Closure::Expr {
			args: self.closure.args.clone(),
			returns: self.closure.returns.clone(),
			body: self.closure.body.clone(),
			captures,
		})))
	}

	fn access_mode(&self) -> AccessMode {
		// Closures themselves are read-only (they're values)
		// What they do when called is a different matter
		AccessMode::ReadOnly
	}
}

impl ToSql for ClosureExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.closure.fmt_sql(f, fmt);
	}
}

// =============================================================================
// ClosureCallExec - for invoking closures stored in parameters
// =============================================================================

/// Closure call expression - $closure(args...)
///
/// Invokes a closure value with the provided arguments. The target expression
/// must evaluate to a `Value::Closure`.
#[derive(Debug, Clone)]
pub struct ClosureCallExec {
	/// The expression that evaluates to a closure value
	pub(crate) target: Arc<dyn PhysicalExpr>,
	/// The argument expressions to pass to the closure
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ClosureCallExec {
	fn name(&self) -> &'static str {
		"ClosureCall"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// The closure body may require any context level, so be conservative
		// and take the max of target and arguments
		let target_ctx = self.target.required_context();
		let args_ctx = args_required_context(&self.arguments);
		target_ctx.max(args_ctx)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::val::Closure;

		// 1. Evaluate the target expression to get the closure
		let target_value = self.target.evaluate(ctx.clone()).await?;

		let closure = match target_value {
			Value::Closure(c) => c,
			other => {
				return Err(Error::InvalidFunction {
					name: "ANONYMOUS".to_string(),
					message: format!("'{}' is not a function", other.kind_of()),
				}
				.into());
			}
		};

		// 2. Evaluate all argument expressions
		let evaluated_args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// 3. Invoke the closure based on its type
		match closure.as_ref() {
			Closure::Expr {
				args: arg_spec,
				returns,
				body,
				captures,
			} => {
				// Create isolated execution context with captured variables
				let mut isolated_ctx = ctx.exec_ctx.clone();
				for (name, value) in captures.clone() {
					isolated_ctx = isolated_ctx.with_param(name, value);
				}

				// Check for missing required arguments
				if arg_spec.len() > evaluated_args.len()
					&& let Some((param, kind)) =
						arg_spec[evaluated_args.len()..].iter().find(|(_, k)| !k.can_be_none())
				{
					return Err(Error::InvalidFunctionArguments {
						name: "ANONYMOUS".to_string(),
						message: format!(
							"Expected a value of type '{}' for argument {}",
							kind.to_sql(),
							param.to_sql()
						),
					}
					.into());
				}

				// Bind arguments to parameter names with type coercion
				let mut local_params: HashMap<String, Value> = HashMap::new();
				for ((param, kind), arg_value) in arg_spec.iter().zip(evaluated_args.into_iter()) {
					let coerced = arg_value.coerce_to_kind(kind).map_err(|_| {
						Error::InvalidFunctionArguments {
							name: "ANONYMOUS".to_string(),
							message: format!(
								"Expected a value of type '{}' for argument {}",
								kind.to_sql(),
								param.to_sql()
							),
						}
					})?;
					local_params.insert(param.clone().into_string(), coerced);
				}

				// Add parameters to the execution context
				for (name, value) in &local_params {
					isolated_ctx = isolated_ctx.with_param(name.clone(), value.clone());
				}

				// Execute the closure body
				let block_expr = BlockPhysicalExpr {
					block: crate::expr::Block(vec![body.clone()]),
				};
				let eval_ctx = EvalContext {
					exec_ctx: &isolated_ctx,
					current_value: ctx.current_value,
					local_params: Some(&local_params),
					recursion_ctx: None,
					document_root: None,
				};

				let result = match block_expr.evaluate(eval_ctx).await {
					Ok(v) => v,
					Err(ControlFlow::Return(v)) => v,
					Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
						// BREAK/CONTINUE inside a closure (outside of loop) is an error
						return Err(Error::InvalidControlFlow.into());
					}
					Err(e) => return Err(e),
				};

				// Coerce return value to declared type if specified
				Ok(validate_return("ANONYMOUS", returns.as_ref(), result)?)
			}
			Closure::Builtin(_) => {
				// Builtin closures are not yet supported in the streaming executor
				// They require the legacy compute path with Stk
				Err(anyhow::anyhow!(
					"Builtin closures are not yet supported in the streaming executor"
				)
				.into())
			}
		}
	}

	fn access_mode(&self) -> AccessMode {
		// Closures can potentially do anything, so be conservative
		AccessMode::ReadWrite
			.combine(self.target.access_mode())
			.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for ClosureCallExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.target.fmt_sql(f, fmt);
		f.push_str("(...)");
	}
}
