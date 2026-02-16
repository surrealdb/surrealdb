//! User-defined function expression - fn::my_function(), etc.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{
	args_access_mode, args_required_context, check_permission, evaluate_args, validate_arg_count,
	validate_return,
};
use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{BlockPhysicalExpr, EvalContext, PhysicalExpr};
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// User-defined function expression - fn::my_function(), etc.
///
/// These functions are stored in the database and retrieved at runtime.
#[derive(Debug, Clone)]
pub struct UserDefinedFunctionExec {
	/// Function name without the "fn::" prefix
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for UserDefinedFunctionExec {
	fn name(&self) -> &'static str {
		"UserDefinedFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// User-defined functions are stored in the database, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let func_name = format!("fn::{}", self.name);

		// 1. Require database context
		let db_ctx = ctx.exec_ctx.database().map_err(|_| {
			anyhow::anyhow!("Custom function '{}' requires database context", func_name)
		})?;

		// 2. Check if function is allowed by capabilities
		if !ctx.capabilities().allows_function_name(&func_name) {
			return Err(Error::FunctionNotAllowed(func_name).into());
		}

		// 3. Retrieve function definition
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let func_def = ctx
			.txn()
			.get_db_function(ns_id, db_id, &self.name)
			.await
			.map_err(|e| anyhow::anyhow!("Function '{}' not found: {}", func_name, e))?;

		// 4. Apply auth limiting — cap the caller's privileges to the definer's auth level,
		//    matching the old compute path behaviour.
		let auth_limit = crate::iam::AuthLimit::try_from(&func_def.auth_limit).map_err(|e| {
			anyhow::anyhow!("Invalid auth limit on function '{}': {}", func_name, e)
		})?;
		let limited_ctx = ctx.exec_ctx.with_limited_auth(&auth_limit);
		let ctx = EvalContext {
			exec_ctx: &limited_ctx,
			current_value: ctx.current_value,
			local_params: ctx.local_params,
			recursion_ctx: ctx.recursion_ctx,
			document_root: ctx.document_root,
		};

		// 5. Check permissions (with limited auth)
		check_permission(&func_def.permissions, &func_name, &ctx).await?;

		// 6. Evaluate all arguments
		let evaluated_args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// 7. Validate argument count
		validate_arg_count(&func_name, evaluated_args.len(), &func_def.args)?;

		// 8. Create isolated context with function parameters bound
		let mut local_params: HashMap<String, Value> = HashMap::new();
		for ((param_name, kind), arg_value) in func_def.args.iter().zip(evaluated_args.into_iter())
		{
			let coerced =
				arg_value.coerce_to_kind(kind).map_err(|e| Error::InvalidFunctionArguments {
					name: func_name.clone(),
					message: format!("Failed to coerce argument `${param_name}`: {e}"),
				})?;
			local_params.insert(param_name.clone(), coerced);
		}

		// 9. Create a new execution context with the parameters
		let mut isolated_ctx = limited_ctx.clone();
		for (name, value) in &local_params {
			isolated_ctx = isolated_ctx.with_param(name.clone(), value.clone());
		}

		// 9. Execute the function block
		let block_expr = BlockPhysicalExpr {
			block: func_def.block.clone(),
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
				// BREAK/CONTINUE inside a function (outside of loop) is an error
				return Err(Error::InvalidControlFlow.into());
			}
			Err(e) => return Err(e),
		};

		// 10. Validate and coerce return type
		Ok(validate_return(&func_name, func_def.returns.as_ref(), result)?)
	}

	fn access_mode(&self) -> AccessMode {
		// Custom functions are always potentially read-write
		AccessMode::ReadWrite.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for UserDefinedFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("fn::");
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}
