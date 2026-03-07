//! Agent functions for the streaming executor.
//!
//! Provides `ai::agent::run` for the streaming executor path.
//! When the `ai` feature is disabled, returns an `AiDisabled` error.

use anyhow::Result;

#[cfg(feature = "ai")]
use crate::catalog::Permission;
#[cfg(feature = "ai")]
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::EvalContext;
#[cfg(feature = "ai")]
use crate::exec::planner::expr_to_physical_expr;
#[cfg(feature = "ai")]
use crate::expr::ControlFlow;
use crate::val::Value;
use crate::{define_async_function, register_functions};

#[cfg(feature = "ai")]
async fn check_agent_permission(
	permission: &Permission,
	agent_name: &str,
	ctx: &EvalContext<'_>,
) -> Result<()> {
	match permission {
		Permission::Full => Ok(()),
		Permission::None => Err(Error::AgentPermissions {
			name: agent_name.to_string(),
		}
		.into()),
		Permission::Specific(expr) => {
			match expr_to_physical_expr(expr.clone(), ctx.exec_ctx.ctx()).await {
				Ok(phys_expr) => {
					let result = phys_expr.evaluate(ctx.clone()).await.map_err(|cf| match cf {
						ControlFlow::Err(e) => e,
						other => anyhow::anyhow!("{other}"),
					})?;
					if !result.is_truthy() {
						Err(Error::AgentPermissions {
							name: agent_name.to_string(),
						}
						.into())
					} else {
						Ok(())
					}
				}
				Err(_) => Err(Error::AgentPermissions {
					name: agent_name.to_string(),
				}
				.into()),
			}
		}
	}
}

#[cfg(feature = "ai")]
async fn agent_run_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::ai::agent::engine::{AgentInput, run as run_agent};
	use crate::catalog::providers::DatabaseProvider;
	use crate::dbs::capabilities::ExperimentalTarget;

	if !ctx.capabilities().allows_experimental(&ExperimentalTarget::Ai) {
		return Err(Error::InvalidFunction {
			name: "ai::agent::run".to_string(),
			message: "Experimental capability `ai` is not enabled".to_string(),
		}
		.into());
	}

	let agent_name = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			anyhow::bail!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::agent::run".to_owned(),
				message: format!(
					"The first argument must be a string agent name, got: {}",
					v.kind_of()
				),
			})
		}
		None => {
			anyhow::bail!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::agent::run".to_owned(),
				message: "Missing agent name argument".to_owned(),
			})
		}
	};

	let input_obj = match args.get(1) {
		Some(Value::Object(obj)) => obj,
		Some(v) => {
			anyhow::bail!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::agent::run".to_owned(),
				message: format!(
					"The second argument must be an object with a 'message' field, got: {}",
					v.kind_of()
				),
			})
		}
		None => {
			anyhow::bail!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::agent::run".to_owned(),
				message: "Missing input object argument".to_owned(),
			})
		}
	};

	let message = input_obj
		.get("message")
		.and_then(|v| match v {
			Value::String(s) => Some(s.clone()),
			_ => None,
		})
		.ok_or_else(|| {
			anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::agent::run".to_owned(),
				message: "Input object must contain a 'message' string field".to_owned(),
			})
		})?;

	let session_id = input_obj.get("session_id").and_then(|v| match v {
		Value::String(s) => Some(s.clone()),
		_ => None,
	});

	let frozen_ctx = ctx.exec_ctx.ctx();

	// Check agent is allowed by capabilities
	crate::ai::chat::check_ai_agent_allowed(frozen_ctx, &agent_name)?;

	let opt = ctx.exec_ctx.options().ok_or_else(|| {
		anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
			name: "ai::agent::run".to_owned(),
			message: "Execution context is missing Options".to_owned(),
		})
	})?;

	let txn = frozen_ctx.tx();
	let (ns, db) = frozen_ctx.get_ns_db_ids(opt).await?;
	let agent = txn.get_db_agent(ns, db, &agent_name).await?;

	// Check agent's provider is allowed by capabilities
	let (provider_name, _) = crate::ai::chat::parse_model_id(&agent.model.model_id)?;
	crate::ai::chat::check_ai_provider_allowed(frozen_ctx, provider_name)?;

	// Check agent permissions
	check_agent_permission(&agent.permissions, &agent_name, ctx).await?;

	let agent_input = AgentInput {
		message,
		session_id,
	};

	let output = run_agent(frozen_ctx, opt, &agent, agent_input).await?;

	Ok(output.into())
}

#[cfg(not(feature = "ai"))]
async fn agent_run_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	Err(anyhow::anyhow!(crate::err::Error::AiDisabled))
}

define_async_function!(
	AgentRun,
	"ai::agent::run",
	(agent_name: String, input: Any) -> Any,
	agent_run_impl
);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, AgentRun,);
}
