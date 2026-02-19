//! SurrealQL `agent::*` function implementations.
use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::val::Value;

/// Run an agent with the given input.
///
/// # SurrealQL
///
/// ```surql
/// ai::agent::run('support', { message: 'Where is my order?' })
/// ```
#[cfg(not(feature = "ai"))]
pub async fn run(
	(_ctx, _opt): (&FrozenContext, &Options),
	(_agent_name, _input): (String, Value),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Run an agent with the given input.
#[cfg(feature = "ai")]
pub async fn run(
	(ctx, opt): (&FrozenContext, &Options),
	(agent_name, input): (String, Value),
) -> Result<Value> {
	use crate::ai::agent::engine::{AgentInput, run as run_agent};
	use crate::catalog::providers::DatabaseProvider;

	// Parse input object
	let input_obj = match &input {
		Value::Object(obj) => obj,
		_ => anyhow::bail!(Error::InvalidFunctionArguments {
			name: "ai::agent::run".to_owned(),
			message: "Second argument must be an object with a 'message' field".to_owned(),
		}),
	};

	let message = input_obj
		.get("message")
		.and_then(|v| match v {
			Value::String(s) => Some(s.clone()),
			_ => None,
		})
		.ok_or_else(|| {
			anyhow::Error::new(Error::InvalidFunctionArguments {
				name: "ai::agent::run".to_owned(),
				message: "Input object must contain a 'message' string field".to_owned(),
			})
		})?;

	let session_id = input_obj.get("session_id").and_then(|v| match v {
		Value::String(s) => Some(s.clone()),
		_ => None,
	});

	// Load agent definition
	let txn = ctx.tx();
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let agent = txn.get_db_agent(ns, db, &agent_name).await?;

	let agent_input = AgentInput {
		message,
		session_id,
	};

	let output = run_agent(ctx, opt, &agent, agent_input).await?;

	Ok(output.into())
}
