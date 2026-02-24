//! Session functions for the streaming executor.
//!
//! These functions access session information from the EvalContext.

use anyhow::Result;

use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::EvalContext;
use crate::val::Value;
use crate::{define_context_function, register_functions};

// =========================================================================
// Implementation functions
// =========================================================================

fn session_ac_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.ac.as_ref())
		.map(|ac| Value::from(ac.clone()))
		.unwrap_or(Value::None))
}

fn session_db_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.db.as_ref())
		.map(|db| Value::from(db.clone()))
		.unwrap_or(Value::None))
}

fn session_id_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.id.as_ref())
		.map(|id| Value::Uuid((*id).into()))
		.unwrap_or(Value::None))
}

fn session_ip_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.ip.as_ref())
		.map(|ip| Value::from(ip.clone()))
		.unwrap_or(Value::None))
}

fn session_ns_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.ns.as_ref())
		.map(|ns| Value::from(ns.clone()))
		.unwrap_or(Value::None))
}

fn session_origin_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.origin.as_ref())
		.map(|origin| Value::from(origin.clone()))
		.unwrap_or(Value::None))
}

fn session_rd_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx.session().and_then(|s| s.rd.clone()).unwrap_or(Value::None))
}

fn session_token_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx.session().and_then(|s| s.token.clone()).unwrap_or(Value::None))
}

// =========================================================================
// Function definitions using the macro
// =========================================================================

define_context_function!(SessionAc, "session::ac", () -> Any, session_ac_impl);
define_context_function!(SessionDb, "session::db", () -> Any, session_db_impl);
define_context_function!(SessionId, "session::id", () -> Any, session_id_impl);
define_context_function!(SessionIp, "session::ip", () -> Any, session_ip_impl);
define_context_function!(SessionNs, "session::ns", () -> Any, session_ns_impl);
define_context_function!(SessionOrigin, "session::origin", () -> Any, session_origin_impl);
define_context_function!(SessionRd, "session::rd", () -> Any, session_rd_impl);
define_context_function!(SessionToken, "session::token", () -> Any, session_token_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		SessionAc,
		SessionDb,
		SessionId,
		SessionIp,
		SessionNs,
		SessionOrigin,
		SessionRd,
		SessionToken,
	);
}
