//! Session functions for the engine.
//!
//! These functions access session information from the EvalContext.

use anyhow::Result;

use crate::define_context_function;
use crate::exec::EvalContext;
use crate::exec::function::ScalarFunction;
use crate::val::Value;

// =========================================================================
// Implementation functions
// =========================================================================

fn session_ac_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.ac.as_ref())
		.map(|ac| Value::String(ac.clone()))
		.unwrap_or(Value::None))
}

fn session_db_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.db.as_ref())
		.map(|db| Value::String(db.clone()))
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
		.map(|ip| Value::String(ip.clone()))
		.unwrap_or(Value::None))
}

fn session_ns_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.ns.as_ref())
		.map(|ns| Value::String(ns.clone()))
		.unwrap_or(Value::None))
}

fn session_origin_impl(ctx: &EvalContext<'_>) -> Result<Value> {
	Ok(ctx
		.session()
		.and_then(|s| s.origin.as_ref())
		.map(|origin| Value::String(origin.clone()))
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

define_context_function!(SessionAc, "session::ac", effects: EffectSet::READS_PRINCIPAL, fallibility: Fallibility::INFALLIBLE, () -> Any, session_ac_impl);
define_context_function!(SessionDb, "session::db", effects: EffectSet::READS_SESSION_DB, fallibility: Fallibility::INFALLIBLE, () -> Any, session_db_impl);
define_context_function!(SessionId, "session::id", effects: EffectSet::READS_PRINCIPAL, fallibility: Fallibility::INFALLIBLE, () -> Any, session_id_impl);
define_context_function!(SessionIp, "session::ip", effects: EffectSet::READS_PRINCIPAL, fallibility: Fallibility::INFALLIBLE, () -> Any, session_ip_impl);
define_context_function!(SessionNs, "session::ns", effects: EffectSet::READS_SESSION_DB, fallibility: Fallibility::INFALLIBLE, () -> Any, session_ns_impl);
define_context_function!(SessionOrigin, "session::origin", effects: EffectSet::READS_PRINCIPAL, fallibility: Fallibility::INFALLIBLE, () -> Any, session_origin_impl);
define_context_function!(SessionRd, "session::rd", effects: EffectSet::READS_PRINCIPAL, fallibility: Fallibility::INFALLIBLE, () -> Any, session_rd_impl);
define_context_function!(SessionToken, "session::token", effects: EffectSet::READS_PRINCIPAL, fallibility: Fallibility::INFALLIBLE, () -> Any, session_token_impl);

// =========================================================================
// Registration
// =========================================================================

/// The `session::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&SessionAc,
	&SessionDb,
	&SessionId,
	&SessionIp,
	&SessionNs,
	&SessionOrigin,
	&SessionRd,
	&SessionToken,
];
