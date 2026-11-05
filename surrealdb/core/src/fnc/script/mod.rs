#![cfg(feature = "scripting")]
// rquickjs `#[js::methods]`, `#[qjs(constructor)]`, and `FromJs` glue require owned
// `Ctx`/`Value`/`String` parameters in user-written signatures, so this lint fires
// throughout the module by design.
#![allow(clippy::needless_pass_by_value)]

pub use main::run;
pub(crate) use surrealdb_expr::js::classes;
pub(crate) mod config;
mod error;
mod globals;
mod main;
mod modules;

#[cfg(feature = "http")]
mod fetch;
#[cfg(not(feature = "http"))]
mod fetch_stub;
#[cfg(not(feature = "http"))]
use self::fetch_stub as fetch;

#[cfg(test)]
mod tests;

/// Refuse a builtin or `fetch` the active no-write frame does not admit, before
/// the script bridge runs it.
///
/// The bridge dispatches a builtin straight through `fnc::asynchronous` /
/// `fnc::synchronous`, and `fetch` reaches the network through its own client,
/// so neither passes the VM's own guard
/// (`crate::exec::vm::host::refuse_mutating_builtin`). A LIVE clause, and every
/// other no-write frame, refuses a builtin that writes to an external system,
/// advances state or blocks; the refusal is carried on
/// [`crate::dbs::Options::no_write`] (mirrored there by
/// `ExecutionContext::refusing_writes`), so a write a subscriber's projection
/// reaches through a script is refused the way a direct `http::post` is.
///
/// `name` and `effects` are the builtin's, as the registry declares them; a
/// `fetch` is judged by the effect its HTTP method and body imply.
pub(crate) fn refuse_builtin_in_frame(
	no_write: Option<crate::dbs::NoWriteFrame>,
	name: &str,
	effects: crate::expr::logical::EffectSet,
) -> anyhow::Result<()> {
	if let Some(frame) = no_write
		&& frame.refuses_builtin_call(name, effects)
	{
		let construct = crate::exec::clause_effects::builtin_text(name, effects);
		anyhow::bail!(frame.side_effect_error(&construct));
	}
	Ok(())
}
