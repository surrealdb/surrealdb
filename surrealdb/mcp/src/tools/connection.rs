//! Context-switching tool: polymorphic `use` for namespace and/or database.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::output::tool_error;
use super::{structured_success, validate_identifier};
use crate::error::invalid_params;
use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UseParams {
	/// Namespace to switch to. Either this or `database` must be set.
	pub namespace: Option<String>,
	/// Database to switch to. If set without `namespace`, switches DB under
	/// the current namespace.
	pub database: Option<String>,
}

/// Switch the active namespace, the active database, or both at once. Mirrors
/// SurrealQL's `USE NS <x> DB <y>` one-shot so an LLM never needs two calls to
/// change context.
///
/// Unlike the RPC `use` verb, this tool does not auto-provision missing
/// namespaces or databases: it refuses to switch to a non-existent target.
/// Auto-provision is an admin-scale side-effect that should be explicit
/// (e.g. a `DEFINE NAMESPACE` / `DEFINE DATABASE` via the raw `query` tool).
pub async fn r#use(session: &McpSession, params: UseParams) -> Result<CallToolResult, ErrorData> {
	if params.namespace.is_none() && params.database.is_none() {
		return Err(invalid_params("At least one of `namespace` or `database` must be provided"));
	}

	if let Some(ns) = &params.namespace {
		validate_identifier(ns)?;
	}
	if let Some(db) = &params.database {
		validate_identifier(db)?;
	}

	// Permission gate: mirror the RPC layer and reject sessions that aren't
	// allowed to query at all. This prevents a guest session from silently
	// pinning a ns/db that it cannot actually use.
	let (allowed, base_session) = session
		.with_session(|s| (session.datastore().allows_query_by_subject(s.au.as_ref()), s.clone()))
		.await;
	if !allowed {
		return Ok(tool_error(
			"NotAllowed",
			"Not allowed to switch namespace/database with the current session",
		));
	}

	// Resolve the target namespace: prefer the explicit arg, fall back to
	// whatever's already on the session. A database switch with no namespace
	// is only valid if the session is already scoped to a namespace.
	let target_ns = params.namespace.clone().or_else(|| base_session.ns.clone());

	// Existence checks via `INFO FOR NS` / `INFO FOR DB` run under a
	// temporary session that carries the same auth as the caller but with
	// the prospective ns/db applied. If the target does not exist the
	// datastore surfaces `NsNotFound` / `DbNotFound`; any permission denial
	// from the same check is a strong signal the caller shouldn't be
	// switching there either, so we surface both as tool errors.
	if let Some(ns) = &params.namespace {
		let probe = {
			let mut s = base_session.clone();
			s.ns = Some(ns.clone());
			s.db = None;
			s
		};
		match session.datastore().execute("INFO FOR NS", &probe, None).await {
			Ok(results) => {
				if let Some(first) = results.into_iter().next()
					&& let Err(err) = first.result
				{
					return Ok(tool_error_for(&err, "namespace", ns));
				}
			}
			Err(err) => return Ok(tool_error_for(&err, "namespace", ns)),
		}
	}

	if let Some(db) = &params.database {
		let Some(ns) = target_ns.as_ref() else {
			return Ok(tool_error(
				"Validation",
				"Cannot switch database without a namespace: set `namespace` first or pass both",
			));
		};
		let probe = {
			let mut s = base_session.clone();
			s.ns = Some(ns.clone());
			s.db = Some(db.clone());
			s
		};
		match session.datastore().execute("INFO FOR DB", &probe, None).await {
			Ok(results) => {
				if let Some(first) = results.into_iter().next()
					&& let Err(err) = first.result
				{
					return Ok(tool_error_for(&err, "database", db));
				}
			}
			Err(err) => return Ok(tool_error_for(&err, "database", db)),
		}
	}

	// All checks passed; commit the switch.
	if let Some(ns) = &params.namespace {
		session.use_ns(ns).await?;
	}
	if let Some(db) = &params.database {
		session.use_db(db).await?;
	}

	// Surface the resolved context so the LLM sees the final (ns, db) state.
	let ns = session.current_ns().await;
	let db = session.current_db().await;
	Ok(structured_success(json!({
		"namespace": ns,
		"database": db,
	})))
}

/// Translate a SurrealDB error encountered during a `use` existence probe
/// into an in-band tool error.
///
/// `INFO FOR NS` / `INFO FOR DB` run at namespace / database context level,
/// which means the executor's context builder will try to `get_or_add_ns`
/// / `get_or_add_db_upwards` on the probed target. When the target does
/// not exist this falls through to a write on what the planner decided was
/// a read-only transaction, surfacing as
/// `Couldn't write to a read only transaction`. That only ever means "the
/// target doesn't exist", so we promote it to a `NotFound` message. Any
/// other error (permission denial, storage fault) is passed through with
/// its original kind so the LLM can distinguish auth failures from typos.
fn tool_error_for(
	err: &surrealdb_types::Error,
	kind_label: &'static str,
	target: &str,
) -> CallToolResult {
	let msg = err.message();
	let lower = msg.to_ascii_lowercase();
	let is_missing = (lower.contains("not found") && lower.contains(kind_label))
		|| lower.contains("read only transaction");
	if is_missing {
		tool_error("NotFound", format!("The {kind_label} '{target}' does not exist"))
	} else {
		tool_error(static_kind(err.kind_str()), format!("{msg} (target {kind_label}: {target})"))
	}
}

/// `rmcp`'s `tool_error` takes a `&'static str` for the kind, so we map
/// SurrealDB's runtime kind string into a stable set of literals.
fn static_kind(kind: &str) -> &'static str {
	match kind {
		"Validation" => "Validation",
		"NotAllowed" => "NotAllowed",
		"NotFound" => "NotFound",
		"AlreadyExists" => "AlreadyExists",
		"Query" => "Query",
		"Configuration" => "Configuration",
		"Serialization" => "Serialization",
		"Connection" => "Connection",
		"Thrown" => "Thrown",
		_ => "Internal",
	}
}
