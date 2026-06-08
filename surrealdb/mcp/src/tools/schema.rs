//! Schema introspection tools.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use surrealdb_core::dbs::QueryResult;

use super::output::cap_value;
use super::{
	single_statement_result, structured_success, tool_error_from_surreal, validate_table_name,
};
use crate::error::invalid_params;
use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InfoParams {
	/// Scope: "root", "ns", "db", or a table name. Defaults to most specific context.
	pub target: Option<String>,
}

/// Dump full introspection for a scope or table via `INFO FOR <scope>` / `INFO FOR TABLE`.
pub async fn info(session: &McpSession, params: InfoParams) -> Result<CallToolResult, ErrorData> {
	let query = match params.target.as_deref() {
		Some("root") => "INFO FOR ROOT".to_string(),
		Some("ns") | Some("namespace") => "INFO FOR NS".to_string(),
		Some("db") | Some("database") => "INFO FOR DB".to_string(),
		Some(table) => {
			validate_table_name(table)?;
			format!("INFO FOR TABLE {table}")
		}
		None => {
			if session.current_db().await.is_some() {
				"INFO FOR DB".to_string()
			} else if session.current_ns().await.is_some() {
				"INFO FOR NS".to_string()
			} else {
				"INFO FOR ROOT".to_string()
			}
		}
	};
	let mut results = session.execute(&query, None).await?;
	let result = results.pop().unwrap_or_else(|| {
		tracing::warn!(target: "surrealdb::mcp", "info() returned no statements");
		surrealdb_core::dbs::QueryResultBuilder::instant_none()
	});
	Ok(single_statement_result(result, session.config().max_result_bytes))
}

/// Kinds of schema entities that `list` can enumerate.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ListKind {
	// Root-scope
	Namespaces,
	Nodes,
	// Namespace-scope
	Databases,
	// Database-scope
	Tables,
	Functions,
	Analyzers,
	Params,
	Apis,
	Buckets,
	Models,
	Modules,
	Sequences,
	Configs,
	// Multi-scope (require `scope`)
	Users,
	Accesses,
	// Table-scope (require `table`)
	Fields,
	Indexes,
	Events,
}

/// Scope selector for kinds that exist at multiple levels.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ListScope {
	Root,
	Ns,
	Db,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListParams {
	/// Kind of entity to enumerate.
	pub kind: ListKind,
	/// Required when `kind` is one of: fields, indexes, events.
	pub table: Option<String>,
	/// Required when `kind` is one of: users, accesses. One of: root, ns, db.
	pub scope: Option<ListScope>,
}

/// Enumerate entities of a given kind, returning only the requested subtree of
/// `INFO FOR <scope> STRUCTURE` (or `INFO FOR TABLE STRUCTURE`) rather than the
/// full blob. This is significantly cheaper for LLM context than returning the
/// whole structure and makes kinds accessible one-at-a-time.
pub async fn list(session: &McpSession, p: ListParams) -> Result<CallToolResult, ErrorData> {
	// Per-kind scope/table requirements and the INFO query to issue.
	let (info_query, subtree_key) = resolve_list_target(&p)?;
	let results = session.execute(info_query.as_str(), None).await?;
	subtree_result(results, subtree_key, session.config().max_result_bytes)
}

/// Compute the INFO query and the top-level key whose value we should extract.
fn resolve_list_target(p: &ListParams) -> Result<(String, &'static str), ErrorData> {
	use ListKind::*;
	match p.kind {
		// Root scope
		Namespaces => {
			reject_table(p)?;
			reject_scope(p)?;
			Ok(("INFO FOR ROOT STRUCTURE".to_string(), "namespaces"))
		}
		Nodes => {
			reject_table(p)?;
			reject_scope(p)?;
			Ok(("INFO FOR ROOT STRUCTURE".to_string(), "nodes"))
		}
		// NS scope
		Databases => {
			reject_table(p)?;
			reject_scope(p)?;
			Ok(("INFO FOR NS STRUCTURE".to_string(), "databases"))
		}
		// DB scope
		Tables => db_scope(p, "tables"),
		Functions => db_scope(p, "functions"),
		Analyzers => db_scope(p, "analyzers"),
		Params => db_scope(p, "params"),
		Apis => db_scope(p, "apis"),
		Buckets => db_scope(p, "buckets"),
		Models => db_scope(p, "models"),
		Modules => db_scope(p, "modules"),
		Sequences => db_scope(p, "sequences"),
		Configs => db_scope(p, "configs"),
		// Multi-scope
		Users => multi_scope(p, "users"),
		Accesses => multi_scope(p, "accesses"),
		// Table scope
		Fields => table_scope(p, "fields"),
		Indexes => table_scope(p, "indexes"),
		Events => table_scope(p, "events"),
	}
}

fn db_scope(p: &ListParams, key: &'static str) -> Result<(String, &'static str), ErrorData> {
	reject_table(p)?;
	reject_scope(p)?;
	Ok(("INFO FOR DB STRUCTURE".to_string(), key))
}

fn multi_scope(p: &ListParams, key: &'static str) -> Result<(String, &'static str), ErrorData> {
	reject_table(p)?;
	let scope = p.scope.as_ref().ok_or_else(|| {
		invalid_params(format!("`scope` is required when kind is `{key}` (root, ns, or db)"))
	})?;
	let q = match scope {
		ListScope::Root => "INFO FOR ROOT STRUCTURE",
		ListScope::Ns => "INFO FOR NS STRUCTURE",
		ListScope::Db => "INFO FOR DB STRUCTURE",
	};
	Ok((q.to_string(), key))
}

fn table_scope(p: &ListParams, key: &'static str) -> Result<(String, &'static str), ErrorData> {
	reject_scope(p)?;
	let table = p
		.table
		.as_deref()
		.ok_or_else(|| invalid_params(format!("`table` is required when kind is `{key}`")))?;
	validate_table_name(table)?;
	Ok((format!("INFO FOR TABLE {table} STRUCTURE"), key))
}

fn reject_table(p: &ListParams) -> Result<(), ErrorData> {
	if p.table.is_some() {
		return Err(invalid_params(
			"`table` is only valid when kind is `fields`, `indexes`, or `events`",
		));
	}
	Ok(())
}

fn reject_scope(p: &ListParams) -> Result<(), ErrorData> {
	if p.scope.is_some() {
		return Err(invalid_params("`scope` is only valid when kind is `users` or `accesses`"));
	}
	Ok(())
}

/// Pluck the named subtree out of the first statement's result and return it
/// as a structured `CallToolResult`. Statement-level errors (e.g. the session
/// lacks permission, NS/DB doesn't exist) are surfaced via `is_error = true`
/// with the full SurrealDB error message so the LLM can self-correct.
///
/// The plucked subtree (always an array of structured per-kind definitions
/// from `INFO FOR ... STRUCTURE`) is run through [`cap_value`] before
/// emission so a `list` over a high-cardinality kind (e.g. `tables` or
/// `fields` on a real-world schema) cannot exhaust the LLM context window
/// by silently bypassing the documented `SURREAL_MCP_MAX_RESULT_BYTES`
/// cap. The truncation flag is surfaced in the envelope as a sibling of
/// `items` so callers can detect and recover.
fn subtree_result(
	results: Vec<QueryResult>,
	key: &str,
	max_bytes: Option<usize>,
) -> Result<CallToolResult, ErrorData> {
	let Some(first) = results.into_iter().next() else {
		return Err(crate::error::invalid_params("No result returned for INFO query"));
	};
	let value = match first.result {
		Ok(v) => v,
		Err(err) => {
			tracing::warn!(
				target: "surrealdb::mcp",
				kind = err.kind_str(),
				error = %err.message(),
				"INFO query failed while enumerating schema"
			);
			return Ok(tool_error_from_surreal(&err));
		}
	};

	let subtree = match value.into_json_value() {
		serde_json::Value::Object(mut map) => {
			map.remove(key).unwrap_or(serde_json::Value::Array(Vec::new()))
		}
		// The INFO statement always returns an object, but guard against it.
		_ => serde_json::Value::Array(Vec::new()),
	};
	let (items, truncated) = cap_value(subtree, max_bytes);
	Ok(structured_success(json!({
		"items": items,
		"truncated": truncated,
	})))
}
