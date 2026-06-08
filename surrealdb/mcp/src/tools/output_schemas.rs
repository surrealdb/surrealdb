//! Declarative output schemas for MCP tools.
//!
//! MCP clients can introspect each tool's `output_schema` to know the shape
//! of `structured_content` they will receive. The schemas here mirror the
//! JSON shapes produced by helpers in [`crate::tools::output`]. They are
//! attached to the generated [`rmcp::handler::server::router::tool::ToolRouter`]
//! after it is constructed, since the `#[tool]` attribute macro does not
//! expose an attribute for output schemas.
//!
//! All schemas are strict objects, as required by the MCP 2025-06-18 spec.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::JsonObject;
use serde_json::{Map, Value, json};

/// Schema for single-statement tool responses (select/create/insert/upsert/
/// update/delete/relate/run/info).
fn single_statement_schema() -> JsonObject {
	to_object(json!({
		"type": "object",
		"description": "A single SurrealQL statement result, either ok with a value or an error with a kind and message.",
		"required": ["status"],
		"properties": {
			"status": {
				"type": "string",
				"enum": ["ok", "error"],
				"description": "Whether the statement succeeded or failed.",
			},
			"value": {
				"description": "The statement result value (present when status == 'ok'). Type depends on the query.",
			},
			"error": {
				"type": "string",
				"description": "The SurrealDB error message (present when status == 'error').",
			},
			"kind": {
				"type": "string",
				"description": "Short error-kind classifier (e.g. 'parse', 'validation'). Present when status == 'error'.",
			},
			"time_ms": {
				"type": "number",
				"description": "Wall-clock time in milliseconds taken to execute the statement on the server.",
			},
			"truncated": {
				"type": "boolean",
				"description": "True if `value` was replaced by a truncation marker because it exceeded the configured size cap.",
			},
		},
	}))
}

/// Schema for the multi-statement `query` tool response.
fn multi_statement_schema() -> JsonObject {
	to_object(json!({
		"type": "object",
		"description": "Per-statement results for a multi-statement SurrealQL query.",
		"required": ["results", "has_errors"],
		"properties": {
			"results": {
				"type": "array",
				"description": "One entry per top-level SurrealQL statement, in order.",
				"items": {
					"type": "object",
					"required": ["index", "status"],
					"properties": {
						"index": {
							"type": "integer",
							"description": "Zero-based statement index.",
						},
						"status": {
							"type": "string",
							"enum": ["ok", "error"],
						},
						"value": {
							"description": "Statement value when status == 'ok'.",
						},
						"error": {
							"type": "string",
							"description": "SurrealDB error message when status == 'error'.",
						},
						"kind": {
							"type": "string",
							"description": "Short error-kind classifier when status == 'error'.",
						},
						"time_ms": {
							"type": "number",
						},
						"truncated": {
							"type": "boolean",
						},
					},
				},
			},
			"has_errors": {
				"type": "boolean",
				"description": "True when any statement failed.",
			},
		},
	}))
}

/// Schema for the `use` tool response.
fn use_schema() -> JsonObject {
	to_object(json!({
		"type": "object",
		"description": "The resolved namespace and database context after switching.",
		"properties": {
			"namespace": {
				"type": ["string", "null"],
				"description": "Active namespace, or null if not set.",
			},
			"database": {
				"type": ["string", "null"],
				"description": "Active database, or null if not set.",
			},
		},
	}))
}

/// Schema for the `list` tool response. `list` always issues an
/// `INFO FOR ... STRUCTURE` query and surfaces the requested subtree
/// (uniformly a JSON array of structured per-kind definitions) under
/// `items`, alongside a `truncated` flag so a real-world schema that
/// exceeds the configured `SURREAL_MCP_MAX_RESULT_BYTES` cap is
/// signalled to the caller rather than silently overflowing.
fn list_schema() -> JsonObject {
	to_object(json!({
		"type": "object",
		"description": "Structured definitions for the requested kind, one entry per entity, with a sibling truncation flag.",
		"required": ["items", "truncated"],
		"properties": {
			"items": {
				"type": "array",
				"description": "Per-kind structured definitions, ordered as the database returned them. Replaced by a truncation marker when the serialised payload exceeds the response cap.",
				"items": {
					"type": "object",
					"additionalProperties": true,
				},
			},
			"truncated": {
				"type": "boolean",
				"description": "True when `items` was replaced by a truncation marker because the serialised payload exceeded the configured cap.",
			},
		},
	}))
}

fn to_object(value: Value) -> JsonObject {
	match value {
		Value::Object(map) => map,
		_ => Map::new(),
	}
}

/// Attach the appropriate `output_schema` to every registered tool in the
/// router. Called once during [`crate::service::McpService::new`].
pub(crate) fn attach<S>(router: &mut ToolRouter<S>) {
	for (name, route) in router.map.iter_mut() {
		let schema = match name.as_ref() {
			"query" => multi_statement_schema(),
			"use" => use_schema(),
			"list" => list_schema(),
			// All other tools (select, create, insert, upsert, update, delete,
			// relate, run, info) emit a single-statement shape.
			"select" | "create" | "insert" | "upsert" | "update" | "delete" | "relate" | "run"
			| "info" => single_statement_schema(),
			_ => continue,
		};
		route.attr.output_schema = Some(Arc::new(schema));
	}
}
