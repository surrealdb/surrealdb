//! Declarative output schemas for MCP tools.
//!
//! MCP clients introspect a tool's `output_schema` to know the shape of the
//! `structured_content` they will receive. Clients also *validate* against it:
//! the MCP TypeScript SDK rejects any result whose structured content fails the
//! advertised schema, and it does so whenever structured content is present —
//! including on results that carry `is_error = true`, which this server always
//! populates. A schema that omits a shape a tool can emit therefore turns a
//! working call into a client-side protocol error.
//!
//! Each schema is consequently the union of every branch its tool can produce,
//! success and in-band failure alike, and is keyed to the helper in
//! [`crate::tools::output`] the tool formats through rather than to the tool's
//! name. Adding a tool means adding it to [`attach`]: a tool with no arm there
//! ships without a schema, which
//! `service::tests::every_advertised_tool_carries_a_root_object_output_schema`
//! rejects.
//!
//! Every schema is an object at the root, which the MCP spec requires of an
//! output schema and SDK clients enforce when parsing `tools/list`. Unions are
//! expressed as an `anyOf` alongside that root `type`. No schema closes
//! `additionalProperties`: a client must tolerate keys added by a later server.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::JsonObject;
use serde_json::{Map, Value, json};

/// The marker [`crate::tools::output::cap_value`] substitutes for a payload
/// whose serialised size exceeds the configured cap. It can stand in for any
/// capped value, so every schema position that carries one accepts it.
fn truncation_marker_schema() -> Value {
	json!({
		"type": "object",
		"description": "Stand-in for a payload that exceeded the configured response cap.",
		"required": ["$truncated", "original_bytes", "limit_bytes", "message"],
		"properties": {
			"$truncated": {
				"const": true,
				"description": "Self-identifying marker key. `$`-prefixed so it cannot collide with a sibling `truncated` flag on the envelope.",
			},
			"original_bytes": {
				"type": "integer",
				"description": "Serialised size of the omitted payload.",
			},
			"limit_bytes": {
				"type": "integer",
				"description": "Cap that the payload exceeded.",
			},
			"message": {
				"type": "string",
				"description": "Human-readable explanation naming the cap and how to work within it.",
			},
		},
	})
}

/// A statement that succeeded. `value` carries the statement's result, or a
/// [`truncation_marker_schema`] in its place when the result was capped.
fn statement_ok_schema() -> Value {
	json!({
		"type": "object",
		"description": "A SurrealQL statement that succeeded.",
		"required": ["status", "value", "truncated", "time_ms"],
		"properties": {
			"status": {
				"const": "ok",
			},
			"value": {
				"description": "The statement result. Type depends on the query; replaced by a truncation marker when `truncated` is true.",
			},
			"truncated": {
				"type": "boolean",
				"description": "True when `value` was replaced by a truncation marker because it exceeded the configured size cap.",
			},
			"time_ms": {
				"type": "number",
				"description": "Wall-clock time in milliseconds taken to execute the statement on the server.",
			},
		},
	})
}

/// A statement that failed. Carries no `value`: the error text and its kind
/// take its place.
fn statement_error_schema() -> Value {
	json!({
		"type": "object",
		"description": "A SurrealQL statement that failed. The statement is reported in band; the call itself succeeded.",
		"required": ["status", "error", "kind", "time_ms"],
		"properties": {
			"status": {
				"const": "error",
			},
			"error": {
				"type": "string",
				"description": "The SurrealDB error message, identical to what `/sql` and `/rpc` return for the same statement.",
			},
			"kind": {
				"type": "string",
				"description": "Short error-kind classifier (e.g. 'Validation', 'NotFound') for branching without parsing the message.",
			},
			"time_ms": {
				"type": "number",
				"description": "Wall-clock time in milliseconds taken to execute the statement on the server.",
			},
		},
	})
}

/// Add the zero-based `index` that a per-statement entry inside a
/// [`batch_schema`] `results` array carries and a standalone envelope does not.
///
/// Panics if handed anything other than one of the object schemas above, which
/// are literals in this module.
fn with_index(mut schema: Value) -> Value {
	let object = schema.as_object_mut().expect("statement schema is an object");
	object
		.get_mut("properties")
		.and_then(Value::as_object_mut)
		.expect("statement schema declares properties")
		.insert(
			"index".to_string(),
			json!({
				"type": "integer",
				"minimum": 0,
				"description": "Zero-based position of this statement in the submitted query.",
			}),
		);
	object
		.get_mut("required")
		.and_then(Value::as_array_mut)
		.expect("statement schema declares required")
		.push(json!("index"));
	schema
}

/// The per-statement envelope emitted when a query resolved to a number of
/// statements other than one.
fn batch_schema() -> Value {
	json!({
		"type": "object",
		"description": "Per-statement results for a query that did not resolve to exactly one statement.",
		"required": ["results", "has_errors"],
		"properties": {
			"results": {
				"type": "array",
				"description": "One entry per top-level statement, in submission order.",
				"items": {
					"anyOf": [with_index(statement_ok_schema()), with_index(statement_error_schema())],
				},
			},
			"has_errors": {
				"type": "boolean",
				"description": "True when any statement failed. Mixed success and failure still reports the call as successful so the caller can see which statements went through.",
			},
		},
	})
}

/// An in-band tool failure raised by the tool itself rather than by a
/// statement: [`crate::tools::output::tool_error`] and
/// [`crate::tools::output::tool_error_from_surreal`].
fn tool_error_schema() -> Value {
	json!({
		"type": "object",
		"description": "A failure the tool raised before or instead of running a statement. Accompanied by `isError`.",
		"required": ["error", "kind"],
		"properties": {
			"error": {
				"type": "string",
				"description": "Human-readable failure message.",
			},
			"kind": {
				"type": "string",
				"description": "Short error-kind classifier for branching without parsing the message.",
			},
		},
	})
}

/// Wrap `branches` as a root-level object schema. The root `type` is what the
/// MCP spec requires of an output schema; the union rides alongside it.
fn any_of(description: &str, branches: &[Value]) -> JsonObject {
	to_object(json!({
		"type": "object",
		"description": description,
		"anyOf": branches,
	}))
}

/// Schema for tools formatting through
/// [`crate::tools::output::single_statement_result`], which always describes
/// exactly one statement: `run` and `info`.
fn single_statement_schema() -> JsonObject {
	any_of(
		"The outcome of a single SurrealQL statement.",
		&[statement_ok_schema(), statement_error_schema()],
	)
}

/// Schema for tools formatting through
/// [`crate::tools::output::multi_statement_result`], directly (`query`, `gql`)
/// or via `crate::tools::crud::collapse_single` (the CRUD family).
///
/// Both helpers emit the single-statement shape for a lone statement and the
/// [`batch_schema`] envelope otherwise, so all three branches are reachable
/// from every tool that uses them. A CRUD tool reaches the batch branch because
/// its clause arguments are raw SurrealQL fragments: a `where_clause` carrying
/// a statement separator submits more than one statement.
fn multi_statement_schema() -> JsonObject {
	any_of(
		"The outcome of a SurrealQL query: the statement itself when it resolved to exactly one, otherwise a per-statement batch.",
		&[statement_ok_schema(), statement_error_schema(), batch_schema()],
	)
}

/// Schema for the `use` tool.
fn use_schema() -> JsonObject {
	any_of(
		"The resolved namespace and database context after switching, or the reason the switch was refused.",
		&[
			json!({
				"type": "object",
				"description": "The context in force after the switch.",
				"required": ["namespace", "database"],
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
			}),
			tool_error_schema(),
		],
	)
}

/// Schema for the `list` tool, which surfaces one subtree of
/// `INFO FOR ... STRUCTURE` under `items` alongside a truncation flag, so a
/// real-world schema that exceeds the configured cap is signalled rather than
/// silently overflowing the caller's context.
fn list_schema() -> JsonObject {
	any_of(
		"Structured definitions for the requested kind, or the reason they could not be read.",
		&[
			json!({
				"type": "object",
				"description": "One entry per entity of the requested kind, with a sibling truncation flag.",
				"required": ["items", "truncated"],
				"properties": {
					"items": {
						"description": "Per-kind structured definitions, ordered as the database returned them. Replaced by a truncation marker when `truncated` is true.",
						"anyOf": [
							{
								"type": "array",
								"items": {
									"type": "object",
									"additionalProperties": true,
								},
							},
							truncation_marker_schema(),
						],
					},
					"truncated": {
						"type": "boolean",
						"description": "True when `items` was replaced by a truncation marker because the serialised payload exceeded the configured cap.",
					},
				},
			}),
			tool_error_schema(),
		],
	)
}

/// Schema for the `graphql` tool: the standard GraphQL response envelope.
/// Execution errors travel inside it rather than as a tool error, so only a
/// hard failure (capability denial, schema not configured, timeout) takes the
/// [`tool_error_schema`] branch.
fn graphql_schema() -> JsonObject {
	any_of(
		"A GraphQL response envelope, or the reason the request could not be executed at all.",
		&[
			json!({
				"type": "object",
				"description": "The standard GraphQL response envelope. Execution errors are reported in `errors` rather than as a tool error.",
				"required": ["data"],
				"properties": {
					"data": {
						"description": "The data returned by the operation, or null if execution failed before producing data.",
					},
					"errors": {
						"type": "array",
						"description": "Errors raised while validating or executing the operation. Omitted when there are none.",
						"items": {
							"type": "object",
							"additionalProperties": true,
						},
					},
					"extensions": {
						"type": "object",
						"description": "Implementation-defined extension data. Omitted when there is none.",
						"additionalProperties": true,
					},
				},
			}),
			tool_error_schema(),
		],
	)
}

fn to_object(value: Value) -> JsonObject {
	match value {
		Value::Object(map) => map,
		Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
			Map::new()
		}
	}
}

/// Attach the appropriate `output_schema` to every registered tool in the
/// router. Called once during [`crate::service::McpService::new_with_config`],
/// on the router the service stores and every handler reads.
pub(crate) fn attach<S>(router: &mut ToolRouter<S>) {
	for (name, route) in router.map.iter_mut() {
		let schema = match name.as_ref() {
			// `gql` runs a different dialect through the same formatter as
			// `query`, so it returns the same shapes.
			"query" | "gql" => multi_statement_schema(),
			"graphql" => graphql_schema(),
			"use" => use_schema(),
			"list" => list_schema(),
			// The CRUD family collapses to a single statement but keeps the
			// batch shape reachable; `run` and `info` never leave one
			// statement's worth of output.
			"select" | "create" | "insert" | "upsert" | "update" | "delete" | "relate" => {
				multi_statement_schema()
			}
			"run" | "info" => single_statement_schema(),
			_ => continue,
		};
		route.attr.output_schema = Some(Arc::new(schema));
	}
}
