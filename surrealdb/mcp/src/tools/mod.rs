//! MCP tool implementations for SurrealDB operations.
//!
//! All tools execute SurrealQL through `Datastore::execute()` with proper
//! Session context. Data values are bound via Variables; identifiers are
//! validated to prevent statement injection.

pub mod connection;
pub mod crud;
pub mod query;
pub mod schema;

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use surrealdb_core::dbs::QueryResult;
use surrealdb_types::{SurrealValue, Value, Variables};

/// Format query results as a text CallToolResult.
///
/// SECURITY: Statement-level errors are logged server-side but only a
/// generic message is returned to the MCP client to avoid leaking
/// internal details (paths, engine messages, schema info).
pub(crate) fn result_to_text(results: Vec<QueryResult>) -> CallToolResult {
	let mut output = Vec::new();
	for (i, response) in results.into_iter().enumerate() {
		match response.result {
			Ok(value) => {
				let json = value.into_json_value();
				let text = serde_json::to_string_pretty(&json).unwrap_or_else(|e| {
					tracing::warn!(target: "surrealdb::mcp", error = %e, "Failed to serialize query result");
					"null".to_string()
				});
				output.push(format!("Statement {i}: {text}"));
			}
			Err(err) => {
				tracing::warn!(
					target: "surrealdb::mcp",
					statement = i,
					error = %err,
					"Query statement failed"
				);
				output.push(format!("Statement {i}: Error: Operation failed"));
			}
		}
	}
	CallToolResult::success(vec![rmcp::model::Content::text(output.join("\n\n"))])
}

/// Validate that a string is safe to use as a SurrealQL identifier.
/// Rejects strings containing statement terminators or other injection vectors.
pub fn validate_identifier(s: &str) -> Result<&str, ErrorData> {
	if s.is_empty() {
		return Err(crate::error::invalid_params("Identifier cannot be empty"));
	}
	if s.contains(';') || s.contains('\n') || s.contains('\r') || s.contains('\0') {
		return Err(crate::error::invalid_params("Identifier contains invalid characters"));
	}
	Ok(s)
}

/// Convert a serde_json::Value into a surrealdb_types::Value, preserving types.
pub(crate) fn json_to_surreal_value(json: &serde_json::Value) -> Value {
	match json {
		serde_json::Value::Null => Value::Null,
		serde_json::Value::Bool(b) => (*b).into_value(),
		serde_json::Value::Number(n) => {
			if let Some(i) = n.as_i64() {
				i.into_value()
			} else if let Some(f) = n.as_f64() {
				f.into_value()
			} else {
				Value::None
			}
		}
		serde_json::Value::String(s) => s.as_str().into_value(),
		serde_json::Value::Array(arr) => {
			let vals: Vec<Value> = arr.iter().map(json_to_surreal_value).collect();
			vals.into_value()
		}
		serde_json::Value::Object(map) => {
			let mut obj = surrealdb_types::Object::default();
			for (k, v) in map {
				obj.insert(k, json_to_surreal_value(v));
			}
			obj.into_value()
		}
	}
}

/// Convert a JSON object into typed Variables for query binding.
/// Returns an error if the value is not a JSON object.
pub fn json_to_variables(json: &serde_json::Value) -> Result<Variables, ErrorData> {
	match json {
		serde_json::Value::Object(map) => {
			let mut vars = Variables::new();
			for (k, v) in map {
				vars.insert(k, json_to_surreal_value(v));
			}
			Ok(vars)
		}
		_ => Err(crate::error::invalid_params("Parameters must be a JSON object")),
	}
}
