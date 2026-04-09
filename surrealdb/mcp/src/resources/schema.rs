//! Schema and server info resources.

use crate::session::McpSession;
use crate::tools::validate_identifier;

pub fn get_server_info() -> String {
	serde_json::json!({
		"name": "SurrealDB",
		"version": env!("CARGO_PKG_VERSION"),
		"protocol": "MCP 2025-11-25",
	})
	.to_string()
}

pub async fn get_database_schema(session: &McpSession) -> String {
	match session.execute("INFO FOR DB", None).await {
		Ok(results) => {
			let values: Vec<serde_json::Value> = results
				.into_iter()
				.filter_map(|r| r.result.ok().map(|v| v.into_json_value()))
				.collect();
			serde_json::to_string_pretty(&values).unwrap_or_else(|e| {
				tracing::warn!(target: "surrealdb::mcp", error = %e, "Failed to serialize schema");
				"[]".to_string()
			})
		}
		Err(e) => {
			tracing::warn!(target: "surrealdb::mcp", error = %e, "Failed to fetch database schema");
			"Unable to fetch database schema".to_string()
		}
	}
}

pub async fn get_table_schema(session: &McpSession, table: &str) -> String {
	if validate_identifier(table).is_err() {
		return "Invalid table name".to_string();
	}
	let query = format!("INFO FOR TABLE `{table}`");
	match session.execute(&query, None).await {
		Ok(results) => {
			let values: Vec<serde_json::Value> = results
				.into_iter()
				.filter_map(|r| r.result.ok().map(|v| v.into_json_value()))
				.collect();
			serde_json::to_string_pretty(&values).unwrap_or_else(|e| {
				tracing::warn!(target: "surrealdb::mcp", error = %e, "Failed to serialize table schema");
				"[]".to_string()
			})
		}
		Err(e) => {
			tracing::warn!(target: "surrealdb::mcp", error = %e, "Failed to fetch table schema");
			"Unable to fetch table schema".to_string()
		}
	}
}
