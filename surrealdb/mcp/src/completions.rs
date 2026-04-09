//! Auto-completion support for MCP tool arguments.

use rmcp::model::{CompleteRequestParams, CompleteResult, CompletionInfo};
use surrealdb_core::dbs::QueryResult;

use crate::session::McpSession;

/// Handle a completion request by providing suggestions based on context.
pub async fn handle_completion(
	session: &McpSession,
	params: &CompleteRequestParams,
) -> CompleteResult {
	let values = match params.argument.name.as_str() {
		"table" | "target" | "targets" => list_tables(session).await,
		"namespace" => list_namespaces(session).await,
		"database" => list_databases(session).await,
		_ => Vec::new(),
	};

	let info = CompletionInfo::with_all_values(values)
		.unwrap_or_else(|_| CompletionInfo::new(Vec::new()).expect("empty vec always valid"));
	CompleteResult::new(info)
}

async fn list_tables(session: &McpSession) -> Vec<String> {
	let Ok(results) = session.execute("INFO FOR DB STRUCTURE", None).await else {
		return Vec::new();
	};
	extract_keys(results)
}

async fn list_namespaces(session: &McpSession) -> Vec<String> {
	let Ok(results) = session.execute("INFO FOR ROOT STRUCTURE", None).await else {
		return Vec::new();
	};
	extract_keys(results)
}

async fn list_databases(session: &McpSession) -> Vec<String> {
	let Ok(results) = session.execute("INFO FOR NS STRUCTURE", None).await else {
		return Vec::new();
	};
	extract_keys(results)
}

/// Extract identifier keys from an INFO STRUCTURE result by converting to JSON
/// and reading object keys from each section.
fn extract_keys(results: Vec<QueryResult>) -> Vec<String> {
	let Some(result) = results.into_iter().next() else {
		return Vec::new();
	};
	let Ok(value) = result.result else {
		return Vec::new();
	};
	let json = value.into_json_value();
	if let serde_json::Value::Object(top) = json {
		top.values()
			.filter_map(|v| v.as_object())
			.flat_map(|obj| obj.keys().cloned())
			.take(100)
			.collect()
	} else {
		Vec::new()
	}
}
