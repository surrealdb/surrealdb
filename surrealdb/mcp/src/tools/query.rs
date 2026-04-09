//! Raw SurrealQL query execution and EXPLAIN tools.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;

use super::{json_to_variables, result_to_text};
use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryParams {
	/// The SurrealQL query to execute. Use $param syntax for parameter placeholders.
	pub query: String,
	/// Optional JSON object of parameter bindings (e.g. {"name": "John", "age": 30}).
	/// Values are bound with their native types -- numbers stay numbers, objects stay objects.
	pub parameters: Option<serde_json::Value>,
}

pub async fn execute(
	session: &McpSession,
	params: QueryParams,
) -> Result<CallToolResult, ErrorData> {
	let vars = match params.parameters {
		Some(ref json) => Some(json_to_variables(json)?),
		None => None,
	};
	let results = session.execute(&params.query, vars).await?;
	Ok(result_to_text(results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExplainParams {
	/// The SurrealQL query to explain.
	pub query: String,
	/// Whether to run EXPLAIN FULL (includes actual execution statistics).
	#[serde(default)]
	pub full: bool,
}

/// Run EXPLAIN on a query to show the execution plan.
pub async fn explain(
	session: &McpSession,
	params: ExplainParams,
) -> Result<CallToolResult, ErrorData> {
	let suffix = if params.full {
		" FULL"
	} else {
		""
	};
	let query = format!("{} EXPLAIN{suffix}", params.query);
	let results = session.execute(&query, None).await?;
	Ok(result_to_text(results))
}
