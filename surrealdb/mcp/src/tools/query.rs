//! Raw SurrealQL query execution.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;

use super::{json_to_variables, multi_statement_result};
use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryParams {
	/// The SurrealQL query to execute. Use $param syntax for parameter placeholders.
	pub query: String,
	/// Optional JSON object of parameter bindings (e.g. {"name": "John", "age": 30}).
	/// Values are bound with their native types -- numbers stay numbers, objects stay objects.
	/// Use `{"$ql": "<surrealql expr>"}` to embed typed SurrealDB values
	/// such as decimals, datetimes, durations, record ids, or uuids
	/// (e.g. `{"price": {"$ql": "9.99dec"}, "user": {"$ql": "person:alice"}}`).
	pub parameters: Option<serde_json::Value>,
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
}

pub async fn execute(
	session: &McpSession,
	params: QueryParams,
) -> Result<CallToolResult, ErrorData> {
	let core = session.datastore().parser_config();
	let vars = match params.parameters {
		Some(ref json) => Some(json_to_variables(json, session.config(), core.as_ref())?),
		None => None,
	};
	let results = session.execute(&params.query, vars).await?;
	Ok(multi_statement_result(results, session.config().max_result_bytes))
}
