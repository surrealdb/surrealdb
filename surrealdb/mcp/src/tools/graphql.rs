//! GraphQL request execution.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;

use super::{structured_success, tool_error};
use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GraphqlParams {
	/// The GraphQL document to execute (a query or mutation operation).
	/// Subscriptions, which require a streaming transport, are not supported
	/// through this tool.
	pub query: String,
	/// Optional JSON object of GraphQL variables (e.g. {"id": "person:tobie"}).
	pub variables: Option<serde_json::Value>,
	/// Optional operation name, used to select an operation when the document
	/// defines more than one named operation.
	pub operation: Option<String>,
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
}

pub async fn execute(
	session: &McpSession,
	params: GraphqlParams,
) -> Result<CallToolResult, ErrorData> {
	let variables = params.variables.unwrap_or(serde_json::Value::Null);
	// GraphQL execution errors travel inside the `{ data, errors }` envelope, so
	// a successful call still returns `structured_success`. Only hard failures
	// (capability denial, schema not configured, timeout) surface as a tool
	// error, mirroring how the `query` tool treats top-level failures in-band.
	match session.execute_graphql(&params.query, variables, params.operation).await {
		Ok(envelope) => Ok(structured_success(envelope)),
		Err(message) => Ok(tool_error("graphql", message)),
	}
}
