//! Context-switching tools: use_namespace, use_database.

use rmcp::ErrorData;
use rmcp::model::{CallToolResult, Content};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UseNamespaceParams {
	/// The namespace to switch to.
	pub namespace: String,
}

pub async fn use_namespace(
	session: &McpSession,
	params: UseNamespaceParams,
) -> Result<CallToolResult, ErrorData> {
	session.use_ns(&params.namespace).await?;
	Ok(CallToolResult::success(vec![Content::text(format!(
		"Switched to namespace '{}'",
		params.namespace
	))]))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UseDatabaseParams {
	/// The database to switch to.
	pub database: String,
}

pub async fn use_database(
	session: &McpSession,
	params: UseDatabaseParams,
) -> Result<CallToolResult, ErrorData> {
	session.use_db(&params.database).await?;
	Ok(CallToolResult::success(vec![Content::text(format!(
		"Switched to database '{}'",
		params.database
	))]))
}
