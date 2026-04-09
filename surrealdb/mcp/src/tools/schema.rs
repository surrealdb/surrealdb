//! Schema introspection tools.

use rmcp::ErrorData;
use rmcp::model::{CallToolResult, Content};
use schemars::JsonSchema;
use serde::Deserialize;

use super::{result_to_text, validate_identifier};
use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InfoParams {
	/// Scope: "root", "ns", "db", or a table name. Defaults to most specific context.
	pub target: Option<String>,
}

pub async fn info(session: &McpSession, params: InfoParams) -> Result<CallToolResult, ErrorData> {
	let query = match params.target.as_deref() {
		Some("root") => "INFO FOR ROOT".to_string(),
		Some("ns") | Some("namespace") => "INFO FOR NS".to_string(),
		Some("db") | Some("database") => "INFO FOR DB".to_string(),
		Some(table) => {
			validate_identifier(table)?;
			format!("INFO FOR TABLE `{table}`")
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
	let results = session.execute(&query, None).await?;
	Ok(result_to_text(results))
}

pub async fn list_namespaces(session: &McpSession) -> Result<CallToolResult, ErrorData> {
	let results = session.execute("INFO FOR ROOT STRUCTURE", None).await?;
	Ok(result_to_text(results))
}

pub async fn list_databases(session: &McpSession) -> Result<CallToolResult, ErrorData> {
	let results = session.execute("INFO FOR NS STRUCTURE", None).await?;
	Ok(result_to_text(results))
}

pub async fn list_tables(session: &McpSession) -> Result<CallToolResult, ErrorData> {
	let results = session.execute("INFO FOR DB STRUCTURE", None).await?;
	Ok(result_to_text(results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DescribeTableParams {
	/// The name of the table to describe.
	pub table: String,
}

pub async fn describe_table(
	session: &McpSession,
	params: DescribeTableParams,
) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&params.table)?;
	let query = format!("INFO FOR TABLE `{}`", params.table);
	let results = session.execute(&query, None).await?;
	Ok(result_to_text(results))
}

/// Return SurrealDB version information.
pub fn version() -> CallToolResult {
	CallToolResult::success(vec![Content::text(format!("SurrealDB {}", env!("CARGO_PKG_VERSION")))])
}
