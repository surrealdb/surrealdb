//! Error types for the MCP server.
//!
//! This module distinguishes two error channels:
//!
//! - **Tool-level errors** — failed SurrealQL statements, permission denials, parse errors,
//!   constraint violations. These surface in-band via [`rmcp::model::CallToolResult`] with
//!   `is_error = true` so the LLM can see and self-correct against the full SurrealDB error
//!   message. They never reach this module.
//! - **Protocol-level errors** — the `McpService` was never initialised, the caller passed an
//!   unrecognised tool name, the request body didn't match the tool's input schema, or something
//!   internal to the server broke. These become JSON-RPC errors via [`ErrorData`] and are what this
//!   module handles.
//!
//! This mirrors the MCP spec: JSON-RPC failure should be reserved for the
//! protocol layer, not for legitimate tool outcomes.

use rmcp::ErrorData;

/// Internal MCP error kinds that can escape the tool boundary. Only covers
/// failures that should become JSON-RPC errors; see the module doc above.
#[derive(Debug, thiserror::Error)]
pub enum Error {
	#[error("Not connected to a database")]
	NotConnected,

	#[error("No namespace selected")]
	NoNamespace,

	#[error("No database selected")]
	NoDatabase,

	#[error("Invalid parameters: {0}")]
	InvalidParams(String),

	#[error("Internal error")]
	Internal(#[source] anyhow::Error),
}

impl From<Error> for ErrorData {
	fn from(err: Error) -> Self {
		match err {
			Error::NotConnected => ErrorData::invalid_request("Not connected to a database", None),
			Error::NoNamespace => ErrorData::invalid_request("No namespace selected", None),
			Error::NoDatabase => ErrorData::invalid_request("No database selected", None),
			Error::InvalidParams(msg) => ErrorData::invalid_params(msg, None),
			Error::Internal(ref e) => {
				tracing::error!(error = ?e, "MCP internal error");
				ErrorData::internal_error("An internal error occurred", None)
			}
		}
	}
}

/// Convenience constructor for invalid params errors.
pub fn invalid_params(msg: impl Into<String>) -> ErrorData {
	ErrorData::invalid_params(msg.into(), None)
}
