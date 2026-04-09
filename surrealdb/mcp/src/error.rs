//! Sanitized error types for the MCP server.
//!
//! All errors returned to MCP clients are sanitized to avoid leaking
//! internal paths, schema details, record contents, or storage engine
//! implementation details.

use rmcp::ErrorData;

/// Internal MCP error kinds.
#[derive(Debug, thiserror::Error)]
pub enum Error {
	#[error("Query execution failed")]
	QueryFailed(#[source] anyhow::Error),

	#[error("Not connected to a database")]
	NotConnected,

	#[error("No namespace selected")]
	NoNamespace,

	#[error("No database selected")]
	NoDatabase,

	#[error("Invalid parameters: {0}")]
	InvalidParams(String),

	#[error("Access denied")]
	AccessDenied,

	#[error("Session expired")]
	SessionExpired,

	#[error("Internal error")]
	Internal(#[source] anyhow::Error),
}

impl From<Error> for ErrorData {
	fn from(err: Error) -> Self {
		// SECURITY: Sanitize all errors. Internal details are logged server-side only.
		match err {
			Error::QueryFailed(ref e) => {
				tracing::error!(error = ?e, "MCP query execution failed");
				ErrorData::internal_error("Query execution failed", None)
			}
			Error::NotConnected => ErrorData::invalid_request("Not connected to a database", None),
			Error::NoNamespace => ErrorData::invalid_request("No namespace selected", None),
			Error::NoDatabase => ErrorData::invalid_request("No database selected", None),
			Error::InvalidParams(msg) => ErrorData::invalid_params(msg, None),
			Error::AccessDenied => {
				ErrorData::internal_error("Access denied for the requested operation", None)
			}
			Error::SessionExpired => ErrorData::internal_error("Session has expired", None),
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
