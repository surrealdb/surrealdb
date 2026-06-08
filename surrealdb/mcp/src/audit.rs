//! Canonical audit log for MCP tool invocations.
//!
//! Emits one structured `tracing::info!` event per tool call against the
//! `surrealdb::mcp::audit` target. Every invocation logs the same set of
//! fields so the line is trivially forwardable to a SIEM / log pipeline:
//!
//! - `tool` -- the MCP tool name as advertised in `tools/list`.
//! - `subject` -- the bound subject's level + identity (e.g. `/ns:foo/db:bar/::alice`), or
//!   `anonymous` when no auth is attached.
//! - `namespace` / `database` -- the session's current `use` context at the time of the call.
//!   `null` when the session has not been scoped yet.
//! - `outcome` -- one of `ok`, `tool_error`, `protocol_error`. We distinguish in-band tool errors
//!   (`is_error = true` results) from JSON-RPC protocol failures so the audit pipeline can spot
//!   client-driven problems separately from server-side faults.
//! - `kind` -- best-effort error kind extracted from the structured payload (e.g. `Validation`,
//!   `NotFound`). Empty on success.
//! - `time_ms` -- wall-clock duration of the dispatched handler.
//!
//! No query text, parameter values, or row payloads are ever logged.

use std::time::Duration;

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, ErrorCode};
use serde_json::Value as JsonValue;

/// Outcome classification used in the canonical audit log.
///
/// Crate-internal only; consumers outside the crate should map through
/// [`crate::metrics::McpToolOutcome`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum Outcome {
	Ok,
	ToolError,
	ProtocolError,
}

impl Outcome {
	fn as_str(self) -> &'static str {
		match self {
			Outcome::Ok => "ok",
			Outcome::ToolError => "tool_error",
			Outcome::ProtocolError => "protocol_error",
		}
	}
}

/// Classify a tool result into an [`Outcome`] + best-effort error `kind`.
///
/// `kind` is read from the structured content (`{"kind": "..."}` for
/// single-statement tools, or any per-statement kind for the multi-statement
/// `query` tool). A partial-success result from `query` (one statement
/// errored, another succeeded) keeps `is_error = false` on the wire so the
/// LLM sees the surviving rows; we still surface the first error kind in
/// the audit log so operators can correlate problems.
///
/// Returns `(Outcome::Ok, "")` only for fully successful calls.
pub(crate) fn classify(result: &Result<CallToolResult, McpError>) -> (Outcome, String) {
	match result {
		Ok(call) => {
			let is_error = call.is_error.unwrap_or(false);
			if is_error {
				return (Outcome::ToolError, extract_kind(call.structured_content.as_ref()));
			}
			// Partial-success path: outer `is_error` is false but the
			// structured payload reports `has_errors`. Surface the kind
			// without flipping the outcome so the audit pipeline can
			// distinguish total failure from partial.
			if let Some(structured) = call.structured_content.as_ref()
				&& structured.get("has_errors").and_then(|v| v.as_bool()).unwrap_or(false)
			{
				return (Outcome::Ok, extract_kind(Some(structured)));
			}
			(Outcome::Ok, String::new())
		}
		Err(err) => (Outcome::ProtocolError, error_code_label(err.code)),
	}
}

/// Map a JSON-RPC error code to a stable human-readable label for the
/// audit `kind` field. The standard `Debug` format on [`ErrorCode`] is
/// `"ErrorCode(-32602)"`, which is unhelpful in a log line; this maps
/// the well-known codes to their conventional names and falls back to
/// the numeric code for anything outside the spec.
fn error_code_label(code: ErrorCode) -> String {
	match code {
		ErrorCode::PARSE_ERROR => "PARSE_ERROR".into(),
		ErrorCode::INVALID_REQUEST => "INVALID_REQUEST".into(),
		ErrorCode::METHOD_NOT_FOUND => "METHOD_NOT_FOUND".into(),
		ErrorCode::INVALID_PARAMS => "INVALID_PARAMS".into(),
		ErrorCode::INTERNAL_ERROR => "INTERNAL_ERROR".into(),
		ErrorCode::URL_ELICITATION_REQUIRED => "URL_ELICITATION_REQUIRED".into(),
		other => format!("CODE_{}", other.0),
	}
}

fn extract_kind(structured: Option<&JsonValue>) -> String {
	let Some(value) = structured else {
		return String::new();
	};
	if let Some(kind) = value.get("kind").and_then(|v| v.as_str()) {
		return kind.to_string();
	}
	// Multi-statement payload: {"results":[{"kind":"..."}], "has_errors": true}
	if let Some(results) = value.get("results").and_then(|v| v.as_array()) {
		for entry in results {
			if let Some(kind) = entry.get("kind").and_then(|v| v.as_str()) {
				return kind.to_string();
			}
		}
	}
	String::new()
}

/// Emit the canonical audit record for a completed tool invocation.
pub(crate) fn record(
	tool: &str,
	subject: &str,
	namespace: Option<&str>,
	database: Option<&str>,
	outcome: Outcome,
	kind: &str,
	elapsed: Duration,
) {
	let time_ms = elapsed.as_secs_f64() * 1000.0;
	tracing::info!(
		target: "surrealdb::mcp::audit",
		tool,
		subject,
		namespace,
		database,
		outcome = outcome.as_str(),
		kind,
		time_ms,
		"mcp tool invocation"
	);
}

#[cfg(test)]
mod tests {
	use rmcp::model::Content;
	use serde_json::json;

	use super::*;

	#[test]
	fn classifies_success() {
		let mut call = CallToolResult::success(vec![Content::text("ok")]);
		call.structured_content = Some(json!({"status": "ok"}));
		let result: Result<CallToolResult, McpError> = Ok(call);
		let (outcome, kind) = classify(&result);
		assert!(matches!(outcome, Outcome::Ok));
		assert!(kind.is_empty());
	}

	#[test]
	fn classifies_tool_error_and_extracts_kind() {
		let mut call = CallToolResult::error(vec![Content::text("Error (Validation): bad")]);
		call.structured_content = Some(json!({"error": "bad", "kind": "Validation"}));
		let result: Result<CallToolResult, McpError> = Ok(call);
		let (outcome, kind) = classify(&result);
		assert!(matches!(outcome, Outcome::ToolError));
		assert_eq!(kind, "Validation");
	}

	#[test]
	fn classifies_multi_statement_first_error_kind() {
		let mut call = CallToolResult::success(vec![Content::text("...")]);
		call.structured_content = Some(json!({
			"results": [
				{"index": 0, "status": "ok"},
				{"index": 1, "status": "error", "kind": "Query", "error": "boom"}
			],
			"has_errors": true,
		}));
		let result: Result<CallToolResult, McpError> = Ok(call);
		let (_outcome, kind) = classify(&result);
		assert_eq!(kind, "Query");
	}

	#[test]
	fn classifies_protocol_error() {
		let result: Result<CallToolResult, McpError> = Err(McpError {
			code: ErrorCode::INVALID_PARAMS,
			message: "bad".into(),
			data: None,
		});
		let (outcome, kind) = classify(&result);
		assert!(matches!(outcome, Outcome::ProtocolError));
		assert_eq!(kind, "INVALID_PARAMS");
	}
}
