//! Shared helpers for formatting tool outputs.
//!
//! MCP tool responses emit both structured and unstructured content:
//!
//! - `CallToolResult.structured_content` carries the typed JSON the LLM can deserialize verbatim.
//!   This is the 2025-06-18 spec "structured content" affordance.
//! - `CallToolResult.content` carries a human-readable rendering of the same data. Tools that
//!   predate structured content still work off this.
//!
//! Statement-level failures surface via `is_error = true` with a structured
//! error payload; protocol-level / bug-level failures keep going through
//! [`crate::error::Error`] and become JSON-RPC errors. Mixed success/failure
//! in a multi-statement query keeps `is_error = false` so the caller can see
//! which statements succeeded.

use rmcp::model::{CallToolResult, Content};
use serde_json::{Value as JsonValue, json};
use surrealdb_core::dbs::QueryResult;
use surrealdb_types::Error as TypesError;

/// If the serialised form of `value` exceeds the supplied cap, replace it
/// with a marker object `{truncated: true, original_bytes: N, message: ...}`
/// and return the marker along with `true`. Otherwise return the value
/// unchanged and `false`. `max_bytes = None` disables the cap.
///
/// `pub(crate)` so resource readers can share the same cap policy (and the
/// same wire shape for the truncation marker) instead of returning
/// uncapped INFO blobs.
pub(crate) fn cap_value(value: JsonValue, max_bytes: Option<usize>) -> (JsonValue, bool) {
	let Some(max) = max_bytes else {
		return (value, false);
	};
	// Count the compact serialization size: it is always <= the pretty size
	// we actually send, so this is a conservative lower bound.
	let size = serde_json::to_string(&value).map(|s| s.len()).unwrap_or(0);
	if size <= max {
		return (value, false);
	}
	tracing::warn!(
		target: "surrealdb::mcp",
		bytes = size,
		limit = max,
		"truncating large MCP tool result"
	);
	(truncation_marker(size, max), true)
}

/// Build the standard truncation marker. Shared by [`cap_value`] and the
/// resource-side cap so MCP clients see exactly the same wire shape
/// regardless of whether truncation happened in a tool result or a
/// resource body.
///
/// The self-identifying key is `$truncated` (not `truncated`) so it does
/// not collide with the outer-level `truncated: bool` flag that
/// [`single_statement_result`] / [`multi_statement_result`] stamp on the
/// envelope. The `$`-prefixed key is part of the MCP wire vocabulary
/// reserved for server-emitted metadata, alongside `$ql` on the input
/// side.
pub(crate) fn truncation_marker(original_bytes: usize, limit_bytes: usize) -> JsonValue {
	json!({
		"$truncated": true,
		"original_bytes": original_bytes,
		"limit_bytes": limit_bytes,
		"message": format!(
			"Result omitted: serialized size {original_bytes} bytes exceeds the {limit_bytes}-byte cap. Refine the query (add LIMIT / projections) or raise SURREAL_MCP_MAX_RESULT_BYTES."
		),
	})
}

/// Classify a [`TypesError`] into a short, stable identifier suitable for
/// embedding in a tool's structured error payload. The LLM (and downstream
/// clients) can use this to branch on error kind without parsing English.
fn error_kind(error: &TypesError) -> &'static str {
	error.kind_str()
}

/// Turn the surrealdb error message + kind into a structured JSON object. We
/// surface the full SurrealDB error text because these messages are already
/// user-facing (identical to what `/sql` and `/rpc` return) and crucially
/// help the LLM correct itself.
fn error_payload(error: &TypesError) -> JsonValue {
	json!({
		"error": error.message(),
		"kind": error_kind(error),
	})
}

/// Format a single [`QueryResult`] as a [`CallToolResult`].
///
/// Used by tools that always emit a single SurrealQL statement (select,
/// create, insert, upsert, update, delete, relate, run, info, list). The
/// structured content is the value directly; the text block is pretty JSON.
/// On failure, `is_error` is set and both blocks carry the sanitized-but-
/// informative SurrealDB error text. `max_bytes` is the per-result
/// truncation cap (typically [`crate::cnf::McpConfig::max_result_bytes`]);
/// pass `None` to disable.
pub(crate) fn single_statement_result(
	result: QueryResult,
	max_bytes: Option<usize>,
) -> CallToolResult {
	let time_ms = duration_to_millis(result.time);
	match result.result {
		Ok(value) => {
			let raw = value.into_json_value();
			let (json, truncated) = cap_value(raw, max_bytes);
			let text = pretty_json(&json);
			let structured = json!({
				"status": "ok",
				"value": json,
				"truncated": truncated,
				"time_ms": time_ms,
			});
			let mut r = CallToolResult::success(vec![Content::text(text)]);
			r.structured_content = Some(structured);
			r
		}
		Err(err) => {
			tracing::warn!(
				target: "surrealdb::mcp",
				kind = err.kind_str(),
				error = %err.message(),
				"tool statement failed"
			);
			let structured = json!({
				"status": "error",
				"error": err.message(),
				"kind": err.kind_str(),
				"time_ms": time_ms,
			});
			let mut r = CallToolResult::error(vec![Content::text(format!(
				"Error ({}): {}",
				err.kind_str(),
				err.message()
			))]);
			r.structured_content = Some(structured);
			r
		}
	}
}

/// Format a list of [`QueryResult`]s as a [`CallToolResult`].
///
/// Used by the raw `query` tool, which can return multiple statements. The
/// structured content is:
///
/// ```json
/// {
///   "results": [
///     {"index": 0, "status": "ok",    "value": ..., "time_ms": 1.23},
///     {"index": 1, "status": "error", "error": "...", "kind": "Validation", "time_ms": 0.04}
///   ],
///   "has_errors": true
/// }
/// ```
///
/// The text block is either a single pretty JSON (if there's exactly one
/// successful statement) or a labelled list when there's more than one /
/// any statement failed. `is_error` is only set when every statement failed —
/// partial success stays `false` so the LLM sees what went through.
/// `max_bytes` is forwarded to [`cap_value`] for each per-statement payload.
pub(crate) fn multi_statement_result(
	results: Vec<QueryResult>,
	max_bytes: Option<usize>,
) -> CallToolResult {
	if results.len() == 1 {
		return single_statement_result(results.into_iter().next().expect("len == 1"), max_bytes);
	}

	let mut entries = Vec::with_capacity(results.len());
	let mut text_blocks = Vec::with_capacity(results.len());
	let mut ok_count = 0usize;
	let mut err_count = 0usize;

	for (i, response) in results.into_iter().enumerate() {
		let time_ms = duration_to_millis(response.time);
		match response.result {
			Ok(value) => {
				ok_count += 1;
				let raw = value.into_json_value();
				let (json, truncated) = cap_value(raw, max_bytes);
				let pretty = pretty_json(&json);
				entries.push(json!({
					"index": i,
					"status": "ok",
					"value": json,
					"truncated": truncated,
					"time_ms": time_ms,
				}));
				text_blocks.push(format!("Statement {i} (ok):\n{pretty}"));
			}
			Err(err) => {
				err_count += 1;
				tracing::warn!(
					target: "surrealdb::mcp",
					statement = i,
					kind = err.kind_str(),
					error = %err.message(),
					"query statement failed"
				);
				entries.push(json!({
					"index": i,
					"status": "error",
					"error": err.message(),
					"kind": err.kind_str(),
					"time_ms": time_ms,
				}));
				text_blocks.push(format!(
					"Statement {i} (error, {}): {}",
					err.kind_str(),
					err.message()
				));
			}
		}
	}

	let has_errors = err_count > 0;
	let is_error = ok_count == 0 && err_count > 0;
	let structured = json!({
		"results": entries,
		"has_errors": has_errors,
	});
	let text = text_blocks.join("\n\n");
	let mut r = if is_error {
		CallToolResult::error(vec![Content::text(text)])
	} else {
		CallToolResult::success(vec![Content::text(text)])
	};
	r.structured_content = Some(structured);
	r
}

/// Build a successful [`CallToolResult`] from a structured JSON payload.
///
/// Used by tools that don't map one-to-one to a SurrealQL result (e.g.
/// `use`, `list` after subtree extraction). The text block is a pretty
/// rendering of the payload.
pub(crate) fn structured_success(structured: JsonValue) -> CallToolResult {
	let text = pretty_json(&structured);
	let mut r = CallToolResult::success(vec![Content::text(text)]);
	r.structured_content = Some(structured);
	r
}

/// Build an error [`CallToolResult`] from a surrealdb [`TypesError`]. Used
/// when a tool detected a failed statement and wants to surface it as an
/// in-band error rather than a protocol-level JSON-RPC error.
pub(crate) fn tool_error_from_surreal(err: &TypesError) -> CallToolResult {
	let payload = error_payload(err);
	let mut r = CallToolResult::error(vec![Content::text(format!(
		"Error ({}): {}",
		err.kind_str(),
		err.message()
	))]);
	r.structured_content = Some(payload);
	r
}

/// Build an in-band tool error from an arbitrary message + kind. Used for
/// tool-specific validation failures (e.g. "kind `users` requires scope")
/// that we want to surface to the model via `is_error = true` rather than
/// as a JSON-RPC `invalid_params`.
pub(crate) fn tool_error(kind: &'static str, message: impl Into<String>) -> CallToolResult {
	let message = message.into();
	let structured = json!({
		"error": message,
		"kind": kind,
	});
	let mut r = CallToolResult::error(vec![Content::text(format!("Error ({kind}): {message}"))]);
	r.structured_content = Some(structured);
	r
}

fn duration_to_millis(duration: std::time::Duration) -> f64 {
	// Sub-millisecond resolution matters for micro-benchmarks inside the
	// database, so emit a float rather than an integer.
	duration.as_secs_f64() * 1000.0
}

/// Pretty-print a JSON value, falling back to a literal `null` on the
/// (vanishingly rare) serialization failure. Emits a warning so server
/// operators can investigate the underlying value.
pub(crate) fn pretty_json(value: &JsonValue) -> String {
	serde_json::to_string_pretty(value).unwrap_or_else(|e| {
		tracing::warn!(
			target: "surrealdb::mcp",
			error = %e,
			"failed to pretty-print JSON value"
		);
		"null".to_string()
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::cnf::McpConfig;

	/// Values below the configured cap must pass through `cap_value`
	/// untouched with `truncated = false`.
	#[test]
	fn cap_value_preserves_small_payloads() {
		let payload = json!({"hello": "world"});
		let (out, truncated) = cap_value(payload.clone(), McpConfig::default().max_result_bytes);
		assert!(!truncated);
		assert_eq!(out, payload);
	}

	/// Values whose serialised size exceeds the cap must be replaced with
	/// a structured truncation marker describing the original size and
	/// the limit that was exceeded, so MCP clients can surface an actionable
	/// message to the user / model.
	#[test]
	fn cap_value_truncates_large_payloads() {
		let limit = McpConfig::default().max_result_bytes.expect("default cap must be active");
		// Build a string whose JSON encoding is guaranteed to exceed the
		// cap, without going through the datastore (whose per-call
		// allocation limits are independent of the MCP response cap).
		let oversized = "x".repeat(limit + 1024);
		let (out, truncated) = cap_value(JsonValue::String(oversized), Some(limit));
		assert!(truncated, "cap_value should flag oversized payloads");
		let marker = out.as_object().expect("truncation marker is an object");
		assert_eq!(marker.get("$truncated").and_then(|v| v.as_bool()), Some(true));
		assert!(marker.get("original_bytes").and_then(|v| v.as_u64()).unwrap_or(0) > limit as u64);
		assert_eq!(marker.get("limit_bytes").and_then(|v| v.as_u64()), Some(limit as u64));
		// The marker must not carry a bare `truncated` key that would
		// collide with the envelope-level flag.
		assert!(marker.get("truncated").is_none());
	}

	/// The single-statement formatter must propagate the truncation flag
	/// to the top-level structured content so rmcp clients can detect it
	/// without digging into the nested value.
	#[test]
	fn single_statement_result_surfaces_truncation() {
		use surrealdb_core::dbs::QueryResultBuilder;
		let limit = McpConfig::default().max_result_bytes.expect("default cap must be active");
		let oversized = "x".repeat(limit + 1024);
		let result = QueryResultBuilder::started_now()
			.finish_with_result(Ok(surrealdb_types::Value::String(oversized)));
		let call = single_statement_result(result, Some(limit));
		let structured = call.structured_content.expect("structured content");
		assert_eq!(structured.get("truncated").and_then(|v| v.as_bool()), Some(true));
		assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("ok"));
		let value = structured.get("value").and_then(|v| v.as_object()).expect("marker object");
		assert_eq!(value.get("$truncated").and_then(|v| v.as_bool()), Some(true));
		assert!(value.get("truncated").is_none(), "marker must not shadow envelope flag");
	}
}
