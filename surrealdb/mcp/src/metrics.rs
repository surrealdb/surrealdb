//! Optional metrics recording surface for MCP tool dispatch.
//!
//! The MCP crate is transport-agnostic and does not depend on the OpenTelemetry
//! SDK directly. Instead, embedders (the SurrealDB server, custom hosts, ...)
//! supply an [`McpMetricsRecorder`] that translates the bounded MCP-side
//! events into whatever metric pipeline they own.
//!
//! When no recorder is attached the dispatch path runs without overhead --
//! every `record_*` call is gated behind an `Option<Arc<dyn ...>>` short-circuit
//! at the dispatch site, so the cost of an absent recorder is one null check.

use std::time::Duration;

/// Outcome class for an MCP tool invocation, mirroring the labels recorded
/// in the canonical audit log so dashboards and SIEM rules agree on every
/// dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum McpToolOutcome {
	Success,
	/// Tool reported a caller-visible error (validation, missing record,
	/// SurrealQL error). Surfaces as the audit log's `tool_error`.
	ToolError,
	/// JSON-RPC framing or protocol-level rejection: malformed request,
	/// invalid params, unknown method, missing `initialize`, subject-binding
	/// mismatch, or an internal error from the dispatch path. Surfaces as
	/// the audit log's `protocol_error`.
	ProtocolError,
}

impl McpToolOutcome {
	/// Stable lower-case label suitable for use as a metric attribute value.
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Success => "success",
			Self::ToolError => "error",
			Self::ProtocolError => "error",
		}
	}

	/// Bounded error classification shared with the rest of the SurrealDB
	/// observability surface. `None` for the success outcome.
	///
	/// Both `ToolError` and `ProtocolError` fold into the bounded `client`
	/// class because every concrete trigger (parse error, invalid request,
	/// unknown method, invalid params, subject-binding mismatch) is a
	/// caller-visible logical error: the bounded `permission` class is
	/// reserved for "caller is authenticated but lacks permission for the
	/// resource" and would mislead SIEM rules keyed on
	/// `surrealdb_mcp_tool_invocation_total{error_class="permission"}`.
	/// Granular triage stays available via the audit log's `kind` field
	/// (`PARSE_ERROR`, `INVALID_PARAMS`, ...) and is intentionally not
	/// promoted to a metric attribute to keep cardinality bounded.
	pub const fn error_class(self) -> Option<&'static str> {
		match self {
			Self::Success => None,
			Self::ToolError => Some("client"),
			Self::ProtocolError => Some("client"),
		}
	}
}

/// Thin trait the MCP service uses to publish per-invocation metrics.
///
/// Implementations live in the embedding crate (e.g. the SurrealDB server's
/// [`MetricsObserver`](https://docs.rs/surrealdb-server)) so the MCP crate
/// itself stays free of any OpenTelemetry / Prometheus dependency.
pub trait McpMetricsRecorder: Send + Sync + 'static {
	/// Record one tool invocation completion. Called once per dispatch,
	/// regardless of whether the inner handler succeeded, was rejected
	/// for protocol reasons (e.g. a subject mismatch), or raised an
	/// internal error.
	///
	/// `tool` is one of the static identifier strings passed to
	/// [`super::service::McpService::dispatch_tool`] (`"query"`, `"select"`,
	/// `"create"`, ...). It is bounded by the static dispatch table so it
	/// is safe to use as a metric attribute value.
	fn record_tool_invocation(
		&self,
		tool: &'static str,
		transport: &'static str,
		outcome: McpToolOutcome,
		duration: Duration,
	);

	/// Bump or drop the active MCP session gauge. Connect / disconnect must
	/// pass the same `transport` so the gauge series stays balanced.
	///
	/// Optional: implementations that do not track session lifecycle (or
	/// do so via a different surface, e.g. the audit pipeline) may
	/// implement this as a no-op.
	fn adjust_session_active(&self, _delta: i64, _transport: &'static str) {}
}
