//! Adapter that lets the [`super::MetricsObserver`] satisfy the
//! [`surrealdb_mcp::metrics::McpMetricsRecorder`] trait.
//!
//! The MCP crate is transport-agnostic and has no compile-time dependency on
//! the OpenTelemetry SDK. The server crate implements the recorder trait
//! using its own [`MetricsObserver`] so the `surrealdb.mcp.tool.*` family
//! shares the unified meter provider with every other server-side metric.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_mcp::metrics::{McpMetricsRecorder, McpToolOutcome};

use super::MetricsObserver;

/// Recorder bridge between the MCP crate and the server's
/// [`MetricsObserver`].
///
/// The recorder maps the bounded [`McpToolOutcome`] returned by the MCP
/// audit classifier into the server's `Outcome` / `error_class` pair, then
/// forwards to the metrics observer's `record_mcp_tool` /
/// `adjust_mcp_session_active` methods.
pub struct McpRecorderAdapter {
	inner: Arc<MetricsObserver>,
}

impl McpRecorderAdapter {
	/// Build an adapter from a shared [`MetricsObserver`].
	pub fn new(inner: Arc<MetricsObserver>) -> Self {
		Self {
			inner,
		}
	}
}

impl McpMetricsRecorder for McpRecorderAdapter {
	fn record_tool_invocation(
		&self,
		tool: &'static str,
		transport: &'static str,
		outcome: McpToolOutcome,
		duration: Duration,
	) {
		// Map the MCP-side outcome onto the server-wide `Outcome` /
		// `error_class` pair. `McpToolOutcome::error_class` returns the
		// already-bounded canonical strings used by the rest of the
		// observability surface.
		let metric_outcome = match outcome {
			McpToolOutcome::Success => surrealdb_core::observe::Outcome::Success,
			McpToolOutcome::ToolError | McpToolOutcome::ProtocolError => {
				surrealdb_core::observe::Outcome::Error
			}
		};
		self.inner.record_mcp_tool(
			tool,
			transport,
			metric_outcome,
			outcome.error_class(),
			duration,
		);
	}

	fn adjust_session_active(&self, delta: i64, transport: &'static str) {
		self.inner.adjust_mcp_session_active(delta, transport);
	}
}
