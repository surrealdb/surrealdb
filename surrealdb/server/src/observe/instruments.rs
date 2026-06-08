//! Single source of truth for the unified observability instrument schema.
//!
//! Every metric registered by the server, the enterprise composer, the
//! audit / slow-query pipelines, the SurrealDS cluster transport, and the
//! storage backend bridge funnels through one of the meter scopes defined
//! here. Instrument names are listed alongside so registration sites and
//! tests can compare against a single list rather than scattering string
//! literals.
//!
//! # Design
//!
//! - **Scopes are signal-domain, not edition-tier.** `surrealdb.statement`, `surrealdb.query`,
//!   `surrealdb.transaction`, … each describe what the instrument records, not which build flavour
//!   shipped it. Edition is conveyed once on the OTel `Resource` via `service.edition` (see
//!   [`crate::telemetry::set_service_edition`]).
//! - **One labelled family per signal.** Aggregate / dimensional dual-emission is collapsed: a
//!   single `surrealdb.statement` counter carries the safe half (`statement_type`, `outcome`) and
//!   the resolved tenant ctx (`namespace`, `database`, `user`, sentinel `"-"` when unset). Drop the
//!   former `.dim.` infix and the parallel `*.errors` counters.
//! - **Security boundary stays at instrument-name allowlisting.** Anonymous `/metrics` consumers
//!   see only families on [`super::public::PUBLIC_METRICS`]; that list keys off rendered Prometheus
//!   family names, so renaming scopes here changes nothing at the boundary.
//!
//! Instrument-name conversions follow the OTel-Prometheus contract: dotted
//! names become underscore-separated, counters get a `_total` suffix, and
//! histogram / counter units (`s`, `By`) become `_seconds` / `_bytes`
//! suffixes.

/// Sentinel attribute value used when a `*Ctx` field is absent. The same
/// constant is used by every observer so dimensional roll-ups, audit
/// records, and per-tenant rollups all collapse missing context to the
/// same series.
pub static NONE_LABEL: &str = "-";

/// Per-signal-domain meter scopes. The `otel_scope_name` label rendered by
/// the Prometheus exporter takes its value from these constants.
pub mod scope {
	/// Top-level statement events.
	pub static STATEMENT: &str = "surrealdb.statement";
	/// Executor query batch events.
	pub static QUERY: &str = "surrealdb.query";
	/// Transaction lifecycle and KV counters.
	pub static TRANSACTION: &str = "surrealdb.transaction";
	/// RPC method invocations (WebSocket and HTTP-RPC).
	pub static RPC: &str = "surrealdb.rpc";
	/// Authentication attempts.
	pub static AUTH: &str = "surrealdb.auth";
	/// Session connect / disconnect lifecycle.
	pub static SESSION: &str = "surrealdb.session";
	/// Inbound / outbound bytes at HTTP / WebSocket ingress.
	pub static NETWORK: &str = "surrealdb.network";
	/// HTTP request lifecycle (mounted as the outer tower layer).
	pub static HTTP: &str = "surrealdb.http";
	/// Live-query subscription gauge and notification counter.
	pub static LIVE_QUERY: &str = "surrealdb.live_query";
	/// Process-level gauges (build info, uptime, memory, CPU).
	pub static PROCESS: &str = "surrealdb.process";
	/// Cardinality-capped per-tenant rollup counters keyed on `(namespace, database)`.
	pub static TENANT: &str = "surrealdb.tenant";
	/// SurrealDS cluster internals (TAPIR per-peer counters).
	pub static DS: &str = "surrealdb.ds";
	/// Audit pipeline self-metrics (queue + sink counters).
	pub static AUDIT: &str = "surrealdb.audit";
	/// Slow-query pipeline self-metrics.
	pub static SLOW_QUERY: &str = "surrealdb.slow_query";
	/// Storage-backend manifest gauges.
	pub static STORAGE: &str = "surrealdb.storage";
	/// GraphQL operation lifecycle counters / histograms.
	pub static GRAPHQL: &str = "surrealdb.graphql";
	/// MCP (Model Context Protocol) tool invocation counters / histograms.
	pub static MCP: &str = "surrealdb.mcp";

	/// Every scope known to the unified provider. Tests assert this list
	/// matches what observers actually register.
	pub static ALL: &[&str] = &[
		AUDIT,
		AUTH,
		DS,
		GRAPHQL,
		HTTP,
		LIVE_QUERY,
		MCP,
		NETWORK,
		PROCESS,
		QUERY,
		RPC,
		SESSION,
		SLOW_QUERY,
		STATEMENT,
		STORAGE,
		TENANT,
		TRANSACTION,
	];
}

/// Instrument names used by the unified [`super::MetricsObserver`]. Listed
/// here so tests and security-review tooling can scan the canonical set
/// without grepping the recording sites.
///
/// Names use the OTel-canonical dotted form. The Prometheus text exporter
/// converts `.` to `_` and appends `_total` (counters) / `_seconds`
/// (histograms with unit `s`) / `_bytes` (counters or histograms with unit
/// `By`).
pub mod names {
	// --- Process / build (scope: PROCESS) ------------------------------

	pub static BUILD_INFO: &str = "surrealdb.build.info";
	pub static PROCESS_UPTIME: &str = "surrealdb.process.uptime";
	pub static PROCESS_MEMORY: &str = "surrealdb.process.memory";
	pub static PROCESS_CPU_PERCENT: &str = "surrealdb.process.cpu_percent";

	// --- Statement (scope: STATEMENT) ----------------------------------

	pub static STATEMENT_TOTAL: &str = "surrealdb.statement";
	pub static STATEMENT_DURATION: &str = "surrealdb.statement.duration";
	pub static STATEMENT_ROWS: &str = "surrealdb.statement.rows";

	// --- Query (scope: QUERY) ------------------------------------------

	pub static QUERY_TOTAL: &str = "surrealdb.query";
	pub static QUERY_DURATION: &str = "surrealdb.query.duration";

	// --- Transaction (scope: TRANSACTION) ------------------------------

	pub static TRANSACTION_TOTAL: &str = "surrealdb.transaction";
	pub static TRANSACTION_DURATION: &str = "surrealdb.transaction.duration";
	pub static TRANSACTION_KV_OPS: &str = "surrealdb.transaction.kv_ops";
	pub static TRANSACTION_KEYS_READ: &str = "surrealdb.transaction.keys_read";
	pub static TRANSACTION_KEYS_WRITTEN: &str = "surrealdb.transaction.keys_written";
	pub static TRANSACTION_KEY_BYTES_READ: &str = "surrealdb.transaction.key_bytes_read";
	pub static TRANSACTION_VALUE_BYTES_READ: &str = "surrealdb.transaction.value_bytes_read";
	pub static TRANSACTION_KEY_BYTES_WRITTEN: &str = "surrealdb.transaction.key_bytes_written";
	pub static TRANSACTION_VALUE_BYTES_WRITTEN: &str = "surrealdb.transaction.value_bytes_written";
	/// Cumulative count of transactions that aborted due to a commit conflict
	/// (write-write, optimistic-lock, version conflict, etc.) Labelled by
	/// tenant ctx.
	///
	/// Each conflict is itself a retry trigger when surrounded by an
	/// optimistic-retry loop, so this counter doubles as the canonical
	/// retry-pressure signal: alert on
	/// `rate(surrealdb_transaction_conflicts_total[5m])`.
	pub static TRANSACTION_CONFLICTS: &str = "surrealdb.transaction.conflicts";

	// --- RPC (scope: RPC) ----------------------------------------------

	pub static RPC_TOTAL: &str = "surrealdb.rpc";
	pub static RPC_DURATION: &str = "surrealdb.rpc.duration";

	// --- Auth (scope: AUTH) --------------------------------------------

	pub static AUTH_TOTAL: &str = "surrealdb.auth";

	// --- Session (scope: SESSION) --------------------------------------

	pub static SESSION_TOTAL: &str = "surrealdb.session";
	pub static SESSION_ACTIVE: &str = "surrealdb.session.active";
	pub static SESSION_DURATION: &str = "surrealdb.session.duration";

	// --- Network (scope: NETWORK) --------------------------------------

	pub static NETWORK_RECEIVED: &str = "surrealdb.network.received";
	pub static NETWORK_SENT: &str = "surrealdb.network.sent";

	// --- HTTP (scope: HTTP) --------------------------------------------

	pub static HTTP_REQUEST_TOTAL: &str = "surrealdb.http.request";
	pub static HTTP_REQUEST_DURATION: &str = "surrealdb.http.request.duration";
	pub static HTTP_REQUEST_SIZE: &str = "surrealdb.http.request.size";
	pub static HTTP_RESPONSE_SIZE: &str = "surrealdb.http.response.size";
	pub static HTTP_ACTIVE_REQUESTS: &str = "surrealdb.http.active_requests";

	// --- Live query (scope: LIVE_QUERY) --------------------------------

	pub static LIVE_QUERY_ACTIVE: &str = "surrealdb.live_query.active";
	pub static LIVE_QUERY_NOTIFICATIONS: &str = "surrealdb.live_query.notifications";

	// --- Slow query (scope: SLOW_QUERY) --------------------------------

	/// Cumulative count of statements whose execution exceeded
	/// [`crate::cnf::SLOW_QUERY_METRIC_THRESHOLD_MS`].
	pub static SLOW_QUERY_TOTAL: &str = "surrealdb.slow_query";

	// --- GraphQL (scope: GRAPHQL) --------------------------------------

	pub static GRAPHQL_OPERATION_TOTAL: &str = "surrealdb.graphql.operation";
	pub static GRAPHQL_OPERATION_DURATION: &str = "surrealdb.graphql.operation.duration";

	// --- MCP (scope: MCP) ----------------------------------------------

	pub static MCP_TOOL_INVOCATION: &str = "surrealdb.mcp.tool.invocation";
	pub static MCP_TOOL_DURATION: &str = "surrealdb.mcp.tool.duration";
	pub static MCP_SESSION_ACTIVE: &str = "surrealdb.mcp.session.active";
}

/// Attribute keys recorded on labelled instruments. Centralised here so
/// observers, tests, and the `safe` security audit all reference the same
/// strings.
pub mod attrs {
	/// `success` / `error` / `cancelled` (from [`surrealdb_core::observe::Outcome::as_label`]).
	pub static OUTCOME: &str = "outcome";
	/// Bounded statement classification (from
	/// [`surrealdb_core::observe::StatementType::as_label`]).
	pub static STATEMENT_TYPE: &str = "statement_type";
	/// Resolved namespace identifier (sentinel `"-"` when unset).
	pub static NAMESPACE: &str = "namespace";
	/// Resolved database identifier (sentinel `"-"` when unset).
	pub static DATABASE: &str = "database";
	/// Resolved user / actor identifier (sentinel `"-"` when unset, `<record>` for
	/// record-access principals — see
	/// [`surrealdb_core::observe::TenantIdentity::from_session`]).
	pub static USER: &str = "user";
	/// Bounded session protocol identifier (`websocket` / `http`).
	pub static PROTOCOL: &str = "protocol";
	/// Lifecycle action on a session event (`connect` / `disconnect`).
	pub static SESSION_ACTION: &str = "session_action";
	/// Service name supplied by the client on session connect (sentinel `"-"` when unset).
	pub static SERVICE: &str = "service";
	/// `true` / `false` -- whether a transaction performed at least one write.
	pub static WRITE: &str = "write";
	/// KV operation classification (`get` / `scan` / `put` / `set` / `del`).
	pub static KV_OP: &str = "op";
	/// Bounded RPC method identifier (from [`surrealdb_core::rpc::Method::to_str`]).
	pub static RPC_METHOD: &str = "rpc.method";
	/// Auth attempt type (`signin` / `signup` / `authenticate` / …).
	pub static AUTH_ACTION: &str = "auth_action";
	/// Auth credential level (`root` / `namespace` / `database` / `record` / `none`).
	pub static AUTH_SCOPE: &str = "auth_scope";
	/// Bounded HTTP method identifier (OTel SemConv-aligned key).
	pub static HTTP_METHOD: &str = "http.request.method";
	/// Matched HTTP route template (sentinel `"-"` when the request did not match a route).
	pub static HTTP_ROUTE: &str = "http.route";
	/// HTTP response status code, stringified.
	pub static HTTP_STATUS_CODE: &str = "http.response.status_code";
	/// Inbound / outbound (`received` / `sent`) on network byte counters.
	pub static NETWORK_DIRECTION: &str = "direction";
	/// Bounded error classification recorded only when `outcome="error"`. Sourced
	/// from [`error_class`] constants so cardinality stays closed.
	pub static ERROR_CLASS: &str = "error_class";
	/// GraphQL operation type (`query` / `mutation` / `subscription`).
	pub static GRAPHQL_OPERATION_TYPE: &str = "operation_type";
	/// MCP tool name (bounded — sourced from the static tool dispatch in the mcp crate).
	pub static MCP_TOOL: &str = "tool";
	/// MCP transport (`stdio` / `http`).
	pub static MCP_TRANSPORT: &str = "transport";
}

/// Re-export the core's bounded error-classification constants so server-side
/// recording sites and tests share a single source of truth with the core
/// emit sites. The core uses these from the executor (per-statement
/// classification, cancel/timeout paths), the kvs commit-failure path, and
/// from the RPC dispatch shim. Server crates additionally use them via the
/// shared classifier helpers ([`error_class::classify_types_error`],
/// [`error_class::classify_anyhow_error`],
/// [`error_class::classify_http_status`]) at the HTTP / GraphQL / MCP layers.
pub use surrealdb_core::observe::error_class;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn scope_all_is_sorted_and_unique() {
		let mut sorted = scope::ALL.to_vec();
		sorted.sort_unstable();
		sorted.dedup();
		assert_eq!(scope::ALL, sorted, "scope::ALL must be sorted and free of duplicates");
	}

	#[test]
	fn every_instrument_name_starts_with_surrealdb_prefix() {
		const NAMES: &[&str] = &[
			names::AUTH_TOTAL,
			names::BUILD_INFO,
			names::GRAPHQL_OPERATION_DURATION,
			names::GRAPHQL_OPERATION_TOTAL,
			names::HTTP_ACTIVE_REQUESTS,
			names::HTTP_REQUEST_DURATION,
			names::HTTP_REQUEST_SIZE,
			names::HTTP_REQUEST_TOTAL,
			names::HTTP_RESPONSE_SIZE,
			names::LIVE_QUERY_ACTIVE,
			names::LIVE_QUERY_NOTIFICATIONS,
			names::MCP_SESSION_ACTIVE,
			names::MCP_TOOL_DURATION,
			names::MCP_TOOL_INVOCATION,
			names::NETWORK_RECEIVED,
			names::NETWORK_SENT,
			names::PROCESS_CPU_PERCENT,
			names::PROCESS_MEMORY,
			names::PROCESS_UPTIME,
			names::QUERY_DURATION,
			names::QUERY_TOTAL,
			names::RPC_DURATION,
			names::RPC_TOTAL,
			names::SESSION_ACTIVE,
			names::SESSION_DURATION,
			names::SESSION_TOTAL,
			names::SLOW_QUERY_TOTAL,
			names::STATEMENT_DURATION,
			names::STATEMENT_ROWS,
			names::STATEMENT_TOTAL,
			names::TRANSACTION_CONFLICTS,
			names::TRANSACTION_DURATION,
			names::TRANSACTION_KEYS_READ,
			names::TRANSACTION_KEYS_WRITTEN,
			names::TRANSACTION_KEY_BYTES_READ,
			names::TRANSACTION_KEY_BYTES_WRITTEN,
			names::TRANSACTION_KV_OPS,
			names::TRANSACTION_TOTAL,
			names::TRANSACTION_VALUE_BYTES_READ,
			names::TRANSACTION_VALUE_BYTES_WRITTEN,
		];
		for n in NAMES {
			assert!(
				n.starts_with("surrealdb."),
				"instrument `{n}` does not use the `surrealdb.` namespace",
			);
		}
	}

	#[test]
	fn no_dim_infix_in_instrument_names() {
		// Regression guard: the `.dim.` infix is the symptom that meter
		// scopes were being used to disambiguate aggregate from labelled
		// emissions of the same family. The unified observer registers
		// one labelled family per signal so the infix MUST stay gone.
		const NAMES: &[&str] = &[
			names::QUERY_DURATION,
			names::HTTP_REQUEST_DURATION,
			names::HTTP_ACTIVE_REQUESTS,
			names::NETWORK_RECEIVED,
			names::NETWORK_SENT,
		];
		for n in NAMES {
			assert!(!n.contains(".dim."), "`{n}` retained the legacy `.dim.` infix");
		}
	}
}
