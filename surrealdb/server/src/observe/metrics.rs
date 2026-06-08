//! The unified metrics observer.
//!
//! [`MetricsObserver`] translates the structured events emitted by the core
//! into one labelled OpenTelemetry instrument family per signal domain,
//! registered under signal-domain meter scopes (`surrealdb.statement`,
//! `surrealdb.query`, `surrealdb.transaction`, `surrealdb.rpc`,
//! `surrealdb.session`, `surrealdb.auth`, `surrealdb.network`,
//! `surrealdb.http`, `surrealdb.live_query`). The shared `SdkMeterProvider`
//! built in [`crate::telemetry::metrics`] routes those instruments to two
//! readers: the Prometheus text exporter (rendered by `GET /metrics`) and
//! the OTLP push exporter (when configured). One recording site, two
//! exposition formats.
//!
//! # One labelled family per signal
//!
//! There is no aggregate-vs-dimensional duplication: every counter and
//! histogram carries the resolved tenant ctx
//! (`namespace`, `database`, `user`) plus the safe-half attributes
//! (`outcome`, `statement_type`, `protocol`, …). Anonymous and pre-auth
//! events collapse missing ctx fields to the sentinel `"-"` so a single
//! Prometheus query works in every deployment, without the operator
//! needing to know whether the build was community or enterprise.
//!
//! Edition (community vs enterprise) is conveyed once on the OTel
//! `Resource` via `service.edition` (set at process start through
//! [`crate::telemetry::set_service_edition`]), surfaced as
//! `target_info{service_edition=…}` in the Prometheus output and as a
//! resource attribute on every OTLP record. Operators filter or group by
//! it without observers having to per-instrument-stamp the value.
//!
//! # Naming
//!
//! Instruments use OpenTelemetry semantic-convention-style dotted names
//! (`surrealdb.statement.duration`); the Prometheus text exporter converts
//! them deterministically to `surrealdb_statement_duration_seconds` style
//! family names. Counters get a `_total` suffix; histograms / counters
//! with unit `s` produce `_seconds`, with unit `By` produce `_bytes`.
//!
//! Attribute keys with a dot in the name (`http.request.method`,
//! `http.route`, `http.response.status_code`, `rpc.method`) follow OTel
//! semantic conventions so generic OTel-aligned dashboards work without
//! relabel rules. The Prometheus exporter sanitises the dot to `_` for
//! Prom-form rendering.

use std::borrow::Cow;
use std::sync::Arc;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use surrealdb_core::observe::{
	AuthEvent, ExecutionObserver, HttpRequestEvent, HttpRequestStartEvent, NetworkBytesEvent,
	NetworkDirection, QueryEvent, RpcEvent, SessionAction, SessionEvent, StatementEvent,
	TransactionEvent, process_snapshot,
};

use super::instruments::{NONE_LABEL, attrs, names, scope};
use super::provider::PipelineCounters;
use super::runtime::ObservabilityRuntime;

/// Resolve an `Option<&str>` ctx field into a stable attribute value.
///
/// Returns a [`Cow`] so the `NONE_LABEL` sentinel and any pre-interned
/// caller-supplied value skip the allocation; only present, non-empty
/// user-supplied strings are owned.
fn label_cow(opt: Option<&str>) -> Cow<'static, str> {
	match opt {
		Some(s) if !s.is_empty() => Cow::Owned(s.to_owned()),
		_ => Cow::Borrowed(NONE_LABEL),
	}
}

/// Stringify an HTTP status code as an attribute value. Falls back to
/// [`NONE_LABEL`] when the inner service did not produce a response.
fn http_status(code: Option<u16>) -> Cow<'static, str> {
	match code {
		Some(c) => Cow::Owned(c.to_string()),
		None => Cow::Borrowed(NONE_LABEL),
	}
}

/// Resolve an `Option<&'static str>` error classification into a stable
/// attribute value. Successful / cancelled outcomes carry `None` and collapse
/// to [`NONE_LABEL`]; error outcomes carry one of the bounded
/// [`super::instruments::error_class`] constants.
fn error_class_label(class: Option<&'static str>) -> &'static str {
	class.unwrap_or(NONE_LABEL)
}

/// Records [`surrealdb_core::observe`] events as a labelled OpenTelemetry
/// instrument family per signal domain.
///
/// The observer is the single recording site for every signal in the
/// unified surface. Enterprise composers contribute additional observers
/// for the cardinality-capped per-tenant rollups and SurrealDS cluster
/// internals, but the primary `surrealdb.*` family lives here.
pub struct MetricsObserver {
	// Statement (scope: surrealdb.statement)
	statement_total: Counter<u64>,
	statement_duration: Histogram<f64>,
	statement_rows: Counter<u64>,
	// Query (scope: surrealdb.query)
	query_total: Counter<u64>,
	query_duration: Histogram<f64>,
	// Transaction (scope: surrealdb.transaction)
	transaction_total: Counter<u64>,
	transaction_duration: Histogram<f64>,
	transaction_kv_ops: Counter<u64>,
	transaction_keys_read: Counter<u64>,
	transaction_keys_written: Counter<u64>,
	transaction_key_bytes_read: Counter<u64>,
	transaction_value_bytes_read: Counter<u64>,
	transaction_key_bytes_written: Counter<u64>,
	transaction_value_bytes_written: Counter<u64>,
	transaction_conflicts: Counter<u64>,
	// RPC (scope: surrealdb.rpc)
	rpc_total: Counter<u64>,
	rpc_duration: Histogram<f64>,
	// Auth (scope: surrealdb.auth)
	auth_total: Counter<u64>,
	// Session (scope: surrealdb.session)
	session_total: Counter<u64>,
	session_active: UpDownCounter<i64>,
	session_duration: Histogram<f64>,
	// Network (scope: surrealdb.network)
	network_received: Counter<u64>,
	network_sent: Counter<u64>,
	// HTTP (scope: surrealdb.http)
	http_request_total: Counter<u64>,
	http_request_duration: Histogram<f64>,
	http_request_size: Counter<u64>,
	http_response_size: Counter<u64>,
	http_active_requests: UpDownCounter<i64>,
	// Live query (scope: surrealdb.live_query)
	live_query_active: UpDownCounter<i64>,
	live_query_notifications: Counter<u64>,
	// Slow query (scope: surrealdb.slow_query)
	slow_query_total: Counter<u64>,
	/// Cached threshold (in milliseconds) for `slow_query_total`. `0` disables
	/// the slow-query counter without affecting the duration histogram.
	slow_query_threshold_ms: u64,
	// GraphQL (scope: surrealdb.graphql)
	graphql_operation_total: Counter<u64>,
	graphql_operation_duration: Histogram<f64>,
	// MCP (scope: surrealdb.mcp)
	mcp_tool_invocation: Counter<u64>,
	mcp_tool_duration: Histogram<f64>,
	mcp_session_active: UpDownCounter<i64>,
}

impl MetricsObserver {
	/// Construct the unified observer, lazily registering each
	/// instrument against the supplied
	/// [`ObservabilityRuntime`](super::ObservabilityRuntime)'s meter
	/// provider under its signal-domain scope.
	///
	/// When the runtime carries the no-op default
	/// [`opentelemetry_sdk::metrics::SdkMeterProvider`] (because telemetry
	/// is disabled), every recording becomes a no-op; the observer
	/// itself stays cheap because the OTel SDK short-circuits no-op
	/// meters internally.
	pub fn new(runtime: &ObservabilityRuntime) -> anyhow::Result<Self> {
		let stmt = runtime.meter(scope::STATEMENT);
		let qry = runtime.meter(scope::QUERY);
		let tx = runtime.meter(scope::TRANSACTION);
		let rpc = runtime.meter(scope::RPC);
		let auth = runtime.meter(scope::AUTH);
		let sess = runtime.meter(scope::SESSION);
		let net = runtime.meter(scope::NETWORK);
		let http = runtime.meter(scope::HTTP);
		let lq = runtime.meter(scope::LIVE_QUERY);
		let slow = runtime.meter(scope::SLOW_QUERY);
		let gql = runtime.meter(scope::GRAPHQL);
		let mcp = runtime.meter(scope::MCP);

		Ok(Self {
			statement_total: stmt
				.u64_counter(names::STATEMENT_TOTAL)
				.with_description("Cumulative count of top-level statement completions")
				.build(),
			statement_duration: stmt
				.f64_histogram(names::STATEMENT_DURATION)
				.with_description("Distribution of top-level statement execution latency")
				.with_unit("s")
				.build(),
			statement_rows: stmt
				.u64_counter(names::STATEMENT_ROWS)
				.with_description(
					"Cumulative count of rows returned (SELECT) or affected \
					 (CREATE / UPDATE / UPSERT / DELETE / RELATE / INSERT) by \
					 completed statements",
				)
				.build(),
			query_total: qry
				.u64_counter(names::QUERY_TOTAL)
				.with_description("Cumulative count of query batch completions")
				.build(),
			query_duration: qry
				.f64_histogram(names::QUERY_DURATION)
				.with_description("Distribution of query batch latency")
				.with_unit("s")
				.build(),
			transaction_total: tx
				.u64_counter(names::TRANSACTION_TOTAL)
				.with_description("Cumulative count of transaction completions")
				.build(),
			transaction_duration: tx
				.f64_histogram(names::TRANSACTION_DURATION)
				.with_description("Distribution of transaction lifetime at commit / cancel")
				.with_unit("s")
				.build(),
			transaction_kv_ops: tx
				.u64_counter(names::TRANSACTION_KV_OPS)
				.with_description("Per-transaction KV operation counts, broken out by op type")
				.build(),
			transaction_keys_read: tx
				.u64_counter(names::TRANSACTION_KEYS_READ)
				.with_description("Total keys read across transactions")
				.build(),
			transaction_keys_written: tx
				.u64_counter(names::TRANSACTION_KEYS_WRITTEN)
				.with_description("Total keys written across transactions")
				.build(),
			transaction_key_bytes_read: tx
				.u64_counter(names::TRANSACTION_KEY_BYTES_READ)
				.with_description("Total key bytes read across transactions")
				.with_unit("By")
				.build(),
			transaction_value_bytes_read: tx
				.u64_counter(names::TRANSACTION_VALUE_BYTES_READ)
				.with_description("Total value bytes read across transactions")
				.with_unit("By")
				.build(),
			transaction_key_bytes_written: tx
				.u64_counter(names::TRANSACTION_KEY_BYTES_WRITTEN)
				.with_description("Total key bytes written across transactions")
				.with_unit("By")
				.build(),
			transaction_value_bytes_written: tx
				.u64_counter(names::TRANSACTION_VALUE_BYTES_WRITTEN)
				.with_description("Total value bytes written across transactions")
				.with_unit("By")
				.build(),
			transaction_conflicts: tx
				.u64_counter(names::TRANSACTION_CONFLICTS)
				.with_description(
					"Cumulative count of transactions that aborted due to a commit conflict",
				)
				.build(),
			rpc_total: rpc
				.u64_counter(names::RPC_TOTAL)
				.with_description("Cumulative count of RPC method invocations")
				.build(),
			rpc_duration: rpc
				.f64_histogram(names::RPC_DURATION)
				.with_description("Distribution of RPC method latency")
				.with_unit("s")
				.build(),
			auth_total: auth
				.u64_counter(names::AUTH_TOTAL)
				.with_description("Cumulative count of authentication attempts")
				.build(),
			session_total: sess
				.u64_counter(names::SESSION_TOTAL)
				.with_description("Cumulative count of session connect / disconnect events")
				.build(),
			session_active: sess
				.i64_up_down_counter(names::SESSION_ACTIVE)
				.with_description("Number of currently-connected sessions")
				.build(),
			session_duration: sess
				.f64_histogram(names::SESSION_DURATION)
				.with_description("Distribution of session lifetime at disconnect")
				.with_unit("s")
				.build(),
			network_received: net
				.u64_counter(names::NETWORK_RECEIVED)
				.with_description(
					"Cumulative inbound bytes observed at the HTTP / WebSocket ingress",
				)
				.with_unit("By")
				.build(),
			network_sent: net
				.u64_counter(names::NETWORK_SENT)
				.with_description(
					"Cumulative outbound bytes written at the HTTP / WebSocket egress",
				)
				.with_unit("By")
				.build(),
			http_request_total: http
				.u64_counter(names::HTTP_REQUEST_TOTAL)
				.with_description("Cumulative count of HTTP requests served by the tower stack")
				.build(),
			http_request_duration: http
				.f64_histogram(names::HTTP_REQUEST_DURATION)
				.with_description(
					"Distribution of HTTP request latency, measured at the outer tower layer",
				)
				.with_unit("s")
				.build(),
			http_request_size: http
				.u64_counter(names::HTTP_REQUEST_SIZE)
				.with_description("Cumulative inbound bytes observed on completed HTTP requests")
				.with_unit("By")
				.build(),
			http_response_size: http
				.u64_counter(names::HTTP_RESPONSE_SIZE)
				.with_description("Cumulative outbound bytes written on completed HTTP responses")
				.with_unit("By")
				.build(),
			// Up/down gauge attributes are intentionally restricted to
			// the safe-half of the request envelope (method / route).
			// HTTP request-start fires BEFORE auth runs and so carries
			// `HttpRequestEventCtx::default()`; the corresponding `+1`
			// and the `-1` from the populated completion event must hit
			// the same series, otherwise the gauge drifts forever.
			// Tenant attribution stays on the total / duration / size
			// families above.
			http_active_requests: http
				.i64_up_down_counter(names::HTTP_ACTIVE_REQUESTS)
				.with_description("HTTP requests currently in flight on the tower stack")
				.build(),
			live_query_active: lq
				.i64_up_down_counter(names::LIVE_QUERY_ACTIVE)
				.with_description("Number of currently-registered LIVE queries")
				.build(),
			live_query_notifications: lq
				.u64_counter(names::LIVE_QUERY_NOTIFICATIONS)
				.with_description("Cumulative count of LIVE query notifications dispatched")
				.build(),
			slow_query_total: slow
				.u64_counter(names::SLOW_QUERY_TOTAL)
				.with_description(
					"Cumulative count of statements whose execution exceeded \
					 SURREAL_SLOW_QUERY_METRIC_THRESHOLD_MS",
				)
				.build(),
			slow_query_threshold_ms: *crate::cnf::SLOW_QUERY_METRIC_THRESHOLD_MS,
			graphql_operation_total: gql
				.u64_counter(names::GRAPHQL_OPERATION_TOTAL)
				.with_description("Cumulative count of GraphQL operations executed")
				.build(),
			graphql_operation_duration: gql
				.f64_histogram(names::GRAPHQL_OPERATION_DURATION)
				.with_description("Distribution of GraphQL operation latency")
				.with_unit("s")
				.build(),
			mcp_tool_invocation: mcp
				.u64_counter(names::MCP_TOOL_INVOCATION)
				.with_description("Cumulative count of MCP tool invocations")
				.build(),
			mcp_tool_duration: mcp
				.f64_histogram(names::MCP_TOOL_DURATION)
				.with_description("Distribution of MCP tool invocation latency")
				.with_unit("s")
				.build(),
			mcp_session_active: mcp
				.i64_up_down_counter(names::MCP_SESSION_ACTIVE)
				.with_description("Number of currently-active MCP sessions")
				.build(),
		})
	}

	/// Bump or drop the active LIVE query gauge under the supplied tenant
	/// ctx. Callers are expected to pass the namespace/database of the
	/// session that registered the LIVE statement; the corresponding
	/// decrement on KILL or session teardown MUST use the same ctx so the
	/// gauge series stays balanced.
	pub fn adjust_live_query_active(
		&self,
		delta: i64,
		namespace: Option<&str>,
		database: Option<&str>,
	) {
		if delta == 0 {
			return;
		}
		let attrs = [
			KeyValue::new(attrs::NAMESPACE, label_cow(namespace)),
			KeyValue::new(attrs::DATABASE, label_cow(database)),
		];
		self.live_query_active.add(delta, &attrs);
	}

	/// Record a single LIVE query notification dispatch under the supplied
	/// tenant ctx. The labels mirror those used on the active gauge so
	/// operators can join across the two families.
	pub fn record_live_query_notification(&self, namespace: Option<&str>, database: Option<&str>) {
		let attrs = [
			KeyValue::new(attrs::NAMESPACE, label_cow(namespace)),
			KeyValue::new(attrs::DATABASE, label_cow(database)),
		];
		self.live_query_notifications.add(1, &attrs);
	}

	/// Record a single GraphQL operation completion.
	///
	/// `operation_type` should be a stable lower-case identifier from the
	/// closed set `{"query", "mutation", "subscription", "unknown"}`.
	/// `error_class` is `Some` only when `outcome` is
	/// [`surrealdb_core::observe::Outcome::Error`].
	#[allow(clippy::too_many_arguments)]
	pub fn record_graphql_operation(
		&self,
		operation_type: &'static str,
		outcome: surrealdb_core::observe::Outcome,
		error_class: Option<&'static str>,
		duration: std::time::Duration,
		namespace: Option<&str>,
		database: Option<&str>,
		user: Option<&str>,
	) {
		let attrs = [
			KeyValue::new(attrs::GRAPHQL_OPERATION_TYPE, operation_type),
			KeyValue::new(attrs::OUTCOME, outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(namespace)),
			KeyValue::new(attrs::DATABASE, label_cow(database)),
			KeyValue::new(attrs::USER, label_cow(user)),
		];
		self.graphql_operation_total.add(1, &attrs);
		self.graphql_operation_duration.record(duration.as_secs_f64(), &attrs);
	}

	/// Record a single MCP tool invocation.
	///
	/// `tool` is the bounded tool identifier from the MCP service's static
	/// dispatch table; `transport` is one of `"stdio"` / `"http"`.
	pub fn record_mcp_tool(
		&self,
		tool: &'static str,
		transport: &'static str,
		outcome: surrealdb_core::observe::Outcome,
		error_class: Option<&'static str>,
		duration: std::time::Duration,
	) {
		let attrs = [
			KeyValue::new(attrs::MCP_TOOL, tool),
			KeyValue::new(attrs::MCP_TRANSPORT, transport),
			KeyValue::new(attrs::OUTCOME, outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(error_class)),
		];
		self.mcp_tool_invocation.add(1, &attrs);
		self.mcp_tool_duration.record(duration.as_secs_f64(), &attrs);
	}

	/// Bump or drop the active MCP session gauge. Connect / disconnect must
	/// pass the same `transport` so the gauge series stays balanced.
	pub fn adjust_mcp_session_active(&self, delta: i64, transport: &'static str) {
		if delta == 0 {
			return;
		}
		let attrs = [KeyValue::new(attrs::MCP_TRANSPORT, transport)];
		self.mcp_session_active.add(delta, &attrs);
	}

	/// Register pipeline self-metrics
	/// (`<scope_and_prefix>.records` / `.dropped` / `.queue_depth` /
	/// `.appended` / `.append_errors`) for an audit-style sink.
	///
	/// The same string is used both as the OTel meter scope and as the
	/// metric-name prefix, e.g. [`scope::AUDIT`] = `"surrealdb.audit"`
	/// produces gauges `surrealdb.audit.records`, `.dropped`, … under
	/// the `surrealdb.audit` instrumentation scope. Callers pass
	/// `&'static str` so the meter scope value lives for the lifetime
	/// of the global provider without per-call allocation.
	///
	/// Implemented as observable gauges so values are pulled at scrape /
	/// export time from the supplied [`PipelineCounters`]. The registered
	/// gauges live for the lifetime of the runtime's meter provider and
	/// need no explicit unregister.
	pub fn register_pipeline_self_metrics(
		runtime: &ObservabilityRuntime,
		scope_and_prefix: &'static str,
		kind: &str,
		counters: Arc<dyn PipelineCounters>,
	) -> anyhow::Result<()> {
		let meter = runtime.meter(scope_and_prefix);
		let records = Arc::clone(&counters);
		let _records_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.records"))
			.with_description(format!("Cumulative count of {kind} records successfully enqueued",))
			.with_callback(move |obs| obs.observe(records.records_total(), &[]))
			.build();
		let dropped = Arc::clone(&counters);
		let _dropped_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.dropped"))
			.with_description(format!(
				"Cumulative count of {kind} records dropped due to queue overflow",
			))
			.with_callback(move |obs| obs.observe(dropped.dropped_total(), &[]))
			.build();
		let depth = Arc::clone(&counters);
		let _depth_gauge = meter
			.i64_observable_gauge(format!("{scope_and_prefix}.queue_depth"))
			.with_description(format!(
				"Number of {kind} records currently buffered between observer and sink",
			))
			.with_callback(move |obs| obs.observe(depth.queue_depth(), &[]))
			.build();
		let appended = Arc::clone(&counters);
		let _appended_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.appended"))
			.with_description(format!(
				"Cumulative count of {kind} records successfully written to the sink",
			))
			.with_callback(move |obs| obs.observe(appended.appended_total(), &[]))
			.build();
		let errors = counters;
		let _errors_gauge = meter
			.u64_observable_gauge(format!("{scope_and_prefix}.append_errors"))
			.with_description(format!(
				"Cumulative count of {kind} records that failed to write to the sink",
			))
			.with_callback(move |obs| obs.observe(errors.append_errors_total(), &[]))
			.build();
		Ok(())
	}
}

/// Register process-level observable gauges (build_info, uptime, memory,
/// cpu) against the supplied runtime's `surrealdb.process` meter scope.
///
/// Build info is a constant gauge with the `build_version` attribute. The
/// other three observe live values via the system snapshot helper on every
/// collection call, so scrape / export latency is bounded by `sysinfo`'s
/// refresh cost.
pub fn register_process_metrics(runtime: &ObservabilityRuntime) {
	let meter = runtime.meter(scope::PROCESS);
	let started_at = web_time::Instant::now();
	let version = crate::cnf::PKG_VERSION.as_str().to_owned();
	let _build_info = meter
		.u64_observable_gauge(names::BUILD_INFO)
		.with_description("SurrealDB build information (value is always 1)")
		.with_callback(move |obs| {
			obs.observe(1, &[KeyValue::new("build_version", version.clone())]);
		})
		.build();
	let _uptime = meter
		.i64_observable_gauge(names::PROCESS_UPTIME)
		.with_description("Seconds since the surreal process started")
		.with_unit("s")
		.with_callback(move |obs| {
			obs.observe(started_at.elapsed().as_secs() as i64, &[]);
		})
		.build();
	let _memory = meter
		.i64_observable_gauge(names::PROCESS_MEMORY)
		.with_description("Resident set size of the surreal process")
		.with_unit("By")
		.with_callback(|obs| {
			let snap = process_snapshot();
			obs.observe(snap.memory_bytes as i64, &[]);
		})
		.build();
	let _cpu = meter
		.f64_observable_gauge(names::PROCESS_CPU_PERCENT)
		.with_description(
			"Total CPU usage of the surreal process as a percentage (may exceed 100% on \
			 multi-core hosts)",
		)
		.with_callback(|obs| {
			let snap = process_snapshot();
			obs.observe(snap.cpu_percent as f64, &[]);
		})
		.build();
}

impl ExecutionObserver for MetricsObserver {
	fn on_statement_complete(&self, event: &StatementEvent) {
		let attrs = [
			KeyValue::new(attrs::STATEMENT_TYPE, event.safe.kind.as_label()),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.statement_total.add(1, &attrs);
		self.statement_duration.record(event.safe.duration.as_secs_f64(), &attrs);
		if event.safe.result_rows > 0 {
			self.statement_rows.add(event.safe.result_rows, &attrs);
		}
		if self.slow_query_threshold_ms > 0 {
			let duration_ms = event.safe.duration.as_millis() as u64;
			if duration_ms >= self.slow_query_threshold_ms {
				// Slow-query counter intentionally re-uses the same labels
				// as `surrealdb.statement.*` so dashboards can join across
				// the two families on `statement_type` / `outcome` / tenant ctx.
				self.slow_query_total.add(1, &attrs);
			}
		}
	}

	fn on_query_complete(&self, event: &QueryEvent) {
		let attrs = [
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.query_total.add(1, &attrs);
		self.query_duration.record(event.safe.duration.as_secs_f64(), &attrs);
	}

	fn on_transaction_complete(&self, event: &TransactionEvent) {
		let outcome = event.safe.outcome.as_label();
		let write = if event.safe.write {
			"true"
		} else {
			"false"
		};
		let core_attrs = [
			KeyValue::new(attrs::WRITE, write),
			KeyValue::new(attrs::OUTCOME, outcome),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.transaction_total.add(1, &core_attrs);
		self.transaction_duration.record(event.safe.duration.as_secs_f64(), &core_attrs);
		// Each commit-conflict failure is itself a retry trigger when
		// surrounded by an optimistic-retry loop, so the conflicts
		// counter doubles as the retry-pressure signal. Operators alert
		// on `rate(surrealdb_transaction_conflicts_total[5m])` rather
		// than a separate `retries` counter; per-transaction retry
		// counting does not fit the architecture (each optimistic
		// retry creates a fresh `Transaction` instance).
		if matches!(event.safe.error_class, Some(c) if c == super::instruments::error_class::TXN_CONFLICT)
		{
			// Drop the WRITE attr from conflicts so the series is keyed on
			// tenant ctx alone -- conflict rate is what operators alert on,
			// not whether the doomed txn was a write or read.
			let conflict_attrs = [
				KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
				KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
				KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
			];
			self.transaction_conflicts.add(1, &conflict_attrs);
		}
		let m = event.safe.metrics;
		let bump = |op: &'static str, n: u32| {
			if n > 0 {
				self.transaction_kv_ops.add(u64::from(n), &[KeyValue::new(attrs::KV_OP, op)]);
			}
		};
		bump("get", m.ops_get);
		bump("scan", m.ops_scan);
		bump("put", m.ops_put);
		bump("set", m.ops_set);
		bump("del", m.ops_del);
		// keys / bytes counters carry the outcome dimension so success
		// vs error cohorts can be separated. Tenant attribution lives
		// on the per-transaction total / duration above.
		let outcome_attrs = [KeyValue::new(attrs::OUTCOME, outcome)];
		if m.keys_read > 0 {
			self.transaction_keys_read.add(m.keys_read, &outcome_attrs);
		}
		if m.keys_written > 0 {
			self.transaction_keys_written.add(m.keys_written, &outcome_attrs);
		}
		if m.key_bytes_read > 0 {
			self.transaction_key_bytes_read.add(m.key_bytes_read, &outcome_attrs);
		}
		if m.value_bytes_read > 0 {
			self.transaction_value_bytes_read.add(m.value_bytes_read, &outcome_attrs);
		}
		if m.key_bytes_written > 0 {
			self.transaction_key_bytes_written.add(m.key_bytes_written, &outcome_attrs);
		}
		if m.value_bytes_written > 0 {
			self.transaction_value_bytes_written.add(m.value_bytes_written, &outcome_attrs);
		}
	}

	fn on_rpc_complete(&self, event: &RpcEvent) {
		let attrs = [
			KeyValue::new(attrs::RPC_METHOD, event.safe.method.to_str()),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.rpc_total.add(1, &attrs);
		self.rpc_duration.record(event.safe.duration.as_secs_f64(), &attrs);
	}

	fn on_auth_event(&self, event: &AuthEvent) {
		let attrs = [
			KeyValue::new(attrs::AUTH_ACTION, event.safe.action.as_label()),
			KeyValue::new(attrs::AUTH_SCOPE, event.safe.scope.as_label()),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.auth_total.add(1, &attrs);
	}

	fn on_session_event(&self, event: &SessionEvent) {
		let action = event.safe.action.as_label();
		let protocol = event.safe.protocol.as_label();
		let service = label_cow(event.ctx.service_name.as_deref());
		let total_attrs = [
			KeyValue::new(attrs::SESSION_ACTION, action),
			KeyValue::new(attrs::PROTOCOL, protocol),
			KeyValue::new(attrs::SERVICE, service.clone()),
		];
		self.session_total.add(1, &total_attrs);
		// Gauge attribute set is the protocol + service subset that is
		// stable across connect/disconnect so the `+1` and `-1` hit
		// the same series.
		let active_attrs = [
			KeyValue::new(attrs::PROTOCOL, protocol),
			KeyValue::new(attrs::SERVICE, service.clone()),
		];
		match event.safe.action {
			SessionAction::Connect => self.session_active.add(1, &active_attrs),
			SessionAction::Disconnect => {
				self.session_active.add(-1, &active_attrs);
				if let Some(d) = event.safe.duration {
					self.session_duration.record(d.as_secs_f64(), &active_attrs);
				}
			}
		}
	}

	fn on_network_bytes(&self, event: &NetworkBytesEvent) {
		if event.safe.bytes == 0 {
			return;
		}
		let attrs = [
			KeyValue::new(attrs::PROTOCOL, event.safe.protocol.as_label()),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		match event.safe.direction {
			NetworkDirection::Received => self.network_received.add(event.safe.bytes, &attrs),
			NetworkDirection::Sent => self.network_sent.add(event.safe.bytes, &attrs),
		}
	}

	fn on_http_request_started(&self, event: &HttpRequestStartEvent) {
		// Active-request gauge is attribute-stripped to method / route
		// so the `+1` here and the `-1` from completion stay balanced
		// across the gauge series. See the constructor for the rationale.
		let attrs = [
			KeyValue::new(attrs::HTTP_METHOD, event.safe.method.as_label()),
			KeyValue::new(attrs::HTTP_ROUTE, label_cow(event.safe.route)),
		];
		self.http_active_requests.add(1, &attrs);
	}

	fn on_http_request_complete(&self, event: &HttpRequestEvent) {
		let route = label_cow(event.safe.route);
		let active_attrs = [
			KeyValue::new(attrs::HTTP_METHOD, event.safe.method.as_label()),
			KeyValue::new(attrs::HTTP_ROUTE, route.clone()),
		];
		self.http_active_requests.add(-1, &active_attrs);

		let attrs = [
			KeyValue::new(attrs::HTTP_METHOD, event.safe.method.as_label()),
			KeyValue::new(attrs::HTTP_ROUTE, route),
			KeyValue::new(attrs::HTTP_STATUS_CODE, http_status(event.safe.status_code)),
			KeyValue::new(attrs::OUTCOME, event.safe.outcome.as_label()),
			KeyValue::new(attrs::ERROR_CLASS, error_class_label(event.safe.error_class)),
			KeyValue::new(attrs::NAMESPACE, label_cow(event.ctx.namespace.as_deref())),
			KeyValue::new(attrs::DATABASE, label_cow(event.ctx.database.as_deref())),
			KeyValue::new(attrs::USER, label_cow(event.ctx.user.as_deref())),
		];
		self.http_request_total.add(1, &attrs);
		self.http_request_duration.record(event.safe.duration.as_secs_f64(), &attrs);
		if let Some(bytes) = event.safe.request_size
			&& bytes > 0
		{
			self.http_request_size.add(bytes, &attrs);
		}
		if let Some(bytes) = event.safe.response_size
			&& bytes > 0
		{
			self.http_response_size.add(bytes, &attrs);
		}
	}

	fn needs_statement_text(&self) -> bool {
		false
	}
}

/// Construct a pre-built [`MetricsObserver`] wrapped in [`Arc`] suitable for
/// handing to the datastore builder.
pub fn into_arc_observer(observer: MetricsObserver) -> Arc<dyn ExecutionObserver> {
	Arc::new(observer)
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::time::Duration;

	use opentelemetry_sdk::error::OTelSdkError;
	use opentelemetry_sdk::metrics::data::ResourceMetrics;
	use opentelemetry_sdk::metrics::reader::MetricReader;
	use opentelemetry_sdk::metrics::{InstrumentKind, ManualReader, SdkMeterProvider, Temporality};
	use surrealdb_core::observe::{
		AuthAction, AuthEvent, AuthEventCtx, AuthEventSafe, AuthScope, ExecutionObserver,
		HttpMethod, HttpRequestEvent, HttpRequestEventCtx, HttpRequestEventSafe,
		HttpRequestStartEvent, HttpRequestStartEventSafe, HttpVersion, NetworkBytesEvent,
		NetworkBytesEventCtx, NetworkBytesEventSafe, NetworkDirection, Outcome, QueryCounters,
		QueryEvent, QueryEventCtx, QueryEventSafe, RpcEvent, RpcEventCtx, RpcEventSafe,
		SessionAction, SessionEvent, SessionEventCtx, SessionEventSafe, SessionProtocol,
		StatementEvent, StatementEventCtx, StatementEventSafe, StatementType, TransactionEvent,
		TransactionEventCtx, TransactionEventSafe, TransactionMetricsSnapshot,
	};

	use super::*;

	#[derive(Clone, Debug)]
	struct TestReader {
		inner: Arc<ManualReader>,
	}

	impl MetricReader for TestReader {
		fn register_pipeline(
			&self,
			pipeline: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>,
		) {
			self.inner.register_pipeline(pipeline);
		}
		fn collect(&self, rm: &mut ResourceMetrics) -> Result<(), OTelSdkError> {
			self.inner.collect(rm)
		}
		fn force_flush(&self) -> Result<(), OTelSdkError> {
			self.inner.force_flush()
		}
		fn shutdown(&self) -> Result<(), OTelSdkError> {
			self.inner.shutdown()
		}
		fn shutdown_with_timeout(&self, timeout: Duration) -> Result<(), OTelSdkError> {
			self.inner.shutdown_with_timeout(timeout)
		}
		fn temporality(&self, kind: InstrumentKind) -> Temporality {
			self.inner.temporality(kind)
		}
	}

	/// Build a fresh runtime with an isolated [`ManualReader`] so each
	/// test sees only its own recordings without touching any global
	/// state. Returns the provider (so the test can call `shutdown()`),
	/// the reader (for collection), and the runtime (for the observer).
	fn fresh_runtime() -> (SdkMeterProvider, TestReader, ObservabilityRuntime) {
		let reader = TestReader {
			inner: Arc::new(ManualReader::builder().build()),
		};
		let provider = SdkMeterProvider::builder().with_reader(reader.clone()).build();
		let runtime = ObservabilityRuntime::builder(provider.clone()).build();
		(provider, reader, runtime)
	}

	/// Snapshot every metric name observed under any scope.
	fn collect_names(reader: &TestReader) -> Vec<(String, String)> {
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		rm.scope_metrics()
			.flat_map(|sm| {
				let scope = sm.scope().name().to_string();
				sm.metrics().map(move |m| (scope.clone(), m.name().to_string()))
			})
			.collect()
	}

	/// Sum every `i64::Sum` data-point on `metric_name` across every
	/// scope. Used to assert balanced gauges return to zero.
	fn sum_i64(reader: &TestReader, metric_name: &str) -> (i64, usize) {
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut total: i64 = 0;
		let mut points = 0usize;
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != metric_name {
					continue;
				}
				if let opentelemetry_sdk::metrics::data::AggregatedMetrics::I64(
					opentelemetry_sdk::metrics::data::MetricData::Sum(sum),
				) = metric.data()
				{
					for dp in sum.data_points() {
						total += dp.value();
						points += 1;
					}
				}
			}
		}
		(total, points)
	}

	/// Walk every data-point on every metric in every scope looking for
	/// an attribute whose value matches `needle`. Used as a tenant-canary
	/// leak check.
	fn any_attr_value_contains(reader: &TestReader, needle: &str) -> bool {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				match metric.data() {
					AggregatedMetrics::U64(MetricData::Sum(s)) => {
						for dp in s.data_points() {
							if dp.attributes().any(|kv| kv.value.as_str().contains(needle)) {
								return true;
							}
						}
					}
					AggregatedMetrics::I64(MetricData::Sum(s)) => {
						for dp in s.data_points() {
							if dp.attributes().any(|kv| kv.value.as_str().contains(needle)) {
								return true;
							}
						}
					}
					AggregatedMetrics::F64(MetricData::Histogram(h)) => {
						for dp in h.data_points() {
							if dp.attributes().any(|kv| kv.value.as_str().contains(needle)) {
								return true;
							}
						}
					}
					_ => {}
				}
			}
		}
		false
	}

	fn statement_event(
		kind: StatementType,
		ns: Option<&str>,
		user: Option<&str>,
		result_rows: u64,
		outcome: Outcome,
	) -> StatementEvent {
		StatementEvent {
			safe: StatementEventSafe {
				kind,
				outcome,
				duration: Duration::from_millis(3),
				read_only: true,
				result_rows,
				error_class: None,
			},
			ctx: StatementEventCtx {
				sql: None,
				namespace: ns.map(String::from),
				database: Some("main".into()),
				user: user.map(String::from),
				..Default::default()
			},
		}
	}

	#[tokio::test]
	async fn statement_records_under_signal_domain_scope() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&statement_event(
			StatementType::Select,
			Some("acme"),
			Some("alice"),
			17,
			Outcome::Success,
		));
		let names = collect_names(&reader);
		// Statement instruments live on the `surrealdb.statement` scope.
		for (instrument, expected_scope) in [
			(names::STATEMENT_TOTAL, scope::STATEMENT),
			(names::STATEMENT_DURATION, scope::STATEMENT),
			(names::STATEMENT_ROWS, scope::STATEMENT),
		] {
			assert!(
				names.iter().any(|(s, n)| s == expected_scope && n == instrument),
				"missing `{instrument}` under `{expected_scope}`: {names:?}",
			);
		}
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn statement_rows_only_increment_when_non_zero() {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&statement_event(
			StatementType::Select,
			Some("acme"),
			Some("alice"),
			17,
			Outcome::Success,
		));
		// DEFINE statements emit zero rows and must not materialise a
		// data point on the rows family.
		obs.on_statement_complete(&statement_event(
			StatementType::Define,
			Some("acme"),
			Some("alice"),
			0,
			Outcome::Success,
		));
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut found_rows_family = false;
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != names::STATEMENT_ROWS {
					continue;
				}
				found_rows_family = true;
				if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
					for dp in sum.data_points() {
						let kind = dp
							.attributes()
							.find(|kv| kv.key.as_str() == attrs::STATEMENT_TYPE)
							.map(|kv| kv.value.as_str().to_string());
						assert_ne!(
							kind.as_deref(),
							Some("define"),
							"non-DML statement leaked into rows family: {dp:?}",
						);
					}
				}
			}
		}
		assert!(found_rows_family, "rows family did not materialise");
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn missing_context_collapses_to_sentinel() {
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&statement_event(
			StatementType::Create,
			None,
			None,
			0,
			Outcome::Success,
		));
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		let mut saw_sentinel = false;
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != names::STATEMENT_TOTAL {
					continue;
				}
				if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
					for dp in sum.data_points() {
						let ns = dp
							.attributes()
							.find(|kv| kv.key.as_str() == attrs::NAMESPACE)
							.map(|kv| kv.value.as_str().to_string());
						let user = dp
							.attributes()
							.find(|kv| kv.key.as_str() == attrs::USER)
							.map(|kv| kv.value.as_str().to_string());
						if ns.as_deref() == Some(NONE_LABEL) && user.as_deref() == Some(NONE_LABEL)
						{
							saw_sentinel = true;
						}
					}
				}
			}
		}
		assert!(saw_sentinel, "sentinel attributes did not appear");
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn transaction_records_under_signal_domain_scope() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_transaction_complete(&TransactionEvent {
			safe: TransactionEventSafe {
				outcome: Outcome::Success,
				write: true,
				duration: Duration::from_millis(7),
				metrics: TransactionMetricsSnapshot {
					keys_read: 12,
					keys_written: 5,
					key_bytes_read: 100,
					value_bytes_read: 1948,
					key_bytes_written: 50,
					value_bytes_written: 974,
					total_bytes_read: 2048,
					total_bytes_written: 1024,
					ops_get: 4,
					ops_scan: 1,
					ops_put: 0,
					ops_set: 5,
					ops_del: 0,
					ops_total: 10,
				},
				error_class: None,
			},
			ctx: TransactionEventCtx::default(),
		});
		let names_seen = collect_names(&reader);
		for expected in [
			names::TRANSACTION_TOTAL,
			names::TRANSACTION_KEYS_READ,
			names::TRANSACTION_KEY_BYTES_WRITTEN,
			names::TRANSACTION_VALUE_BYTES_WRITTEN,
			names::TRANSACTION_KV_OPS,
		] {
			assert!(
				names_seen.iter().any(|(s, n)| s == scope::TRANSACTION && n == expected),
				"missing `{expected}` under `{}`: {names_seen:?}",
				scope::TRANSACTION,
			);
		}
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn rpc_query_auth_record_under_their_scopes() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_query_complete(&QueryEvent {
			safe: QueryEventSafe {
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				counters: QueryCounters {
					total: 3,
					ok: 3,
					err: 0,
				},
				error_class: None,
			},
			ctx: QueryEventCtx {
				namespace: Some("acme".into()),
				database: Some("prod".into()),
				user: Some("svc".into()),
				..Default::default()
			},
		});
		obs.on_rpc_complete(&RpcEvent {
			safe: RpcEventSafe {
				method: surrealdb_core::rpc::Method::Select,
				outcome: Outcome::Success,
				duration: Duration::from_millis(1),
				error_class: None,
			},
			ctx: RpcEventCtx {
				user: Some("svc".into()),
				..Default::default()
			},
		});
		obs.on_auth_event(&AuthEvent {
			safe: AuthEventSafe {
				action: AuthAction::Signin,
				scope: AuthScope::Root,
				outcome: Outcome::Error,
				error_class: None,
			},
			ctx: AuthEventCtx {
				user: Some("alice".into()),
				..Default::default()
			},
		});
		let names_seen = collect_names(&reader);
		for (instrument, expected_scope) in [
			(names::QUERY_TOTAL, scope::QUERY),
			(names::RPC_TOTAL, scope::RPC),
			(names::AUTH_TOTAL, scope::AUTH),
		] {
			assert!(
				names_seen.iter().any(|(s, n)| s == expected_scope && n == instrument),
				"missing `{instrument}` under `{expected_scope}`: {names_seen:?}",
			);
		}
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn session_active_gauge_balances_on_connect_disconnect() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Connect,
				protocol: SessionProtocol::WebSocket,
				duration: None,
			},
			ctx: SessionEventCtx {
				service_name: Some("orders".into()),
				..Default::default()
			},
		});
		obs.on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Disconnect,
				protocol: SessionProtocol::WebSocket,
				duration: Some(Duration::from_secs(30)),
			},
			ctx: SessionEventCtx {
				service_name: Some("orders".into()),
				..Default::default()
			},
		});
		let (sum, points) = sum_i64(&reader, names::SESSION_ACTIVE);
		assert_eq!(
			sum, 0,
			"session.active drifted to {sum} after balanced connect/disconnect over {points} \
			 data point(s)",
		);
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn http_active_requests_balances_with_default_start_ctx() {
		// The dispatcher fires the start event with `HttpRequestEventCtx::default()`
		// (auth has not run yet). The complete event then carries the
		// populated tenant ctx. The gauge MUST be attribute-stripped to
		// the safe-half subset (method / route) so `+1` and `-1` hit
		// the same series.
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_http_request_started(&HttpRequestStartEvent {
			safe: HttpRequestStartEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				version: HttpVersion::Http11,
			},
			ctx: HttpRequestEventCtx::default(),
		});
		obs.on_http_request_complete(&HttpRequestEvent {
			safe: HttpRequestEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				status_code: Some(200),
				version: HttpVersion::Http11,
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				request_size: Some(64),
				response_size: Some(128),
				error_class: None,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some("acme".into()),
				database: Some("prod".into()),
				user: Some("alice".into()),
				..Default::default()
			},
		});
		let (sum, points) = sum_i64(&reader, names::HTTP_ACTIVE_REQUESTS);
		assert_eq!(
			sum, 0,
			"http.active_requests drifted to {sum} (attribute imbalance) across {points} data \
			 point(s)",
		);
		// Gauge must collapse to a single time series — anything else
		// signals tenant attrs leaked onto the +1 / -1 sides.
		assert_eq!(
			points, 1,
			"http.active_requests must collect to a single time series, got {points}",
		);
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn network_bytes_record_with_tenant_dimensions() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Received,
				protocol: SessionProtocol::Http,
				bytes: 1_024,
			},
			ctx: NetworkBytesEventCtx {
				namespace: Some("acme".into()),
				database: Some("prod".into()),
				user: Some("svc".into()),
			},
		});
		let names_seen = collect_names(&reader);
		assert!(
			names_seen.iter().any(|(s, n)| s == scope::NETWORK && n == names::NETWORK_RECEIVED),
			"missing network.received under `{}`: {names_seen:?}",
			scope::NETWORK,
		);
		assert!(any_attr_value_contains(&reader, "acme"));
		provider.shutdown().expect("shutdown");
	}

	#[tokio::test]
	async fn zero_byte_network_event_is_ignored() {
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Received,
				protocol: SessionProtocol::Http,
				bytes: 0,
			},
			ctx: NetworkBytesEventCtx::default(),
		});
		let names_seen = collect_names(&reader);
		assert!(
			!names_seen.iter().any(|(_, n)| n == names::NETWORK_RECEIVED),
			"zero-byte event materialised an instrument: {names_seen:?}",
		);
		provider.shutdown().expect("shutdown");
	}

	/// Tenant canary leak guard: drive every event variant with a
	/// canary identifier in every `*Ctx` field, and assert the canary
	/// only appears on labelled instrument families. The HTTP active
	/// request gauge is attribute-stripped to method / route only and
	/// must NEVER carry the canary.
	#[tokio::test]
	async fn tenant_canary_does_not_leak_into_http_active_requests() {
		const CANARY: &str = "xCANARYx-tenant-secret-xyz";
		let (provider, reader, runtime) = fresh_runtime();
		let obs = MetricsObserver::new(&runtime).expect("observer");
		obs.on_statement_complete(&StatementEvent {
			safe: StatementEventSafe {
				kind: StatementType::Select,
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				read_only: true,
				result_rows: 1,
				error_class: None,
			},
			ctx: StatementEventCtx {
				sql: Some(format!("SELECT * FROM {CANARY}")),
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				..Default::default()
			},
		});
		obs.on_http_request_started(&HttpRequestStartEvent {
			safe: HttpRequestStartEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				version: HttpVersion::Http11,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some(CANARY.into()),
				..Default::default()
			},
		});
		obs.on_http_request_complete(&HttpRequestEvent {
			safe: HttpRequestEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				status_code: Some(200),
				version: HttpVersion::Http11,
				outcome: Outcome::Success,
				duration: Duration::from_millis(2),
				request_size: Some(64),
				response_size: Some(128),
				error_class: None,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				..Default::default()
			},
		});

		// The canary lands on labelled families like `surrealdb.statement`
		// by design (those families require root auth at `/metrics`).
		assert!(
			any_attr_value_contains(&reader, CANARY),
			"tenant canary should appear on labelled families",
		);

		// But it must NEVER appear on the active-request gauge, which
		// is attribute-stripped to method / route only.
		use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
		let mut rm = ResourceMetrics::default();
		reader.collect(&mut rm).expect("collect");
		for sm in rm.scope_metrics() {
			for metric in sm.metrics() {
				if metric.name() != names::HTTP_ACTIVE_REQUESTS {
					continue;
				}
				if let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() {
					for dp in sum.data_points() {
						let leaked = dp.attributes().any(|kv| kv.value.as_str().contains(CANARY));
						assert!(!leaked, "tenant canary leaked onto http.active_requests: {dp:?}",);
					}
				}
			}
		}
		provider.shutdown().expect("shutdown");
	}
}
