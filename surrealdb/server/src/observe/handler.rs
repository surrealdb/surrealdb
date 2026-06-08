//! The `/metrics` HTTP handler.
//!
//! Renders the unified [`opentelemetry_sdk::metrics::SdkMeterProvider`] in
//! Prometheus text exposition format, applying the
//! [`super::public::PUBLIC_METRICS`] allowlist for unauthenticated callers.
//!
//! Behaviour:
//!
//! - **Root-authenticated** scrapers receive every metric family in the unified provider: the
//!   labelled `surrealdb.*` families, per-tenant rollups, SurrealDS cluster internals, audit /
//!   slow-query pipeline self-metrics, storage backend gauges, and process gauges.
//! - **Unauthenticated** scrapers receive only metric families whose name appears in
//!   [`super::public::PUBLIC_METRICS`]. Everything else is filtered out at render time so the
//!   public surface stays bounded to non-attributable process signals.
//!
//! The `PUBLIC_METRICS` allowlist keys off the rendered Prometheus family
//! name. Renaming meter scopes does not affect what an anonymous scraper
//! sees: the boundary is the metric name, not the scope.
//!
//! All access — public or authenticated — is gated upstream on
//! [`crate::cnf::METRICS_ENABLED`]. When disabled the route is not mounted
//! at all, so this handler never runs.

use std::sync::Arc;

use axum::response::IntoResponse;
use http::StatusCode;
use http::header::CONTENT_TYPE;
use opentelemetry_prometheus_text_exporter::PrometheusExporter;

use super::metrics::MetricsObserver;
use super::public::is_public_metric;
use super::session::OperatorAuth;

/// Content-type for the Prometheus text exposition format the exporter
/// produces.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// Shared `/metrics` state installed as an axum extension.
///
/// `exporter` is the [`PrometheusExporter`] cloned out of the
/// [`ObservabilityRuntime`](super::ObservabilityRuntime) at server start;
/// cloning is cheap because the inner state is shared with the SDK's
/// `SdkMeterProvider`. `observer` is the unified [`MetricsObserver`] so
/// call sites that need to bump the live-query gauge or notification
/// counter directly (the RPC dispatcher) can do so without a fan-out
/// indirection.
#[derive(Clone)]
pub struct MetricsState {
	pub exporter: PrometheusExporter,
	pub observer: Arc<MetricsObserver>,
}

/// Handler for `GET /metrics`.
///
/// Renders the Prometheus text exposition format and post-filters by
/// metric-family name when the caller is not an operator.
#[instrument(level = "debug", name = "observe::metrics", skip_all)]
pub async fn metrics(
	axum::Extension(state): axum::Extension<MetricsState>,
	OperatorAuth(is_operator): OperatorAuth,
) -> axum::response::Response {
	// The cached process snapshot is refreshed on a fixed cadence by a
	// background task spawned in [`crate::cli::start::init`] so the
	// observable-gauge callbacks always see fresh values. The handler
	// no longer awaits a per-scrape refresh; OTLP-only deployments get
	// the same freshness guarantee as Prometheus scrapers.
	match render_snapshot(&state.exporter, is_operator) {
		Ok(buf) => (StatusCode::OK, [(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], buf).into_response(),
		Err(err) => {
			error!("failed to encode metrics: {err}");
			(StatusCode::INTERNAL_SERVER_ERROR, "failed to encode metrics").into_response()
		}
	}
}

/// Render a snapshot of every metric family registered against the unified
/// `SdkMeterProvider` in the Prometheus text exposition format. When
/// `is_operator` is `false`, post-filter the output by metric-family name
/// against [`super::public::PUBLIC_METRICS`].
pub(crate) fn render_snapshot(
	exporter: &PrometheusExporter,
	is_operator: bool,
) -> Result<Vec<u8>, anyhow::Error> {
	let mut buf = Vec::with_capacity(4096);
	exporter.export(&mut buf).map_err(|e| anyhow::anyhow!("{e}"))?;
	if is_operator {
		return Ok(buf);
	}
	let text = std::str::from_utf8(&buf)
		.map_err(|e| anyhow::anyhow!("non-utf8 prometheus output: {e}"))?;
	Ok(filter_to_public_metrics(text).into_bytes())
}

/// Retain only metric families whose name is in [`super::public::PUBLIC_METRICS`].
///
/// The Prometheus text format groups each family as a `# HELP <name>` line
/// followed by `# TYPE <name>` and zero or more data lines, terminated by
/// the next family's `# HELP` line. We track the current family name from
/// the `# HELP` directive and emit lines belonging to allowed families
/// only.
fn filter_to_public_metrics(text: &str) -> String {
	let mut out = String::with_capacity(text.len() / 4);
	let mut keep_block = false;
	for line in text.lines() {
		// `# HELP <name> <desc>` starts a new family. Decide whether to
		// keep it before emitting the line itself.
		if let Some(rest) = line.strip_prefix("# HELP ") {
			let name = rest.split_whitespace().next().unwrap_or("");
			keep_block = is_public_metric(name);
		} else if let Some(rest) = line.strip_prefix("# TYPE ") {
			// `# TYPE` for the current family — the keep_block flag is
			// already correct from the preceding `# HELP`. If `# HELP`
			// is absent for some reason, fall back to the type-line name.
			let name = rest.split_whitespace().next().unwrap_or("");
			keep_block = is_public_metric(name);
		}
		if keep_block {
			out.push_str(line);
			out.push('\n');
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use opentelemetry_prometheus_text_exporter::PrometheusExporter;
	use opentelemetry_sdk::metrics::SdkMeterProvider;
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
	use crate::observe::ObservabilityRuntime;

	#[test]
	fn filter_drops_non_public_families() {
		let raw = "\
# HELP surrealdb_build_info Build info
# TYPE surrealdb_build_info gauge
surrealdb_build_info{build_version=\"3.0\"} 1
# HELP surrealdb_statement_total Statements
# TYPE surrealdb_statement_total counter
surrealdb_statement_total{outcome=\"success\"} 42
# HELP surrealdb_process_uptime_seconds Uptime
# TYPE surrealdb_process_uptime_seconds gauge
surrealdb_process_uptime_seconds 99
";
		let filtered = filter_to_public_metrics(raw);
		assert!(filtered.contains("surrealdb_build_info"));
		assert!(filtered.contains("surrealdb_process_uptime_seconds"));
		assert!(!filtered.contains("surrealdb_statement_total"));
	}

	/// Poison-pill string sprinkled through every `*Ctx` field. If it
	/// ever appears in the unauthenticated Prometheus output we have a
	/// security bug.
	const CANARY: &str = "xCANARYx-tenant-secret-xyz";

	/// Build a fresh runtime backed by an isolated Prometheus exporter
	/// so the test owns its rendered output without consulting any
	/// global state.
	fn fresh_runtime() -> (SdkMeterProvider, PrometheusExporter, ObservabilityRuntime) {
		let exporter = PrometheusExporter::builder().build();
		let provider = SdkMeterProvider::builder().with_reader(exporter.clone()).build();
		let runtime = ObservabilityRuntime::builder(provider.clone())
			.with_prometheus_exporter(exporter.clone())
			.build();
		(provider, exporter, runtime)
	}

	fn dump(exporter: &PrometheusExporter) -> String {
		let mut buf = Vec::with_capacity(4096);
		exporter.export(&mut buf).expect("export");
		String::from_utf8(buf).expect("utf8")
	}

	/// Drives every event variant through the unified [`MetricsObserver`]
	/// with tenant-identifying canary strings populated and asserts that
	/// the **filtered** Prometheus output (the view an unauthenticated
	/// scraper sees) never contains the canary. The full unfiltered
	/// output IS expected to carry the canary on the labelled families
	/// — that is exactly why the filter exists. The `PUBLIC_METRICS`
	/// allowlist is the security boundary; this test verifies it
	/// correctly excludes every labelled family.
	#[tokio::test]
	async fn no_tenant_canary_in_unauthenticated_prometheus_output() {
		let (provider, exporter, runtime) = fresh_runtime();
		let metrics_obs = MetricsObserver::new(&runtime).expect("metrics observer");

		let stmt = StatementEvent {
			safe: StatementEventSafe {
				kind: StatementType::Select,
				outcome: Outcome::Success,
				duration: std::time::Duration::from_millis(2),
				read_only: true,
				result_rows: 0,
				error_class: None,
			},
			ctx: StatementEventCtx {
				sql: Some(format!("SELECT * FROM {CANARY}")),
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				session_id: None,
				client_ip: None,
			},
		};
		metrics_obs.on_statement_complete(&stmt);

		metrics_obs.on_query_complete(&QueryEvent {
			safe: QueryEventSafe {
				outcome: Outcome::Success,
				duration: std::time::Duration::from_millis(1),
				counters: QueryCounters::default(),
				error_class: None,
			},
			ctx: QueryEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				session_id: None,
				client_ip: None,
			},
		});

		let txn_event = TransactionEvent {
			safe: TransactionEventSafe {
				outcome: Outcome::Success,
				write: true,
				duration: std::time::Duration::from_millis(1),
				metrics: TransactionMetricsSnapshot::default(),
				error_class: None,
			},
			ctx: TransactionEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				session_id: None,
				client_ip: None,
			},
		};
		metrics_obs.on_transaction_complete(&txn_event);

		let rpc_event = RpcEvent {
			safe: RpcEventSafe {
				method: surrealdb_core::rpc::Method::Select,
				outcome: Outcome::Success,
				duration: std::time::Duration::from_millis(1),
				error_class: None,
			},
			ctx: RpcEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				session_id: None,
				client_ip: None,
			},
		};
		metrics_obs.on_rpc_complete(&rpc_event);

		metrics_obs.on_auth_event(&AuthEvent {
			safe: AuthEventSafe {
				action: AuthAction::Signin,
				scope: AuthScope::Root,
				outcome: Outcome::Success,
				error_class: None,
			},
			ctx: AuthEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				session_id: None,
				client_ip: None,
			},
		});

		let session_event = SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Connect,
				protocol: SessionProtocol::WebSocket,
				duration: None,
			},
			ctx: SessionEventCtx {
				session_id: None,
				service_name: Some(CANARY.into()),
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				client_ip: None,
			},
		};
		metrics_obs.on_session_event(&session_event);

		let bytes_event = NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Received,
				protocol: SessionProtocol::WebSocket,
				bytes: 64,
			},
			ctx: NetworkBytesEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
			},
		};
		metrics_obs.on_network_bytes(&bytes_event);

		let http_start = HttpRequestStartEvent {
			safe: HttpRequestStartEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				version: HttpVersion::Http11,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				session_id: None,
				client_ip: None,
			},
		};
		metrics_obs.on_http_request_started(&http_start);

		let http_complete = HttpRequestEvent {
			safe: HttpRequestEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				status_code: Some(200),
				version: HttpVersion::Http11,
				outcome: Outcome::Success,
				duration: std::time::Duration::from_millis(2),
				request_size: Some(64),
				response_size: Some(128),
				error_class: None,
			},
			ctx: HttpRequestEventCtx {
				namespace: Some(CANARY.into()),
				database: Some(CANARY.into()),
				user: Some(CANARY.into()),
				session_id: None,
				client_ip: None,
			},
		};
		metrics_obs.on_http_request_complete(&http_complete);

		// Sanity check: the canary IS on the unfiltered output —
		// labelled families like `surrealdb_statement_total{namespace=…}`
		// carry it by design. Without this assertion the test could
		// pass by silently failing to record any of the events above.
		let body = dump(&exporter);
		assert!(
			body.contains(CANARY),
			"events did not record: canary should appear on unfiltered output\n{body}",
		);

		// The actual security check: when the rendered output is
		// filtered through the `PUBLIC_METRICS` allowlist (the view
		// an unauthenticated scraper sees), the canary MUST NOT
		// appear. Every labelled family carrying tenant data is
		// rejected by name, leaving only the process / build gauges
		// behind.
		let filtered = filter_to_public_metrics(&body);
		assert!(
			!filtered.contains(CANARY),
			"tenant canary leaked into UNAUTHENTICATED Prometheus output:\n{filtered}",
		);

		provider.shutdown().expect("shutdown");
	}
}
