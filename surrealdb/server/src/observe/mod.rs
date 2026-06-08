//! Server-side observability wiring: the unified `SdkMeterProvider`, the
//! `/metrics` handler, the labelled [`MetricsObserver`] that listens to
//! core events, and the allowlist enforcing what may appear on the public
//! Prometheus surface.
//!
//! # Unified surface
//!
//! Every metric is recorded through OpenTelemetry instruments under
//! signal-domain meter scopes (`surrealdb.statement`, `surrealdb.query`,
//! `surrealdb.transaction`, `surrealdb.rpc`, `surrealdb.session`,
//! `surrealdb.auth`, `surrealdb.network`, `surrealdb.http`,
//! `surrealdb.live_query`, `surrealdb.process`, `surrealdb.tenant`,
//! `surrealdb.ds`, `surrealdb.audit`, `surrealdb.slow_query`,
//! `surrealdb.storage`). The shared
//! [`opentelemetry_sdk::metrics::SdkMeterProvider`] (built in
//! [`crate::telemetry::metrics`]) routes those instruments to two readers:
//! the Prometheus text exporter (rendered by `GET /metrics`) and the OTLP
//! push exporter (when configured). Audit and slow-query records are
//! emitted alongside as OTel `LogRecord`s through a parallel
//! `SdkLoggerProvider` (see [`crate::telemetry::audit_logs`]).
//!
//! # Edition is a resource attribute
//!
//! Build flavour (community vs enterprise) is conveyed once on the OTel
//! `Resource` via `service.edition`, set at process start through
//! [`crate::telemetry::set_service_edition`]. It surfaces on every
//! Prometheus series via `target_info{service_edition=…}` and on every
//! OTLP export via the resource bundle. Operators filter or group by it
//! without observers having to per-instrument-stamp the value.
//!
//! # Safety-first architecture
//!
//! The `/metrics` endpoint is guarded at render time by
//! [`public::PUBLIC_METRICS`]: when an unauthenticated consumer scrapes
//! `/metrics`, only metric families whose names are present in the
//! allowlist are included. Everything else requires operator authentication
//! (root level) to view. The allowlist is keyed off the rendered
//! Prometheus family name, so renaming OTel scopes does not affect the
//! security boundary.
//!
//! Attribute values that may carry tenant identifiers
//! (`namespace`, `database`, `user`) collapse to a sentinel `"-"` when
//! unset, matching the [`surrealdb_core::observe::TenantIdentity`]
//! resolution rules. Record-access principals collapse to a fixed
//! `<record>` sentinel rather than emitting raw record ids; see
//! [`surrealdb_core::observe::events::TenantIdentity::from_session`].

pub mod handler;
pub mod http_tower;
pub mod instruments;
#[cfg(feature = "mcp")]
pub mod mcp_adapter;
pub mod metrics;
pub mod provider;
pub mod public;
pub mod router;
pub mod runtime;
pub mod session;
pub mod storage_metrics;

pub use handler::MetricsState;
pub use http_tower::HttpMetricsLayer;
#[cfg(feature = "mcp")]
pub use mcp_adapter::McpRecorderAdapter;
pub use metrics::MetricsObserver;
pub use provider::{ObservabilityProvider, PipelineCounters};
pub use runtime::{ObservabilityRuntime, ObservabilityRuntimeBuilder};
pub use storage_metrics::register_storage_metrics;
