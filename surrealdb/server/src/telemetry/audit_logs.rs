//! OpenTelemetry [`SdkLoggerProvider`] used for audit and slow-query
//! records.
//!
//! Audit records and slow-query records flow into this logger provider in
//! addition to their existing durable file sink. The provider routes
//! records to the OTLP logs exporter (when configured), so an OTLP
//! subscriber receives the same structured stream that compliance
//! operators see in the file-based audit log.
//!
//! The file sink is intentionally retained as a separate, parallel path:
//! it carries the durable, hash-chained, fsync-able compliance trail and
//! is independent of OTLP availability. The OTel logger provider here
//! adds a non-blocking push exporter on top.
//!
//! The provider is no longer stored in a process-wide `OnceLock`. The
//! telemetry [`Builder`](super::Builder) returns it as part of the
//! [`ObservabilityRuntime`](crate::observe::ObservabilityRuntime) so
//! every observer that wants an audit logger asks the runtime instance
//! it was constructed with rather than mutating a global.

use opentelemetry_otlp::WithTonicConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{BatchLogProcessor, SdkLoggerProvider};
use tonic::transport::ClientTlsConfig;

use super::OTEL_DEFAULT_RESOURCE;
use crate::cnf::{TELEMETRY_DISABLE_METRICS, TELEMETRY_PROVIDER};

/// Whether OTLP push for logs is configured. Reuses the same gate as
/// metrics: when `SURREAL_TELEMETRY_PROVIDER=otlp` and metrics are not
/// explicitly disabled, OTLP logs export is also enabled. A separate
/// `SURREAL_TELEMETRY_DISABLE_LOGS` knob can be added later if operators
/// need finer control.
fn otlp_logs_active() -> bool {
	TELEMETRY_PROVIDER.trim().eq_ignore_ascii_case("otlp") && !*TELEMETRY_DISABLE_METRICS
}

/// Build the audit / slow-query logger provider.
///
/// Returns `None` when OTLP is not configured, in which case the caller
/// builds an [`ObservabilityRuntime`](crate::observe::ObservabilityRuntime)
/// without an audit logger and emit-site code resolves
/// [`ObservabilityRuntime::audit_logger`] to `None`. When OTLP is
/// configured, the provider carries a [`BatchLogProcessor`] feeding the
/// gRPC OTLP logs exporter.
pub fn init() -> anyhow::Result<Option<SdkLoggerProvider>> {
	if !otlp_logs_active() {
		return Ok(None);
	}
	let resource: Resource = OTEL_DEFAULT_RESOURCE.clone();
	// OTLP logs exporter using the same tonic transport as the metrics
	// pipeline. Native TLS roots so that HTTPS OTLP collector endpoints
	// work out of the box, consistent with the trace and metrics exporters.
	let exporter = opentelemetry_otlp::LogExporter::builder()
		.with_tonic()
		.with_tls_config(ClientTlsConfig::new().with_native_roots())
		.build()?;
	let processor = BatchLogProcessor::builder(exporter).build();
	let provider =
		SdkLoggerProvider::builder().with_resource(resource).with_log_processor(processor).build();
	Ok(Some(provider))
}
