use opentelemetry_otlp::WithTonicConfig;
use opentelemetry_prometheus_text_exporter::PrometheusExporter;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use tonic::transport::ClientTlsConfig;

use super::OTEL_DEFAULT_RESOURCE;
use crate::cnf::{METRICS_ENABLED, TELEMETRY_DISABLE_METRICS, TELEMETRY_PROVIDER};

// Histogram buckets in milliseconds. Used by the OTLP push reader so existing
// dashboards over the legacy `*.duration` instruments stay compatible.
static HISTOGRAM_BUCKETS_MS: &[f64] = &[
	5.0, 10.0, 20.0, 50.0, 75.0, 100.0, 150.0, 200.0, 250.0, 300.0, 500.0, 750.0, 1000.0, 1500.0,
	2000.0, 2500.0, 5000.0, 10000.0, 15000.0, 30000.0,
];

// Histogram buckets in seconds. Mirrors `HISTOGRAM_BUCKETS_MS` but in seconds
// for the Prometheus text exposition (which prefers UCUM `s` units).
static HISTOGRAM_BUCKETS_SECONDS: &[f64] = &[
	0.005, 0.01, 0.02, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 5.0,
	10.0, 15.0, 30.0,
];

// Histogram buckets in bytes
const KB: f64 = 1024.0;
const MB: f64 = 1024.0 * KB;
const HISTOGRAM_BUCKETS_BYTES: &[f64] = &[
	1.0 * KB,
	2.0 * KB,
	5.0 * KB,
	10.0 * KB,
	100.0 * KB,
	500.0 * KB,
	1.0 * MB,
	2.5 * MB,
	5.0 * MB,
	10.0 * MB,
	25.0 * MB,
	50.0 * MB,
	100.0 * MB,
];

/// Returns whether the OTLP metrics push pipeline is configured.
pub fn otlp_metrics_active() -> bool {
	TELEMETRY_PROVIDER.trim().eq_ignore_ascii_case("otlp") && !*TELEMETRY_DISABLE_METRICS
}

/// Result of a successful [`init`] call: the unified meter provider plus
/// the optional Prometheus text exporter when the pull pipeline is
/// configured.
pub struct MetricsInit {
	/// Always-present unified meter provider. Carries every reader the
	/// configuration requested. Held by the
	/// [`ObservabilityRuntime`](crate::observe::ObservabilityRuntime) so
	/// observers can look up scoped meters off it.
	pub provider: SdkMeterProvider,
	/// Set when `SURREAL_METRICS_ENABLED=true`; threaded into the
	/// [`MetricsState`](crate::observe::MetricsState) attached to the
	/// `/metrics` Axum extension. The exporter is `Clone`, so the
	/// handler clones it on demand.
	pub prometheus_exporter: Option<PrometheusExporter>,
}

/// Build the unified [`SdkMeterProvider`] together with the optional
/// Prometheus text exporter.
///
/// The provider always carries the histogram-bucket views even when no
/// reader is configured: a no-reader provider is equivalent to a no-op
/// meter and lets observer construction stay branchless.
///
/// Returns `None` when neither pipeline is configured, which is the
/// caller's signal to fall back to [`SdkMeterProvider::default`] inside
/// the `ObservabilityRuntime`.
pub fn init() -> anyhow::Result<Option<MetricsInit>> {
	let prom_enabled = *METRICS_ENABLED;
	let otlp_enabled = otlp_metrics_active();
	if !prom_enabled && !otlp_enabled {
		return Ok(None);
	}

	let mut builder = SdkMeterProvider::builder()
		.with_resource(OTEL_DEFAULT_RESOURCE.clone())
		// Histogram bucket views drive both readers. Instrument-name suffix
		// (`.duration` / `.size`) selects the bucket family.
		.with_view(duration_seconds_view)
		.with_view(duration_ms_view)
		.with_view(size_bytes_view);

	let prometheus_exporter = if prom_enabled {
		// Build the Prometheus text exporter (also a `MetricReader`)
		// and attach it to the provider. The clone returned in
		// [`MetricsInit`] is what `MetricsState` carries into the
		// `/metrics` Axum extension; the original is consumed by the
		// builder so the provider keeps the reader alive for as long
		// as itself.
		let exporter = PrometheusExporter::builder()
			// Counters retain the `_total` suffix that operators alert on;
			// units stay so `*.duration` -> `*_seconds` and `*.size` ->
			// `*_bytes` come out cleanly.
			.build();
		builder = builder.with_reader(exporter.clone());
		Some(exporter)
	} else {
		None
	};

	if otlp_enabled {
		// Native TLS roots so that HTTPS OTLP collector endpoints work out of the box.
		let exporter = opentelemetry_otlp::MetricExporter::builder()
			.with_tonic()
			.with_tls_config(ClientTlsConfig::new().with_native_roots())
			.with_temporality(opentelemetry_sdk::metrics::Temporality::Cumulative)
			.build()?;
		// Use the async PeriodicReader backed by the Tokio runtime so that
		// metric exports don't block the calling thread. The OTel SDK's
		// env-var-aware defaults still apply: operators can tune push
		// frequency via `OTEL_METRIC_EXPORT_INTERVAL` (ms; 60 000 default).
		let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio).build();
		builder = builder.with_reader(reader);
	}

	Ok(Some(MetricsInit {
		provider: builder.build(),
		prometheus_exporter,
	}))
}

/// View that maps `*.duration` instruments measured in seconds to the
/// canonical second bucket boundaries. Selected by suffix on the instrument
/// name plus an explicit unit check so we do not collide with the
/// millisecond-scale views from the legacy OTLP RPC pipeline.
fn duration_seconds_view(instrument: &Instrument) -> Option<Stream> {
	let name_matches = instrument.name().ends_with(".duration");
	let is_seconds = instrument.unit().eq_ignore_ascii_case("s");
	if !name_matches || !is_seconds {
		return None;
	}
	Stream::builder()
		.with_aggregation(Aggregation::ExplicitBucketHistogram {
			boundaries: HISTOGRAM_BUCKETS_SECONDS.to_vec(),
			record_min_max: true,
		})
		.build()
		.ok()
}

/// View that maps `*.duration` instruments measured in milliseconds to the
/// legacy millisecond bucket boundaries. Used by the OTLP HTTP / RPC
/// pipelines.
fn duration_ms_view(instrument: &Instrument) -> Option<Stream> {
	let name_matches = instrument.name().ends_with(".duration");
	let is_milliseconds = instrument.unit().eq_ignore_ascii_case("ms");
	if !name_matches || !is_milliseconds {
		return None;
	}
	Stream::builder()
		.with_aggregation(Aggregation::ExplicitBucketHistogram {
			boundaries: HISTOGRAM_BUCKETS_MS.to_vec(),
			record_min_max: true,
		})
		.build()
		.ok()
}

/// View that maps `*.size` instruments measured in bytes to the canonical
/// byte bucket boundaries.
fn size_bytes_view(instrument: &Instrument) -> Option<Stream> {
	let name_matches = instrument.name().ends_with(".size");
	let is_bytes = {
		let unit = instrument.unit();
		unit.eq_ignore_ascii_case("by") || unit.eq_ignore_ascii_case("bytes")
	};
	if !name_matches || !is_bytes {
		return None;
	}
	Stream::builder()
		.with_aggregation(Aggregation::ExplicitBucketHistogram {
			boundaries: HISTOGRAM_BUCKETS_BYTES.to_vec(),
			record_min_max: true,
		})
		.build()
		.ok()
}
