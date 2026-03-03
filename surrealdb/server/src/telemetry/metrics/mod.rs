pub mod ds;
pub mod http;
pub mod ws;

use std::sync::OnceLock;

use opentelemetry_sdk::metrics::{
	Aggregation, Instrument, PeriodicReader, SdkMeterProvider, Stream,
};

pub use self::http::tower_layer::HttpMetricsLayer;
use super::OTEL_DEFAULT_RESOURCE;
use crate::cnf::TelemetryConfig;

static TELEMETRY_NAMESPACE: OnceLock<Option<String>> = OnceLock::new();

/// Returns the configured telemetry namespace, if any.
pub fn telemetry_namespace() -> Option<&'static String> {
	TELEMETRY_NAMESPACE.get().and_then(|o| o.as_ref())
}

// Histogram buckets in milliseconds
static HISTOGRAM_BUCKETS_MS: &[f64] = &[
	5.0, 10.0, 20.0, 50.0, 75.0, 100.0, 150.0, 200.0, 250.0, 300.0, 500.0, 750.0, 1000.0, 1500.0,
	2000.0, 2500.0, 5000.0, 10000.0, 15000.0, 30000.0,
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

// Returns a metrics configuration based on the telemetry config
pub fn init(telemetry: &TelemetryConfig) -> anyhow::Result<Option<SdkMeterProvider>> {
	let _ = TELEMETRY_NAMESPACE.set(telemetry.namespace.clone());
	match telemetry.provider.trim() {
		// The OTLP telemetry provider has been specified
		s if s.eq_ignore_ascii_case("otlp") && !telemetry.disable_metrics => {
			// Create a new metrics exporter using OTLP with tonic transport
			let exporter = opentelemetry_otlp::MetricExporter::builder()
				.with_tonic()
				.with_temporality(opentelemetry_sdk::metrics::Temporality::Cumulative)
				.build()?;
			let reader = PeriodicReader::builder(exporter)
				.with_interval(std::time::Duration::from_secs(60))
				.build();
			// Create view for histogram durations with custom buckets
			let duration_view = |instrument: &Instrument| -> Option<Stream> {
				if instrument.name().ends_with(".duration") {
					Stream::builder()
						.with_aggregation(Aggregation::ExplicitBucketHistogram {
							boundaries: HISTOGRAM_BUCKETS_MS.to_vec(),
							record_min_max: true,
						})
						.build()
						.ok()
				} else {
					None
				}
			};
			// Create view for histogram sizes with custom buckets
			let size_view = |instrument: &Instrument| -> Option<Stream> {
				if instrument.name().ends_with(".size") {
					Stream::builder()
						.with_aggregation(Aggregation::ExplicitBucketHistogram {
							boundaries: HISTOGRAM_BUCKETS_BYTES.to_vec(),
							record_min_max: true,
						})
						.build()
						.ok()
				} else {
					None
				}
			};
			// Create the new metrics provider
			Ok(Some(
				SdkMeterProvider::builder()
					.with_reader(reader)
					.with_resource(OTEL_DEFAULT_RESOURCE.clone())
					.with_view(duration_view)
					.with_view(size_view)
					.build(),
			))
		}
		// No matching telemetry provider was found
		_ => Ok(None),
	}
}
