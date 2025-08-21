pub mod http;
pub mod ws;

use opentelemetry::metrics::MetricsError;
use opentelemetry_otlp::MetricsExporterBuilder;
use opentelemetry_sdk::metrics::reader::{DefaultAggregationSelector, DefaultTemporalitySelector};
use opentelemetry_sdk::metrics::{
	Aggregation, Instrument, PeriodicReader, SdkMeterProvider, Stream,
};
use opentelemetry_sdk::runtime;

pub use self::http::tower_layer::HttpMetricsLayer;
use super::OTEL_DEFAULT_RESOURCE;
use crate::cnf::{TELEMETRY_DISABLE_METRICS, TELEMETRY_PROVIDER};

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

// Returns a metrics configuration based on the SURREAL_TELEMETRY_PROVIDER
// environment variable
pub fn init() -> Result<Option<SdkMeterProvider>, MetricsError> {
	match TELEMETRY_PROVIDER.trim() {
		// The OTLP telemetry provider has been specified
		s if s.eq_ignore_ascii_case("otlp") && !*TELEMETRY_DISABLE_METRICS => {
			// Create a new metrics exporter using tonic
			let exporter = MetricsExporterBuilder::from(opentelemetry_otlp::new_exporter().tonic())
				.build_metrics_exporter(
					Box::new(DefaultTemporalitySelector::new()),
					Box::new(DefaultAggregationSelector::new()),
				)
				.unwrap();
			// Create the reader to run with Tokio
			let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();
			// Add a view for metering durations
			let histogram_duration_view = {
				let criteria = Instrument::new().name("*.duration");
				let mask = Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
					boundaries: HISTOGRAM_BUCKETS_MS.to_vec(),
					record_min_max: true,
				});
				opentelemetry_sdk::metrics::new_view(criteria, mask)?
			};
			// Add a view for metering sizes
			let histogram_size_view = {
				let criteria = Instrument::new().name("*.size");
				let mask = Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
					boundaries: HISTOGRAM_BUCKETS_BYTES.to_vec(),
					record_min_max: true,
				});
				opentelemetry_sdk::metrics::new_view(criteria, mask)?
			};
			// Create the new metrics provider
			Ok(Some(
				SdkMeterProvider::builder()
					.with_reader(reader)
					.with_resource(OTEL_DEFAULT_RESOURCE.clone())
					.with_view(histogram_duration_view)
					.with_view(histogram_size_view)
					.build(),
			))
		}
		// No matching telemetry provider was found
		_ => Ok(None),
	}
}
