pub mod http;
pub mod ws;

use std::time::Duration;

use once_cell::sync::Lazy;
use opentelemetry::global::{self, ObjectSafeMeterProvider};
use opentelemetry::Context as TelemetryContext;
use opentelemetry::{
	metrics::{Meter, MeterProvider, MetricsError},
	// sdk::{
	// 	export::metrics::aggregation,
	// 	metrics::{
	// 		controllers::{self, BasicController},
	// 		processors, selectors,
	// 	},
	// },
};
use opentelemetry_otlp::MetricsExporterBuilder;
use opentelemetry_sdk::metrics::data::Temporality;
use opentelemetry_sdk::metrics::reader::{DefaultAggregationSelector, DefaultTemporalitySelector};
use opentelemetry_sdk::metrics::{
	Aggregation, Instrument, MeterProviderBuilder, PeriodicReader, PeriodicReaderBuilder,
	SdkMeterProvider, Stream,
};
// use opentelemetry_sdk::export;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::{export, metrics};

pub use self::http::tower_layer::HttpMetricsLayer;
use self::ws::observe_active_connection;

use super::OTEL_DEFAULT_RESOURCE;

// Histogram buckets in milliseconds
static HISTOGRAM_BUCKETS_MS: &[f64] = &[
	5.0, 10.0, 20.0, 50.0, 75.0, 100.0, 150.0, 200.0, 250.0, 300.0, 500.0, 750.0, 1000.0, 1500.0,
	2000.0, 2500.0, 5000.0, 10000.0, 15000.0, 30000.0,
];

// Histogram buckets in bytes
const KB: f64 = 1024.0;
const MB: f64 = 1024.0 * KB;
const HISTOGRAM_BUCKETS_BYTES: &[f64] = &[
	1.0 * KB,   // 1 KB
	2.0 * KB,   // 2 KB
	5.0 * KB,   // 5 KB
	10.0 * KB,  // 10 KB
	100.0 * KB, // 100 KB
	500.0 * KB, // 500 KB
	1.0 * MB,   // 1 MB
	2.5 * MB,   // 2 MB
	5.0 * MB,   // 5 MB
	10.0 * MB,  // 10 MB
	25.0 * MB,  // 25 MB
	50.0 * MB,  // 50 MB
	100.0 * MB, // 100 MB
];

fn build_controller() -> Result<SdkMeterProvider, MetricsError> {
	let exporter = MetricsExporterBuilder::from(opentelemetry_otlp::new_exporter().tonic())
		.build_metrics_exporter(
			Box::new(DefaultTemporalitySelector::new()),
			Box::new(DefaultAggregationSelector::new()),
		)
		.unwrap();
	let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();

	let histo_duration_view = {
		let criteria = Instrument::new().name("*.duration");
		let mask = Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
			boundaries: HISTOGRAM_BUCKETS_MS.to_vec(),
			record_min_max: true,
		});
		opentelemetry_sdk::metrics::new_view(criteria, mask)?
	};

	let histo_size_view = {
		let criteria = Instrument::new().name("*.size");
		let mask = Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
			boundaries: HISTOGRAM_BUCKETS_BYTES.to_vec(),
			record_min_max: true,
		});
		opentelemetry_sdk::metrics::new_view(criteria, mask)?
	};

	Ok(SdkMeterProvider::builder()
		.with_reader(reader)
		.with_resource(OTEL_DEFAULT_RESOURCE.clone())
		.with_view(histo_duration_view)
		.with_view(histo_size_view)
		.build())
}

// Initialize the metrics subsystem
// Panics if initialization fails
pub fn init() -> Result<(), MetricsError> {
	let meter_provider = build_controller()?;

	global::set_meter_provider(meter_provider);
	Ok(())
}

//
// Shutdown the metrics providers
//
pub fn shutdown(_cx: &TelemetryContext) -> Result<(), MetricsError> {
	// TODO(sgirones): The stop method hangs forever, so we are not calling it until we figure out why
	// METER_PROVIDER_DURATION.stop(cx)?;
	// METER_PROVIDER_SIZE.stop(cx)?;

	Ok(())
}
