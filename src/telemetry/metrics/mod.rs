pub mod http;
pub mod ws;

use std::time::Duration;

use once_cell::sync::Lazy;
use opentelemetry::Context as TelemetryContext;
use opentelemetry::{
	metrics::{Meter, MetricsError},
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
// use opentelemetry_sdk::export;
use opentelemetry_sdk::runtime;

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

fn build_controller(boundaries: &'static [f64]) -> BasicController {
	let exporter = MetricsExporterBuilder::from(opentelemetry_otlp::new_exporter().tonic())
		.build_metrics_exporter(Box::new(Temporality::Cumulative), todo!())
		.unwrap();

	let builder = controllers::basic(processors::factory(
		selectors::simple::histogram(boundaries),
		aggregation::cumulative_temporality_selector(),
	))
	.with_push_timeout(Duration::from_secs(5))
	.with_collect_period(Duration::from_secs(5))
	.with_exporter(exporter)
	.with_resource(OTEL_DEFAULT_RESOURCE.clone());

	builder.build()
}

static METER_PROVIDER_DURATION: Lazy<BasicController> =
	Lazy::new(|| build_controller(HISTOGRAM_BUCKETS_MS));

static METER_PROVIDER_SIZE: Lazy<BasicController> =
	Lazy::new(|| build_controller(HISTOGRAM_BUCKETS_BYTES));

static METER_DURATION: Lazy<Meter> = Lazy::new(|| METER_PROVIDER_DURATION.meter("duration"));
static METER_SIZE: Lazy<Meter> = Lazy::new(|| METER_PROVIDER_SIZE.meter("size"));

/// Initialize the metrics subsystem
pub fn init(cx: &TelemetryContext) -> Result<(), MetricsError> {
	METER_PROVIDER_DURATION.start(cx, runtime::Tokio)?;
	METER_PROVIDER_SIZE.start(cx, runtime::Tokio)?;

	observe_active_connection(0)?;

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
