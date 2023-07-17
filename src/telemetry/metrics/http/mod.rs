pub(super) mod tower_layer;

use once_cell::sync::Lazy;
use opentelemetry::{
	metrics::{Histogram, Meter, MeterProvider, ObservableUpDownCounter, Unit},
	runtime,
	sdk::{
		export::metrics::aggregation,
		metrics::{
			controllers::{self, BasicController},
			processors, selectors,
		},
	},
	Context,
};
use opentelemetry_otlp::MetricsExporterBuilder;

use crate::telemetry::OTEL_DEFAULT_RESOURCE;

// Histogram buckets in milliseconds
static HTTP_DURATION_MS_HISTOGRAM_BUCKETS: &[f64] = &[
	5.0, 10.0, 20.0, 50.0, 75.0, 100.0, 150.0, 200.0, 250.0, 300.0, 500.0, 750.0, 1000.0, 1500.0,
	2000.0, 2500.0, 5000.0, 10000.0, 15000.0, 30000.0,
];

const KB: f64 = 1024.0;
const MB: f64 = 1024.0 * KB;

const HTTP_SIZE_HISTOGRAM_BUCKETS: &[f64] = &[
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

static METER_PROVIDER_HTTP_DURATION: Lazy<BasicController> = Lazy::new(|| {
	let exporter = MetricsExporterBuilder::from(opentelemetry_otlp::new_exporter().tonic())
		.build_metrics_exporter(Box::new(aggregation::cumulative_temporality_selector()))
		.unwrap();

	let builder = controllers::basic(processors::factory(
		selectors::simple::histogram(HTTP_DURATION_MS_HISTOGRAM_BUCKETS),
		aggregation::cumulative_temporality_selector(),
	))
	.with_exporter(exporter)
	.with_resource(OTEL_DEFAULT_RESOURCE.clone());

	let controller = builder.build();
	controller.start(&Context::current(), runtime::Tokio).unwrap();
	controller
});

static METER_PROVIDER_HTTP_SIZE: Lazy<BasicController> = Lazy::new(|| {
	let exporter = MetricsExporterBuilder::from(opentelemetry_otlp::new_exporter().tonic())
		.build_metrics_exporter(Box::new(aggregation::cumulative_temporality_selector()))
		.unwrap();

	let builder = controllers::basic(processors::factory(
		selectors::simple::histogram(HTTP_SIZE_HISTOGRAM_BUCKETS),
		aggregation::cumulative_temporality_selector(),
	))
	.with_exporter(exporter)
	.with_resource(OTEL_DEFAULT_RESOURCE.clone());

	let controller = builder.build();
	controller.start(&Context::current(), runtime::Tokio).unwrap();
	controller
});

static HTTP_DURATION_METER: Lazy<Meter> =
	Lazy::new(|| METER_PROVIDER_HTTP_DURATION.meter("http_duration"));
static HTTP_SIZE_METER: Lazy<Meter> = Lazy::new(|| METER_PROVIDER_HTTP_SIZE.meter("http_size"));

pub static HTTP_SERVER_DURATION: Lazy<Histogram<u64>> = Lazy::new(|| {
	HTTP_DURATION_METER
		.u64_histogram("http.server.duration")
		.with_description("The HTTP server duration in milliseconds.")
		.with_unit(Unit::new("ms"))
		.init()
});

pub static HTTP_SERVER_ACTIVE_REQUESTS: Lazy<ObservableUpDownCounter<i64>> = Lazy::new(|| {
	HTTP_DURATION_METER
		.i64_observable_up_down_counter("http.server.active_requests")
		.with_description("The number of active HTTP requests.")
		.init()
});

pub static HTTP_SERVER_REQUEST_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	HTTP_SIZE_METER
		.u64_histogram("http.server.request.size")
		.with_description("Measures the size of HTTP request messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

pub static HTTP_SERVER_RESPONSE_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	HTTP_SIZE_METER
		.u64_histogram("http.server.response.size")
		.with_description("Measures the size of HTTP response messages.")
		.with_unit(Unit::new("mb"))
		.init()
});
