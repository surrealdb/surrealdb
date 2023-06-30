pub(super) mod tower_layer;

use opentelemetry::{metrics::{Meter, Unit, Histogram, ObservableUpDownCounter, MeterProvider}, sdk::{metrics::{controllers::BasicController, selectors}, export::metrics::aggregation}, runtime::{self, Tokio}, Context};
use once_cell::sync::Lazy;

use crate::telemetry::OTEL_DEFAULT_RESOURCE;

// Histogram buckets in milliseconds
static HISTOGRAM_BOUNDARIES_MS: &[f64] = &[
    5.0, 10.0, 20.0, 50.0, 75.0, 100.0,
    150.0, 200.0, 250.0, 300.0, 500.0, 750.0, 1000.0,
    1500.0, 2000.0, 2500.0, 5000.0, 10000.0, 15000.0, 30000.0
];

static METER_PROVIDER: Lazy<BasicController> = Lazy::new(|| {
    let res = opentelemetry_otlp::new_pipeline().metrics(selectors::simple::histogram(HISTOGRAM_BOUNDARIES_MS), aggregation::cumulative_temporality_selector(), runtime::Tokio)
		.with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_resource(OTEL_DEFAULT_RESOURCE.clone())
        .build().unwrap();

    res.start(&Context::current(), Tokio).unwrap();
    res
});

static HTTP_METER: Lazy<Meter> = Lazy::new(|| METER_PROVIDER.meter("http"));

pub static HTTP_SERVER_DURATION: Lazy<Histogram<u64>> = Lazy::new(|| {
    HTTP_METER
        .u64_histogram("http.server.duration")
        .with_description("The HTTP server duration in milliseconds.")
        .with_unit(Unit::new("ms"))
        .init()
});

pub static HTTP_SERVER_ACTIVE_REQUESTS: Lazy<ObservableUpDownCounter<i64>> = Lazy::new(|| {
    HTTP_METER
        .i64_observable_up_down_counter("http.server.active_requests")
        .with_description("The number of active HTTP requests.")
        .init()
});
