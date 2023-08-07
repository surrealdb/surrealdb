pub(super) mod tower_layer;

use once_cell::sync::Lazy;
use opentelemetry::metrics::{Histogram, MetricsError, ObservableUpDownCounter, Unit};
use opentelemetry::Context as TelemetryContext;

use self::tower_layer::HttpCallMetricTracker;

use super::{METER_DURATION, METER_SIZE};

pub static HTTP_SERVER_DURATION: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER_DURATION
		.u64_histogram("http.server.duration")
		.with_description("The HTTP server duration in milliseconds.")
		.with_unit(Unit::new("ms"))
		.init()
});

pub static HTTP_SERVER_ACTIVE_REQUESTS: Lazy<ObservableUpDownCounter<i64>> = Lazy::new(|| {
	METER_DURATION
		.i64_observable_up_down_counter("http.server.active_requests")
		.with_description("The number of active HTTP requests.")
		.init()
});

pub static HTTP_SERVER_REQUEST_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER_SIZE
		.u64_histogram("http.server.request.size")
		.with_description("Measures the size of HTTP request messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

pub static HTTP_SERVER_RESPONSE_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER_SIZE
		.u64_histogram("http.server.response.size")
		.with_description("Measures the size of HTTP response messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

fn observe_request_start(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	observe_active_request(1, tracker)
}

fn observe_request_finish(tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	observe_active_request(-1, tracker)
}

fn observe_active_request(value: i64, tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	let attrs = tracker.active_req_attrs();

	METER_DURATION
		.register_callback(move |ctx| HTTP_SERVER_ACTIVE_REQUESTS.observe(ctx, value, &attrs))
}

fn record_request_duration(tracker: &HttpCallMetricTracker) {
	// Record the duration of the request.
	HTTP_SERVER_DURATION.record(
		&TelemetryContext::current(),
		tracker.duration().as_millis() as u64,
		&tracker.request_duration_attrs(),
	);
}

fn record_request_size(tracker: &HttpCallMetricTracker, size: u64) {
	HTTP_SERVER_REQUEST_SIZE.record(
		&TelemetryContext::current(),
		size,
		&tracker.request_size_attrs(),
	);
}

fn record_response_size(tracker: &HttpCallMetricTracker, size: u64) {
	HTTP_SERVER_RESPONSE_SIZE.record(
		&TelemetryContext::current(),
		size,
		&tracker.response_size_attrs(),
	);
}
