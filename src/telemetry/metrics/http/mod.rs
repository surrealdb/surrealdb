pub(super) mod tower_layer;

use std::sync::LazyLock;
use opentelemetry::metrics::{Histogram, MetricsError, Unit, UpDownCounter};
use opentelemetry::Context as TelemetryContext;

use self::tower_layer::HttpCallMetricTracker;

use super::{METER_DURATION, METER_SIZE};

pub static HTTP_SERVER_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER_DURATION
		.u64_histogram("http.server.duration")
		.with_description("The HTTP server duration in milliseconds.")
		.with_unit(Unit::new("ms"))
		.init()
});

pub static HTTP_SERVER_ACTIVE_REQUESTS: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
	METER_DURATION
		.i64_up_down_counter("http.server.active_requests")
		.with_description("The number of active HTTP requests.")
		.init()
});

pub static HTTP_SERVER_REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER_SIZE
		.u64_histogram("http.server.request.size")
		.with_description("Measures the size of HTTP request messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

pub static HTTP_SERVER_RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER_SIZE
		.u64_histogram("http.server.response.size")
		.with_description("Measures the size of HTTP response messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

fn observe_active_request(value: i64, tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	let attrs = tracker.active_req_attrs();

	HTTP_SERVER_ACTIVE_REQUESTS.add(&TelemetryContext::current(), value, &attrs);
	Ok(())
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
