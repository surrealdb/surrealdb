pub(super) mod tower_layer;

use opentelemetry::global;
use opentelemetry::metrics::{Histogram, Meter, MetricsError, UpDownCounter};
use std::sync::LazyLock;

use self::tower_layer::HttpCallMetricTracker;

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("surrealdb.http"));

pub static HTTP_SERVER_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER
		.u64_histogram("http.server.duration")
		.with_description("The HTTP server duration in milliseconds.")
		.with_unit("ms")
		.init()
});

pub static HTTP_SERVER_ACTIVE_REQUESTS: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
	METER
		.i64_up_down_counter("http.server.active_requests")
		.with_description("The number of active HTTP requests.")
		.init()
});

pub static HTTP_SERVER_REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER
		.u64_histogram("http.server.request.size")
		.with_description("Measures the size of HTTP request messages.")
		.with_unit("mb")
		.init()
});

pub static HTTP_SERVER_RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER
		.u64_histogram("http.server.response.size")
		.with_description("Measures the size of HTTP response messages.")
		.with_unit("mb")
		.init()
});

fn observe_active_request(value: i64, tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	let attrs = tracker.active_req_attrs();

	HTTP_SERVER_ACTIVE_REQUESTS.add(value, &attrs);
	Ok(())
}

fn record_request_duration(tracker: &HttpCallMetricTracker) {
	// Record the duration of the request.
	HTTP_SERVER_DURATION
		.record(tracker.duration().as_millis() as u64, &tracker.request_duration_attrs());
}

fn record_request_size(tracker: &HttpCallMetricTracker, size: u64) {
	HTTP_SERVER_REQUEST_SIZE.record(size, &tracker.request_size_attrs());
}

fn record_response_size(tracker: &HttpCallMetricTracker, size: u64) {
	HTTP_SERVER_RESPONSE_SIZE.record(size, &tracker.response_size_attrs());
}
