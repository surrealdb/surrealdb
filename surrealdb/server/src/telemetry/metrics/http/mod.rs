pub(super) mod tower_layer;

use std::sync::LazyLock;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter, MetricsError, UpDownCounter};

use self::tower_layer::HttpCallMetricTracker;

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("surrealdb.http"));

pub static HTTP_SERVER_ACTIVE_REQUESTS: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
	METER
		.i64_up_down_counter("http.server.active_requests")
		.with_description("The number of active HTTP requests.")
		.init()
});

pub static HTTP_SERVER_REQUEST_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
	METER
		.u64_counter("http.server.request.count")
		.with_description("The total number of HTTP requests processed.")
		.init()
});

pub static HTTP_SERVER_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER
		.u64_histogram("http.server.request.duration")
		.with_description("The duration of inbound HTTP requests in milliseconds.")
		.with_unit("ms")
		.init()
});

pub static HTTP_SERVER_REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER
		.u64_histogram("http.server.request.size")
		.with_description("The size of inbound HTTP request messages.")
		.with_unit("mb")
		.init()
});

pub static HTTP_SERVER_RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
	METER
		.u64_histogram("http.server.response.size")
		.with_description("The size of outbound HTTP response messages.")
		.with_unit("mb")
		.init()
});

fn observe_active_request(value: i64, tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	let attrs = tracker.active_req_attrs();
	HTTP_SERVER_ACTIVE_REQUESTS.add(value, &attrs);
	Ok(())
}

fn record_request_duration(tracker: &HttpCallMetricTracker) {
	HTTP_SERVER_REQUEST_DURATION
		.record(tracker.duration().as_millis() as u64, &tracker.request_duration_attrs());
}

fn record_request_count(tracker: &HttpCallMetricTracker) {
	HTTP_SERVER_REQUEST_COUNT.add(1, &tracker.request_duration_attrs());
}

fn record_request_size(tracker: &HttpCallMetricTracker, size: u64) {
	HTTP_SERVER_REQUEST_SIZE.record(size, &tracker.request_size_attrs());
}

fn record_response_size(tracker: &HttpCallMetricTracker, size: u64) {
	HTTP_SERVER_RESPONSE_SIZE.record(size, &tracker.response_size_attrs());
}
