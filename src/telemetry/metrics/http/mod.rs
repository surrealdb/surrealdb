pub(super) mod tower_layer;

use once_cell::sync::Lazy;
use opentelemetry::global;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::{Histogram, MetricsError, ObservableUpDownCounter, Unit};

use self::tower_layer::HttpCallMetricTracker;

static METER: Lazy<Meter> = Lazy::new(|| global::meter("surrealdb.http"));

pub static HTTP_SERVER_DURATION: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER
		.u64_histogram("http.server.duration")
		.with_description("The HTTP server duration in milliseconds.")
		.with_unit(Unit::new("ms"))
		.init()
});

pub static HTTP_SERVER_ACTIVE_REQUESTS: Lazy<ObservableUpDownCounter<i64>> = Lazy::new(|| {
	METER
		.i64_observable_up_down_counter("http.server.active_requests")
		.with_description("The number of active HTTP requests.")
		.init()
});

pub static HTTP_SERVER_REQUEST_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER
		.u64_histogram("http.server.request.size")
		.with_description("Measures the size of HTTP request messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

pub static HTTP_SERVER_RESPONSE_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER
		.u64_histogram("http.server.response.size")
		.with_description("Measures the size of HTTP response messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

fn observe_active_request(value: i64, tracker: &HttpCallMetricTracker) -> Result<(), MetricsError> {
	let attrs = tracker.active_req_attrs();

	METER.register_callback(&[HTTP_SERVER_ACTIVE_REQUESTS.as_any()], move |o| {
		o.observe_i64(&HTTP_SERVER_ACTIVE_REQUESTS.clone(), value, &attrs)
	})?;

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
