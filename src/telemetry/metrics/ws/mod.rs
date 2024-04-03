use std::time::Instant;

use once_cell::sync::Lazy;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::{Histogram, MetricsError, ObservableUpDownCounter, Unit};
use opentelemetry::{global, Context, KeyValue};

static METER: Lazy<Meter> = Lazy::new(|| global::meter("surrealdb.rpc"));

pub static RPC_SERVER_DURATION: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER
		.u64_histogram("rpc.server.duration")
		.with_description("Measures duration of inbound RPC requests in milliseconds.")
		.with_unit(Unit::new("ms"))
		.init()
});

pub static RPC_SERVER_ACTIVE_CONNECTIONS: Lazy<ObservableUpDownCounter<i64>> = Lazy::new(|| {
	METER
		.i64_observable_up_down_counter("rpc.server.active_connections")
		.with_description("The number of active WebSocket connections.")
		.init()
});

pub static RPC_SERVER_REQUEST_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER
		.u64_histogram("rpc.server.request.size")
		.with_description("Measures the size of HTTP request messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

pub static RPC_SERVER_RESPONSE_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
	METER
		.u64_histogram("rpc.server.response.size")
		.with_description("Measures the size of HTTP response messages.")
		.with_unit(Unit::new("mb"))
		.init()
});

fn otel_common_attrs() -> Vec<KeyValue> {
	vec![KeyValue::new("rpc.service", "surrealdb")]
}

/// Registers the callback that increases the number of active RPC connections.
pub fn on_connect() -> Result<(), MetricsError> {
	observe_active_connection(1)
}

/// Registers the callback that increases the number of active RPC connections.
pub fn on_disconnect() -> Result<(), MetricsError> {
	observe_active_connection(-1)
}

pub(super) fn observe_active_connection(value: i64) -> Result<(), MetricsError> {
	let attrs = otel_common_attrs();

	METER.register_callback(&[RPC_SERVER_ACTIVE_CONNECTIONS.as_any()], move |o| {
		o.observe_i64(&RPC_SERVER_ACTIVE_CONNECTIONS.clone(), value, &attrs)
	})?;

	Ok(())
}

//
// Record an RPC command
//

#[derive(Clone, Debug, PartialEq)]
pub struct RequestContext {
	start: Instant,
	pub method: String,
	pub size: usize,
}

impl Default for RequestContext {
	fn default() -> Self {
		Self {
			start: Instant::now(),
			method: "unknown".to_string(),
			size: 0,
		}
	}
}

impl RequestContext {
	pub fn with_method(self, method: &str) -> Self {
		Self {
			method: method.to_string(),
			..self
		}
	}

	pub fn with_size(self, size: usize) -> Self {
		Self {
			size,
			..self
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct NotificationContext {
	pub live_id: String,
}

impl Default for NotificationContext {
	fn default() -> Self {
		Self {
			live_id: "unknown".to_string(),
		}
	}
}

impl NotificationContext {
	pub fn with_live_id(self, live_id: String) -> Self {
		Self {
			live_id,
		}
	}
}

/// Updates the request and response metrics for an RPC method.
pub fn record_rpc(res_size: usize, is_error: bool) {
	let mut attrs = otel_common_attrs();
	let mut duration = 0;
	let mut req_size = 0;

	let cx = Context::current();

	if let Some(cx) = cx.get::<RequestContext>() {
		attrs.extend_from_slice(&[
			KeyValue::new("rpc.method", cx.method.clone()),
			KeyValue::new("rpc.error", is_error),
		]);
		duration = cx.start.elapsed().as_millis() as u64;
		req_size = cx.size as u64;
	} else if let Some(cx) = cx.get::<NotificationContext>() {
		attrs.extend_from_slice(&[
			KeyValue::new("rpc.method", "notification"),
			KeyValue::new("rpc.error", is_error),
			KeyValue::new("rpc.live_id", cx.live_id.clone()),
		]);
	} else {
		// If a bug causes the RequestContent to be empty, we still want to record the metrics to avoid a silent failure.
		warn!(
			"record_rpc: no request context found, this is a bug. Resulting metrics will have invalid attributes"
		);
		attrs.extend_from_slice(&[
			KeyValue::new("rpc.method", "unknown"),
			KeyValue::new("rpc.error", is_error),
		]);
	};

	RPC_SERVER_DURATION.record(duration, &attrs);
	RPC_SERVER_REQUEST_SIZE.record(req_size, &attrs);
	RPC_SERVER_RESPONSE_SIZE.record(res_size as u64, &attrs);
}
