//! Different embedded and remote database engines

pub mod any;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
))]
pub mod local;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use futures::Stream;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Instant;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Interval;
#[cfg(target_arch = "wasm32")]
use wasmtimer::std::Instant;
#[cfg(target_arch = "wasm32")]
use wasmtimer::tokio::Interval;

// used in http and all local engines.
#[allow(dead_code)]
fn value_to_values(v: Value) -> Values {
	match v {
		Value::Array(x) => {
			let mut values = Values::default();
			values.0 = x.0;
			values
		}
		x => {
			let mut values = Values::default();
			values.0 = vec![x];
			values
		}
	}
}

struct IntervalStream {
	inner: Interval,
}

impl IntervalStream {
	#[allow(unused)]
	fn new(interval: Interval) -> Self {
		Self {
			inner: interval,
		}
	}
}

impl Stream for IntervalStream {
	type Item = Instant;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Instant>> {
		self.inner.poll_tick(cx).map(Some)
	}
}
