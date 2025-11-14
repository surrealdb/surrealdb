//! Different embedded and remote database engines

pub mod any;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
))]
pub mod local;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

struct IntervalStream {
	inner: Interval,
}

impl IntervalStream {
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
