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
#[cfg(any(feature = "protocol-http", feature = "protocol-ws", feature = "protocol-grpc"))]
pub mod remote;
/// The embedded engine's background maintenance tasks.
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
))]
#[doc(hidden)]
pub use surrealdb_engine_local::tasks;

/// Compiled only where something polls on an interval: the WS ping loop.
#[cfg(feature = "protocol-ws")]
mod interval {
	use std::pin::Pin;
	use std::task::{Context, Poll};

	use futures::Stream;
	#[cfg(not(target_family = "wasm"))]
	use tokio::time::{Instant, Interval};
	#[cfg(target_family = "wasm")]
	use wasmtimer::std::Instant;
	#[cfg(target_family = "wasm")]
	use wasmtimer::tokio::Interval;

	pub(crate) struct IntervalStream {
		inner: Interval,
	}

	impl IntervalStream {
		pub(crate) fn new(interval: Interval) -> Self {
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
}
#[cfg(feature = "protocol-ws")]
use interval::IntervalStream;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub(crate) use surrealdb_engine_api::{SessionError, session_error_to_error};
