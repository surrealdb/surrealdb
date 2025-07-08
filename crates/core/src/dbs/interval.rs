#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

pub(in crate::dbs) struct IntervalStream {
	inner: Interval,
}

impl IntervalStream {
	pub(in crate::dbs) fn new(interval: Interval) -> Self {
		Self {
			inner: interval,
		}
	}
}

impl futures::Stream for IntervalStream {
	type Item = Instant;

	fn poll_next(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Instant>> {
		self.inner.poll_tick(cx).map(Some)
	}
}
