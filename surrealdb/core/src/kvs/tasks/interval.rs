use std::pin::Pin;
use std::task::{Context, Poll};

use common::time::{Instant, Interval};
use futures::Stream;

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
