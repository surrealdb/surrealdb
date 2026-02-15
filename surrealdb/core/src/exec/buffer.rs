//! Operator pipeline buffering.
//!
//! Wraps a [`ValueBatchStream`] in a prefetch buffer that eagerly polls the
//! inner stream and caches ready results. This smooths out batch delivery and
//! extends the benefit of the Scanner's KV-level prefetch up through the
//! operator chain.
//!
//! The approach is cooperative (single-task): when the buffer stream is polled,
//! it greedily drains all immediately-ready items from the inner stream into
//! an internal ring buffer, then returns the oldest buffered item. This avoids
//! spawning separate tasks — and the associated lifecycle/cleanup issues —
//! while still reducing the number of poll round-trips through the operator
//! tree.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::exec::{ValueBatch, ValueBatchStream};
use crate::expr::FlowResult;

/// Wrap a [`ValueBatchStream`] in a prefetch buffer.
///
/// The returned stream eagerly polls the inner stream whenever it is polled
/// itself, buffering up to `SURREAL_OPERATOR_BUFFER_SIZE` ready items. This
/// ensures that any data the inner stream can produce without blocking (e.g.
/// from the Scanner's KV prefetch) is consumed immediately and queued for the
/// parent operator.
///
/// When `SURREAL_OPERATOR_BUFFER_SIZE` is 0, returns the stream unchanged.
///
/// On WASM targets, returns the stream unchanged (no buffering overhead in a
/// single-threaded environment where there is nothing to overlap with).
#[cfg(not(target_family = "wasm"))]
pub(crate) fn buffer_stream(stream: ValueBatchStream) -> ValueBatchStream {
	let buffer_size = *crate::cnf::OPERATOR_BUFFER_SIZE;
	if buffer_size == 0 {
		return stream;
	}
	Box::pin(PrefetchStream {
		inner: stream,
		buffer: VecDeque::with_capacity(buffer_size),
		buffer_size,
		exhausted: false,
	})
}

/// WASM: no-op — single-threaded runtime has nothing to overlap with.
#[cfg(target_family = "wasm")]
pub(crate) fn buffer_stream(stream: ValueBatchStream) -> ValueBatchStream {
	stream
}

// ---------------------------------------------------------------------------
// PrefetchStream — cooperative eager-poll buffer
// ---------------------------------------------------------------------------

/// A stream wrapper that eagerly polls its inner stream and buffers ready
/// results, reducing poll round-trips through the operator tree.
///
/// On each poll:
/// 1. Greedily drain all immediately-ready items from the inner stream into the buffer (up to
///    `buffer_size`).
/// 2. Return the oldest buffered item, if any.
/// 3. If the buffer is empty and the inner stream is pending, return pending.
///
/// This is purely cooperative — no tasks are spawned, so all lifecycle and
/// cancellation semantics are preserved. The inner stream is owned directly
/// and dropped synchronously when this wrapper is dropped.
#[cfg(not(target_family = "wasm"))]
struct PrefetchStream {
	/// The wrapped inner stream.
	inner: ValueBatchStream,
	/// Ring buffer of prefetched items.
	buffer: VecDeque<FlowResult<ValueBatch>>,
	/// Maximum number of items to buffer ahead.
	buffer_size: usize,
	/// Whether the inner stream has been exhausted.
	exhausted: bool,
}

#[cfg(not(target_family = "wasm"))]
impl PrefetchStream {
	/// Eagerly poll the inner stream, filling the buffer with any
	/// immediately-ready items (up to `buffer_size`).
	#[inline]
	fn fill_buffer(&mut self, cx: &mut Context<'_>) {
		while self.buffer.len() < self.buffer_size && !self.exhausted {
			match self.inner.as_mut().poll_next(cx) {
				Poll::Ready(Some(item)) => self.buffer.push_back(item),
				Poll::Ready(None) => {
					self.exhausted = true;
				}
				Poll::Pending => break,
			}
		}
	}
}

#[cfg(not(target_family = "wasm"))]
impl Stream for PrefetchStream {
	type Item = FlowResult<ValueBatch>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();

		// Eagerly fill the buffer with any ready items from the inner stream.
		this.fill_buffer(cx);

		// Return the oldest buffered item.
		if let Some(item) = this.buffer.pop_front() {
			// After consuming an item, try to refill the vacated slot so
			// the buffer stays as full as possible for the next poll.
			this.fill_buffer(cx);
			Poll::Ready(Some(item))
		} else if this.exhausted {
			Poll::Ready(None)
		} else {
			// Buffer empty and inner stream pending — nothing to return yet.
			Poll::Pending
		}
	}
}
