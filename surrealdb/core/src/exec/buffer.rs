//! Operator pipeline buffering.
//!
//! Provides two buffering strategies for inter-operator pipelining:
//!
//! - **`SpawnedBufferedStream`** (parallel): Runs the inner stream in a separate tokio task with a
//!   bounded channel, giving true pipeline parallelism. Used for read-only operator chains where no
//!   mutation ordering constraints exist.
//!
//! - **`PrefetchStream`** (cooperative): Eagerly polls the inner stream on the same task and caches
//!   ready results. Used for read-write operator chains where mutation side-effects must stay
//!   sequential.
//!
//! The public entry point [`buffer_stream`] chooses between them based on
//! the child operator's [`AccessMode`] and [`CardinalityHint`].

#[cfg(not(target_family = "wasm"))]
use std::collections::VecDeque;
#[cfg(not(target_family = "wasm"))]
use std::pin::Pin;
#[cfg(not(target_family = "wasm"))]
use std::task::{Context, Poll};

#[cfg(not(target_family = "wasm"))]
use futures::Stream;

#[cfg(not(target_family = "wasm"))]
use crate::exec::ValueBatch;
use crate::exec::ValueBatchStream;
use crate::exec::access_mode::AccessMode;
use crate::exec::cardinality::CardinalityHint;
#[cfg(not(target_family = "wasm"))]
use crate::expr::FlowResult;

/// Value-count threshold for the [`CardinalityHint::Bounded`] short-circuit.
///
/// When an operator declares `Bounded(n)` with `n` at or below this limit, the
/// output fits in roughly one batch, so spawning a dedicated task cannot
/// overlap meaningful work. Cooperative prefetch is used instead.
///
/// This matches the scan batch size used by most operators (see
/// [`super::operators::scan::common::BATCH_SIZE`]).
#[cfg(not(target_family = "wasm"))]
const SMALL_BOUNDED_THRESHOLD: usize = 1000;

/// Buffer a child operator's stream using the appropriate strategy.
///
/// Strategy selection:
/// 1. `CardinalityHint::AtMostOne` — no buffering (overhead exceeds benefit)
/// 2. `CardinalityHint::Bounded(n)` where `n` is small — cooperative prefetch (output fits in ~1
///    batch; spawn overhead would dominate)
/// 3. `ReadOnly` children — spawned-task buffer (true pipeline parallelism)
/// 4. `ReadWrite` children — cooperative prefetch (preserves mutation ordering)
/// 5. `buffer_size == 0` — disables buffering entirely
///
/// On WASM targets, returns the stream unchanged regardless of mode.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn buffer_stream(
	stream: ValueBatchStream,
	mode: AccessMode,
	cardinality: CardinalityHint,
	buffer_size: usize,
) -> ValueBatchStream {
	if buffer_size == 0 {
		return stream;
	}
	// Short-circuit for trivially small streams where buffering overhead
	// exceeds any possible parallelism benefit.
	match cardinality {
		CardinalityHint::AtMostOne => return stream,
		CardinalityHint::Bounded(n) if n <= SMALL_BOUNDED_THRESHOLD => {
			// n is a value count, buffer_size is a batch count — use the
			// batch-denominated buffer_size for the prefetch capacity.
			return prefetch_buffered(stream, buffer_size);
		}
		_ => {}
	}
	match mode {
		AccessMode::ReadOnly => spawn_buffered(stream, buffer_size),
		AccessMode::ReadWrite => prefetch_buffered(stream, buffer_size),
	}
}

/// WASM: no-op — single-threaded runtime has nothing to overlap with.
#[cfg(target_family = "wasm")]
pub(crate) fn buffer_stream(
	stream: ValueBatchStream,
	_mode: AccessMode,
	_cardinality: CardinalityHint,
	_buffer_size: usize,
) -> ValueBatchStream {
	stream
}

// ===========================================================================
// Spawned-task buffer (parallel)
// ===========================================================================

/// Create a spawned-task buffered stream for true pipeline parallelism.
///
/// The inner stream is moved into a separate tokio task that eagerly produces
/// batches into a bounded channel. The returned stream reads from the channel.
///
/// Safe to use when the operator pipeline's `RootContext.ctx` is a snapshot
/// (independent `Arc<Context>`) rather than a clone of the executor's Arc.
#[cfg(not(target_family = "wasm"))]
fn spawn_buffered(stream: ValueBatchStream, buffer_size: usize) -> ValueBatchStream {
	let (tx, rx) = async_channel::bounded(buffer_size);
	let handle = tokio::spawn(async move {
		futures::pin_mut!(stream);
		while let Some(item) = futures::StreamExt::next(&mut stream).await {
			if tx.send(item).await.is_err() {
				break; // consumer dropped
			}
		}
	});
	Box::pin(SpawnedBufferedStream {
		rx: Box::pin(rx),
		handle,
	})
}

/// A stream backed by a bounded channel, with an associated spawned task.
///
/// When dropped, the spawned task is aborted so that any shared references
/// it holds are released promptly.
#[cfg(not(target_family = "wasm"))]
struct SpawnedBufferedStream {
	/// Channel receiver yielding batches from the spawned task.
	rx: ValueBatchStream,
	/// Handle to the spawned producer task — aborted on drop.
	handle: tokio::task::JoinHandle<()>,
}

#[cfg(not(target_family = "wasm"))]
impl Drop for SpawnedBufferedStream {
	fn drop(&mut self) {
		self.handle.abort();
	}
}

#[cfg(not(target_family = "wasm"))]
impl Stream for SpawnedBufferedStream {
	type Item = FlowResult<ValueBatch>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.get_mut().rx.as_mut().poll_next(cx)
	}
}

// ===========================================================================
// Cooperative prefetch buffer (sequential)
// ===========================================================================

/// Create a cooperative prefetch buffer for read-write pipelines.
///
/// Eagerly polls the inner stream and caches ready results, reducing poll
/// round-trips through the operator tree. No tasks are spawned, so mutation
/// ordering is preserved.
#[cfg(not(target_family = "wasm"))]
fn prefetch_buffered(stream: ValueBatchStream, buffer_size: usize) -> ValueBatchStream {
	Box::pin(PrefetchStream {
		inner: stream,
		buffer: VecDeque::with_capacity(buffer_size),
		buffer_size,
		exhausted: false,
	})
}

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
/// cancellation semantics are preserved.
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
