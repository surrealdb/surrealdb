//! Operator pipeline buffering.
//!
//! Provides two buffering strategies for inter-operator pipelining:
//!
//! - **`SpawnedBufferedStream`** (parallel): Runs the inner stream in a separate tokio task with a
//!   bounded channel, giving true pipeline parallelism. Used where neither the buffered stream nor
//!   the operator draining it writes, so there is no mutation ordering to preserve.
//!
//! - **`PrefetchStream`** (cooperative): Eagerly polls the inner stream on the same task and caches
//!   ready results. Used where either of them writes and the side effects must stay sequential.
//!
//! The public entry point [`buffer_stream`] chooses between them from the
//! [`AccessMode`] its caller passes — which covers the caller's own work as well
//! as the buffered stream's, see [`buffer_stream`] — and the buffered stream's
//! [`CardinalityHint`].

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

/// Buffer a child operator's stream using the appropriate strategy.
///
/// Strategy selection:
/// 1. `CardinalityHint::AtMostOne` — no buffering (overhead exceeds benefit)
/// 2. `ReadOnly` — spawned-task buffer (true pipeline parallelism)
/// 3. `ReadWrite` — cooperative prefetch (preserves mutation ordering)
/// 4. `buffer_size == 0` — disables buffering entirely
///
/// # The caller's obligation
///
/// `mode` describes everything that runs while this buffer is being drained,
/// which is the buffering operator's own work as well as the buffered stream's.
/// A caller therefore passes its own [`AccessMode`] combined with the buffered
/// stream's, never the buffered stream's alone:
///
/// ```ignore
/// buffer_stream(
///     self.input.execute(ctx)?,
///     self.access_mode().combine(self.input.access_mode()),
///     cardinality,
///     buffer_size,
/// )
/// ```
///
/// The combination is load-bearing rather than defensive. `ReadOnly` here moves
/// the buffered stream onto its own task, where it runs ahead of its consumer
/// and concurrently with whatever that consumer does between polls. A caller
/// that writes in that window would interleave its writes with reads it no
/// longer orders, and writes in a plan share one transaction whose
/// savepoints are a stack with no handle, so a second writer entering before
/// the first has left unwinds the wrong one. The combined mode keeps the read
/// and the write on one task, in the order the plan states.
///
/// The caller that writes is frequently a read-side operator: one that owns a
/// projection's mutation subquery holds the write for a whole `SELECT`, so
/// the rule cannot be narrowed to the mutation operators. An operator whose
/// own mode is `ReadOnly` passes its input's mode unchanged, which the
/// combination does by construction.
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
		CardinalityHint::Unbounded => {}
	}
	match mode {
		AccessMode::ReadOnly => spawn_buffered(stream, mode, buffer_size),
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
/// Only a [`ReadOnly`](AccessMode::ReadOnly) stream may be moved onto another
/// task: a writing one running ahead of its consumer would order its writes by
/// the scheduler. This is the operator-level counterpart of the row-level rule
/// in [`crate::exec::fan_out`], and [`buffer_stream`] is what enforces it.
///
/// `mode` is the mode that selected this strategy, taken so that the rule is
/// asserted where the task is spawned rather than trusted at the one caller.
///
/// Safe to use when the operator pipeline's `RootContext.ctx` is a snapshot
/// (independent `Arc<Context>`) rather than a clone of the executor's Arc.
#[cfg(not(target_family = "wasm"))]
fn spawn_buffered(
	mut stream: ValueBatchStream,
	mode: AccessMode,
	buffer_size: usize,
) -> ValueBatchStream {
	debug_assert_eq!(
		mode,
		AccessMode::ReadOnly,
		"a ReadWrite stream must not be spawned onto its own task"
	);
	let (tx, rx) = async_channel::bounded(buffer_size);
	let handle = tokio::spawn(async move {
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
/// round-trips between operators. No tasks are spawned, so mutation
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
/// results, reducing poll round-trips between operators.
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
