//! Operator pipeline buffering.
//!
//! Wraps a [`ValueBatchStream`] in a spawned tokio task with a bounded channel,
//! enabling inter-operator pipeline parallelism. Each operator's child stream
//! runs in its own task, eagerly producing batches into the channel while the
//! parent operator processes the current batch.

use crate::exec::ValueBatchStream;

/// Wrap a [`ValueBatchStream`] in a spawned task with a bounded channel buffer.
///
/// The inner stream runs in a separate tokio task, producing batches eagerly
/// into the channel. Returns a receiver stream that the caller polls.
///
/// When `SURREAL_OPERATOR_BUFFER_SIZE` is 0, returns the stream unchanged
/// (disabling pipeline buffering).
///
/// # Backpressure
///
/// The bounded channel blocks the producer when the buffer is full, preventing
/// unbounded memory growth.
///
/// # Cancellation
///
/// When the consumer drops the returned stream, the spawned task is aborted
/// immediately via its `JoinHandle`, ensuring that any references held by the
/// task (e.g. `Arc<Context>`) are released synchronously. This is critical for
/// context-unfreezing to succeed after timeouts or early termination.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn buffer_stream(stream: ValueBatchStream) -> ValueBatchStream {
	let buffer_size = *crate::cnf::OPERATOR_BUFFER_SIZE;
	if buffer_size == 0 {
		return stream;
	}
	let (tx, rx) = async_channel::bounded(buffer_size);
	let handle = tokio::spawn(async move {
		futures::pin_mut!(stream);
		while let Some(item) = futures::StreamExt::next(&mut stream).await {
			if tx.send(item).await.is_err() {
				break; // consumer dropped
			}
		}
	});
	Box::pin(BufferedStream {
		rx: Box::pin(rx),
		handle: handle,
	})
}

/// WASM: no-op — single-threaded runtime cannot benefit from pipeline
/// parallelism.
#[cfg(target_family = "wasm")]
pub(crate) fn buffer_stream(stream: ValueBatchStream) -> ValueBatchStream {
	stream
}

// ---------------------------------------------------------------------------
// BufferedStream — ties the spawned task lifetime to the stream
// ---------------------------------------------------------------------------

/// A stream backed by a bounded channel receiver, with an associated spawned
/// task that feeds it.
///
/// When this stream is dropped, the spawned task is **aborted** so that any
/// shared references it holds (contexts, transactions, etc.) are released
/// immediately rather than lingering until the task notices the channel closed.
#[cfg(not(target_family = "wasm"))]
struct BufferedStream {
	/// Channel receiver that yields batches produced by the spawned task.
	rx: ValueBatchStream,
	/// Handle to the spawned producer task. Aborted on drop.
	handle: tokio::task::JoinHandle<()>,
}

#[cfg(not(target_family = "wasm"))]
impl Drop for BufferedStream {
	fn drop(&mut self) {
		self.handle.abort();
	}
}

#[cfg(not(target_family = "wasm"))]
impl futures::Stream for BufferedStream {
	type Item = crate::expr::FlowResult<crate::exec::ValueBatch>;

	fn poll_next(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		self.get_mut().rx.as_mut().poll_next(cx)
	}
}
