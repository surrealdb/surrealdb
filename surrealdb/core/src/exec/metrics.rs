//! Operator-level metrics and instrumented stream wrapper.
//!
//! Each [`ExecOperator`] owns an [`OperatorMetrics`] instance that records
//! output rows, output batches, and wall-clock elapsed time. The
//! [`monitor_stream`] function wraps a [`ValueBatchStream`] so that every
//! yielded batch automatically updates the metrics and emits a tracing span.
//!
//! This replaces the former `InstrumentedStream` / `instrument_stream` pair
//! with a single wrapper that handles both tracing and metrics collection.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures::Stream;

use crate::exec::{FlowResult, ValueBatch, ValueBatchStream};

// ---------------------------------------------------------------------------
// WASM-compatible timing helper
// ---------------------------------------------------------------------------

/// Returns a monotonic timestamp in nanoseconds.
///
/// On native targets this uses `std::time::Instant`; on WASM it falls back
/// to `performance.now()` (millisecond resolution, converted to ns).
#[cfg(not(target_family = "wasm"))]
fn now_ns() -> u64 {
	use std::time::Instant;
	// Use a thread-local base instant so we get monotonic, small offsets.
	thread_local! {
		static BASE: Instant = Instant::now();
	}
	BASE.with(|base| base.elapsed().as_nanos() as u64)
}

#[cfg(target_family = "wasm")]
fn now_ns() -> u64 {
	// js_sys::Date::now() returns milliseconds as f64.
	(js_sys::Date::now() * 1_000_000.0) as u64
}

// ---------------------------------------------------------------------------
// OperatorMetrics
// ---------------------------------------------------------------------------

/// Per-operator runtime metrics.
///
/// All counters are atomically updated so that concurrent polling of the
/// same stream (should it ever happen) is safe. The typical access pattern
/// is single-writer (the stream) / single-reader (the ANALYZE formatter).
#[derive(Debug)]
pub(crate) struct OperatorMetrics {
	/// Total number of rows emitted.
	output_rows: AtomicU64,
	/// Total number of batches emitted.
	output_batches: AtomicU64,
	/// Inclusive wall-clock time spent inside `poll_next` (nanoseconds).
	elapsed_ns: AtomicU64,
}

impl OperatorMetrics {
	/// Create a new zeroed metrics instance.
	pub(crate) fn new() -> Self {
		Self {
			output_rows: AtomicU64::new(0),
			output_batches: AtomicU64::new(0),
			elapsed_ns: AtomicU64::new(0),
		}
	}

	/// Total output rows recorded so far.
	pub(crate) fn output_rows(&self) -> u64 {
		self.output_rows.load(Ordering::Relaxed)
	}

	/// Total output batches recorded so far.
	pub(crate) fn output_batches(&self) -> u64 {
		self.output_batches.load(Ordering::Relaxed)
	}

	/// Elapsed wall-clock nanoseconds recorded so far.
	pub(crate) fn elapsed_ns(&self) -> u64 {
		self.elapsed_ns.load(Ordering::Relaxed)
	}

	/// Record one batch of `rows` values, adding `delta_ns` to elapsed time.
	fn record_batch(&self, rows: u64, delta_ns: u64) {
		self.output_rows.fetch_add(rows, Ordering::Relaxed);
		self.output_batches.fetch_add(1, Ordering::Relaxed);
		self.elapsed_ns.fetch_add(delta_ns, Ordering::Relaxed);
	}

	/// Record elapsed time without a batch (e.g. when `poll_next` returns
	/// `Pending` or `None`).
	fn record_elapsed(&self, delta_ns: u64) {
		self.elapsed_ns.fetch_add(delta_ns, Ordering::Relaxed);
	}
}

// ---------------------------------------------------------------------------
// MetricsStream
// ---------------------------------------------------------------------------

/// A stream wrapper that records per-operator metrics and emits tracing spans.
///
/// On every `poll_next`:
///   1. A `tracing::trace_span!("batch", op, idx, size)` is entered.
///   2. Wall-clock time is measured around the inner `poll_next`.
///   3. If a batch is yielded, the metrics are updated with the row count and elapsed time.
///
/// This replaces the former `InstrumentedStream` with a single wrapper that
/// handles both tracing and metrics collection.
struct MetricsStream {
	inner: ValueBatchStream,
	metrics: Arc<OperatorMetrics>,
	name: &'static str,
	batch_idx: u64,
}

impl Stream for MetricsStream {
	type Item = FlowResult<ValueBatch>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut(); // safe: MetricsStream is Unpin
		let span = tracing::trace_span!(
			"batch",
			op = this.name,
			idx = this.batch_idx,
			size = tracing::field::Empty,
		);
		let _enter = span.enter();

		let start = now_ns();
		let result = this.inner.as_mut().poll_next(cx);
		let delta = now_ns().saturating_sub(start);

		match &result {
			Poll::Ready(Some(Ok(batch))) => {
				let rows = batch.values.len() as u64;
				span.record("size", rows);
				this.metrics.record_batch(rows, delta);
				this.batch_idx += 1;
			}
			Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
				this.metrics.record_elapsed(delta);
			}
			Poll::Pending => {
				this.metrics.record_elapsed(delta);
			}
		}

		result
	}
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Wrap a [`ValueBatchStream`] with metrics recording and tracing spans.
///
/// Every yielded batch updates the given `metrics` and emits a `trace`-level
/// tracing span. This is the single point of instrumentation for all
/// operators.
pub(crate) fn monitor_stream(
	stream: ValueBatchStream,
	name: &'static str,
	metrics: &Arc<OperatorMetrics>,
) -> ValueBatchStream {
	Box::pin(MetricsStream {
		inner: stream,
		metrics: Arc::clone(metrics),
		name,
		batch_idx: 0,
	})
}
