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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures::Stream;

use crate::exec::{FlowResult, ValueBatch, ValueBatchStream};

// ---------------------------------------------------------------------------
// WASM-compatible timing helper
// ---------------------------------------------------------------------------

/// Returns a monotonic timestamp in nanoseconds.
///
/// Uses `web_time::Instant` which delegates to the standard library on native
/// targets and `performance.now()` on WASM (millisecond resolution, converted to ns).
fn now_ns() -> u64 {
	use web_time::Instant;
	thread_local! {
		static BASE: Instant = Instant::now();
	}
	BASE.with(|base| base.elapsed().as_nanos() as u64)
}

// ---------------------------------------------------------------------------
// OperatorMetrics
// ---------------------------------------------------------------------------

/// Per-operator runtime metrics.
///
/// All counters are atomically updated so that concurrent polling of the
/// same stream (should it ever happen) is safe. The typical access pattern
/// is single-writer (the stream) / single-reader (the ANALYZE formatter).
///
/// When `enabled` is false, [`monitor_stream`] returns the inner stream
/// directly without wrapping, eliminating all per-batch timing, atomic
/// counter, and tracing span overhead on the hot path.
#[derive(Debug)]
pub(crate) struct OperatorMetrics {
	/// Whether metrics collection is active. When false, `monitor_stream`
	/// bypasses the MetricsStream wrapper entirely for zero overhead.
	enabled: AtomicBool,
	/// Total number of rows emitted.
	output_rows: AtomicU64,
	/// Total number of batches emitted.
	output_batches: AtomicU64,
	/// Inclusive wall-clock time spent inside `poll_next` (nanoseconds).
	elapsed_ns: AtomicU64,
	/// Rows skipped before decode by a scan-side reject optimisation
	/// (currently the TopK threshold probe). Unlike the fields above, this is
	/// not written by [`MetricsStream`] — the scan visitor accumulates per
	/// cursor batch and flushes via [`OperatorMetrics::add_skipped_rows`].
	skipped_rows: AtomicU64,
	/// Total graph adjacency-key entries scanned by a graph edge scan,
	/// regardless of whether they matched a pushed-down predicate. This is the
	/// denominator for traversal filter selectivity (matched = `output_rows`).
	/// Like `skipped_rows`, the scan accumulates per cursor batch and flushes
	/// via [`OperatorMetrics::add_edges_scanned`].
	edges_scanned: AtomicU64,
	/// Graph adjacency entries served from packed block chunks by the merged
	/// adjacency reader, and entries served from per-edge pointer keys. The
	/// two split `edges_scanned` by storage tier, making the fold's effect
	/// observable: a fully folded scope scans blocks only. Accumulated per
	/// cursor and flushed via [`OperatorMetrics::add_block_hits`] /
	/// [`OperatorMetrics::add_delta_hits`].
	block_hits: AtomicU64,
	delta_hits: AtomicU64,
	/// Inline sidecar cache lookups by a graph or reference scan: a hit is a
	/// present, live cache serving a `(source, direction)` without opening a
	/// cursor; a miss is an eligible lookup that found the key absent or
	/// spilled and fell back to the scan. Accumulated per source and flushed
	/// via [`OperatorMetrics::add_cache_hits`] /
	/// [`OperatorMetrics::add_cache_misses`].
	cache_hits: AtomicU64,
	cache_misses: AtomicU64,
	/// Inline-payload predicate evaluations by a graph scan (truthy or
	/// falsy — the edges served without fetching their records), and edges
	/// whose payload was unusable and fell back to the record path.
	props_evals: AtomicU64,
	props_fallbacks: AtomicU64,
	/// Candidate records fetched for per-candidate evaluation *inside* an ANN
	/// search (the truthy-document filters' record fetch + permission gate +
	/// cond evaluation), one per unique candidate evaluated. This counts
	/// evaluations, not KV reads: the underlying record read may be served
	/// from the transaction cache (the pendings scan and the neighbourhood
	/// prefetch batch-warm it), and a candidate is counted even if its fetch
	/// subsequently errors. The KNN prefilter (#548) exists to drive this to
	/// zero for index-covered predicates — EXPLAIN ANALYZE renders it so that
	/// claim is observable. Deliberately excludes the exact tier's bounded
	/// resolve-stage fetches, which happen outside any traversal.
	records_fetched: AtomicU64,
	/// Whether a bitmap branch declined to produce its bitmap (budget
	/// overflow, a strict-mode propagation, or a permission or legacy-key
	/// gate) and was dropped from its parent's composition. Distinguishes a
	/// dropped branch from a genuinely empty candidate set in EXPLAIN
	/// ANALYZE, where both otherwise render `rows: 0`. Discloses only that
	/// the branch declined, never any cardinality.
	branch_dropped: AtomicBool,
}

impl OperatorMetrics {
	/// Create a disabled metrics instance (zero overhead at runtime).
	///
	/// Use this for normal query execution where EXPLAIN ANALYZE is not active.
	/// [`monitor_stream`] will return the inner stream directly, skipping all
	/// per-batch timing and tracing work.
	pub(crate) fn new() -> Self {
		Self {
			enabled: AtomicBool::new(false),
			output_rows: AtomicU64::new(0),
			output_batches: AtomicU64::new(0),
			elapsed_ns: AtomicU64::new(0),
			skipped_rows: AtomicU64::new(0),
			edges_scanned: AtomicU64::new(0),
			block_hits: AtomicU64::new(0),
			delta_hits: AtomicU64::new(0),
			cache_hits: AtomicU64::new(0),
			cache_misses: AtomicU64::new(0),
			props_evals: AtomicU64::new(0),
			props_fallbacks: AtomicU64::new(0),
			records_fetched: AtomicU64::new(0),
			branch_dropped: AtomicBool::new(false),
		}
	}

	/// Whether metrics collection is active (i.e. EXPLAIN ANALYZE enabled it).
	pub(crate) fn is_enabled(&self) -> bool {
		self.enabled.load(Ordering::Relaxed)
	}

	/// Enable metrics collection on this instance.
	///
	/// Called by `AnalyzePlan` to activate recording before execution.
	/// After this call, [`monitor_stream`] will wrap streams with the
	/// full timing/counting/tracing instrumentation.
	pub(crate) fn enable(&self) {
		self.enabled.store(true, Ordering::Relaxed);
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

	/// Rows skipped before decode recorded so far.
	pub(crate) fn skipped_rows(&self) -> u64 {
		self.skipped_rows.load(Ordering::Relaxed)
	}

	/// Record `n` rows skipped before decode by a scan-side reject check.
	pub(crate) fn add_skipped_rows(&self, n: u64) {
		self.skipped_rows.fetch_add(n, Ordering::Relaxed);
	}

	/// Record `n` output rows directly, for operators that produce their
	/// result outside a monitored stream (the bitmap candidate nodes, whose
	/// "rows" are candidate doc-IDs computed during the parent's execution).
	pub(crate) fn add_output_rows(&self, n: u64) {
		self.output_rows.fetch_add(n, Ordering::Relaxed);
	}

	/// Total graph adjacency-key entries scanned so far.
	pub(crate) fn edges_scanned(&self) -> u64 {
		self.edges_scanned.load(Ordering::Relaxed)
	}

	/// Record `n` graph adjacency-key entries scanned by a graph edge scan.
	pub(crate) fn add_edges_scanned(&self, n: u64) {
		self.edges_scanned.fetch_add(n, Ordering::Relaxed);
	}

	/// Graph adjacency entries served from packed blocks so far.
	pub(crate) fn block_hits(&self) -> u64 {
		self.block_hits.load(Ordering::Relaxed)
	}

	/// Record `n` adjacency entries served from packed blocks.
	pub(crate) fn add_block_hits(&self, n: u64) {
		self.block_hits.fetch_add(n, Ordering::Relaxed);
	}

	/// Graph adjacency entries served from per-edge pointer keys so far.
	pub(crate) fn delta_hits(&self) -> u64 {
		self.delta_hits.load(Ordering::Relaxed)
	}

	/// Record `n` adjacency entries served from per-edge pointer keys.
	pub(crate) fn add_delta_hits(&self, n: u64) {
		self.delta_hits.fetch_add(n, Ordering::Relaxed);
	}

	/// Inline-cache lookups served without a cursor so far.
	pub(crate) fn cache_hits(&self) -> u64 {
		self.cache_hits.load(Ordering::Relaxed)
	}

	/// Record `n` inline-cache lookups served without a cursor.
	pub(crate) fn add_cache_hits(&self, n: u64) {
		self.cache_hits.fetch_add(n, Ordering::Relaxed);
	}

	/// Eligible inline-cache lookups that fell back to the scan so far.
	pub(crate) fn cache_misses(&self) -> u64 {
		self.cache_misses.load(Ordering::Relaxed)
	}

	/// Record `n` eligible inline-cache lookups that fell back to the scan.
	pub(crate) fn add_cache_misses(&self, n: u64) {
		self.cache_misses.fetch_add(n, Ordering::Relaxed);
	}

	/// Inline-payload predicate evaluations so far.
	pub(crate) fn props_evals(&self) -> u64 {
		self.props_evals.load(Ordering::Relaxed)
	}

	/// Record `n` inline-payload predicate evaluations.
	pub(crate) fn add_props_evals(&self, n: u64) {
		self.props_evals.fetch_add(n, Ordering::Relaxed);
	}

	/// Edges whose payload fell back to the record path so far.
	pub(crate) fn props_fallbacks(&self) -> u64 {
		self.props_fallbacks.load(Ordering::Relaxed)
	}

	/// Record `n` edges whose payload fell back to the record path.
	pub(crate) fn add_props_fallbacks(&self, n: u64) {
		self.props_fallbacks.fetch_add(n, Ordering::Relaxed);
	}

	/// Candidate records fetched for in-traversal evaluation so far.
	pub(crate) fn records_fetched(&self) -> u64 {
		self.records_fetched.load(Ordering::Relaxed)
	}

	/// Record `n` candidate records fetched for in-traversal evaluation by an
	/// ANN search's truthy-document filter.
	pub(crate) fn add_records_fetched(&self, n: u64) {
		self.records_fetched.fetch_add(n, Ordering::Relaxed);
	}

	/// Whether this operator's bitmap branch declined and was dropped.
	pub(crate) fn branch_dropped(&self) -> bool {
		self.branch_dropped.load(Ordering::Relaxed)
	}

	/// Record that this operator's bitmap branch declined to produce its
	/// bitmap and was dropped from its parent's composition.
	pub(crate) fn set_branch_dropped(&self) {
		self.branch_dropped.store(true, Ordering::Relaxed);
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

/// Lets an ANN truthy-document filter count its in-traversal candidate
/// evaluations against the surrounding scan operator's metrics, without the
/// index layer naming this one.
impl crate::idx::trees::gate::CandidateFetchCounter for OperatorMetrics {
	fn record_fetch(&self) {
		self.add_records_fetched(1);
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
				let rows = batch.len() as u64;
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
///
/// When `metrics` is disabled (the default from [`OperatorMetrics::new()`]),
/// the stream is returned directly without any wrapper, eliminating all
/// per-batch overhead (timing, atomic counters, tracing spans).
pub(crate) fn monitor_stream(
	stream: ValueBatchStream,
	name: &'static str,
	metrics: &Arc<OperatorMetrics>,
) -> ValueBatchStream {
	if !metrics.enabled.load(Ordering::Relaxed) {
		return stream;
	}
	Box::pin(MetricsStream {
		inner: stream,
		metrics: Arc::clone(metrics),
		name,
		batch_idx: 0,
	})
}
