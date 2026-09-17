//! The bounded pending-queue scan shared by the ANN kNN read paths.
//!
//! HNSW and DiskANN both queue writes under their own pending keyspaces and
//! drain them from a background compactor. A kNN search over either must read
//! that whole queue — every live pending vector can win a top-K slot, and every
//! pending doc-ID has to mask the stale graph entry it supersedes — so the read
//! is O(backlog) by construction.
//!
//! What this module bounds is *residency*, not the work: a scan holds one
//! cursor page plus the candidates read from it and not yet scored, whatever
//! the backlog is. [`PendingScan`] is that mechanism, plus the accounting the
//! backlog report is derived from. Both engines drive it identically; only the
//! key decoding, the coalescing rules and the scoring differ, and those stay in
//! the engines.
//!
//! One rule about those engines' keys does live here, beside the scan that
//! reads them: [`take_pending_record_id`], which decides whether a pending
//! entry's record id comes from its value or from its key.
//!
//! # What the byte budget is and is not
//!
//! [`PENDING_MAX_BYTES`] is a **target the page sizer converges to, not a
//! ceiling**. A cursor page is requested from the KV layer by entry count, and
//! the size of the entries it will return is not known until they have been
//! read; the sizer therefore sizes each page from the average entry size of the
//! page before it. A range whose entries grow from one page to the next can
//! overshoot the target once, before the count adapts — and the cursor's arena
//! grows to its high-water mark and never shrinks, so that page's allocation is
//! retained for the rest of the scan.
//!
//! No per-engine mechanism can make that byte target absolute. A single
//! pending value can itself be arbitrarily large, and a TiKV or IndexedDB scan
//! decodes its response before this code can inspect a row. The fixed
//! [`PENDING_MAX_ROWS`] cap still matters, though: without it, one narrow page
//! could size the next at the engine-wide 500-row default and multiply an
//! abrupt increase in value width 500 times before the sizer reacted. Pending
//! scans instead start at four rows, never request more than sixteen, and can
//! shrink to one. That bounds the paging amplification while retaining batched
//! reads for remote stores.
//!
//! Two bounds do hold unconditionally, and between them they are what keeps a
//! deep queue from exhausting the process:
//!
//! * **Residency is independent of the backlog.** A scan holds one page and one batch however deep
//!   the queue is, so a deep queue costs more round trips, not more memory. This is the property
//!   that matters, because the cost is paid per *concurrent* search and the queue is deepest
//!   exactly when bulk writes are outrunning compaction.
//! * **A page never holds more than [`PENDING_MAX_ROWS`] entries.** This conservative cap is far
//!   below the engine-wide scan default, so heterogeneous vector counts cannot recreate the
//!   500-value cursor allocation this path exists to prevent; the byte target can only make it
//!   smaller.

use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use surrealdb_kvs::api::ValsBatch;
use tracing::warn;

use crate::IndexKeyBase;
use crate::trees::hnsw::VectorId;
use crate::trees::vector::SerializedVector;
use crate::val::RecordIdKey;

/// Maximum number of pending entries a kNN scan holds in one scoring batch.
///
/// Sized for the batched record multi-get that gates those entries, which stops
/// paying off well below this many ids.
pub(crate) const PENDING_MAX_BATCH_KEYS: usize = 1024;

/// Target for the pending bytes a kNN scan holds in memory at once.
///
/// Covers the scan's whole pending residency, which is two live allocations
/// rather than one: the cursor page being read, and the candidates read from it
/// and not yet scored. The page stays borrowed across the scoring await, so a
/// budget on the batch alone would understate residency by a whole page.
///
/// Deliberately tighter than either engine's compaction budget: compaction runs
/// as one background task per index, while this is paid once per *concurrent*
/// kNN search over the same index.
///
/// A target, not a ceiling — see the module documentation.
pub(crate) const PENDING_MAX_BYTES: usize = 4 * 1024 * 1024;

/// The share of [`PENDING_MAX_BYTES`] one cursor page is sized for.
///
/// The remainder is what a scoring batch can fill before it must be scored, so
/// this trades scan round trips against how many candidates one record
/// multi-get covers.
pub(crate) const PENDING_MAX_PAGE_BYTES: usize = PENDING_MAX_BYTES / 4;

/// Entries requested for a scan's first pending cursor page, before any entry
/// size has been observed.
///
/// Small enough that a page of very wide vectors cannot spike residency before
/// the sizer can act on a measurement; for narrow vectors it costs one extra
/// round trip per scan.
pub(crate) const PENDING_PROBE_ROWS: u32 = 4;

/// Hard upper bound on the entries one pending cursor page may hold, whatever
/// the byte target would allow.
///
/// Unlike [`PENDING_MAX_BYTES`] this one cannot be overshot: it is the row count
/// handed to the KV layer, which never returns more rows than it is asked for.
/// Sixteen preserves useful range-scan and legacy-ownership batching without
/// allowing one narrow page to turn the next into a 500-value allocation.
pub(crate) const PENDING_MAX_ROWS: u32 = 16;

/// The record a pending entry belongs to: the id the value carries, or the one
/// its key spells when the value predates the field.
///
/// **The value's id is authoritative.** A pending key spells its id under
/// `IndexFormat`, the codec for indexed values, which omits a number's kind — an
/// index key stands for its whole equality class, so a lookup for `1` finds a
/// row stored as `1f`. A record id is not matched that way; it addresses a
/// record. So an id holding a number comes back from a key decimalised, naming
/// no record: compaction would map the doc-ID to a key nothing is stored under
/// and the kNN hit would be dropped at the record fetch, after taking a top-k
/// slot.
///
/// `decode_fallback` therefore answers only for an entry written before the
/// value carried an id, and keeps whatever that key could express.
///
/// The id is *taken*, not copied: nothing reads the field afterwards, and a
/// compound `Array`/`Object` id is a deep copy on paths that are O(backlog) by
/// design.
///
/// Each engine keeps its own key decoding — the layouts and their decoders are
/// index concerns, and DiskANN has two of them — so only this rule is shared.
pub(crate) fn take_pending_record_id(
	exact_id: &mut Option<RecordIdKey>,
	decode_fallback: impl FnOnce() -> Result<RecordIdKey>,
) -> Result<RecordIdKey> {
	match exact_id.take() {
		Some(id) => Ok(id),
		None => decode_fallback(),
	}
}

/// The low bit of [`PendingBacklogReport::state`] records whether a backlog is
/// currently reported; the remaining bits form its observation generation.
const PENDING_REPORT_ARMED: usize = 1;

/// Per-index state for the once-per-crossing pending-backlog warning.
///
/// Every scan that crosses a materialisation budget advances the generation,
/// even when an earlier scan already emitted the warning. A clean scan may
/// clear only the armed generation it observed when it started. This prevents
/// an older snapshot from clearing a newer scan's backlog observation while
/// keeping the common path lock-free.
#[derive(Default)]
pub(crate) struct PendingBacklogReport {
	state: AtomicUsize,
}

impl PendingBacklogReport {
	/// Captures the generation a scan is allowed to re-arm when it finishes.
	fn snapshot(&self) -> usize {
		self.state.load(Ordering::Relaxed)
	}

	/// Arms a fresh observation generation, returning whether this observation
	/// transitions the index from unreported to reported and should emit a log.
	fn arm(&self) -> bool {
		let mut current = self.state.load(Ordering::Relaxed);
		loop {
			let next = current.wrapping_add(2) | PENDING_REPORT_ARMED;
			match self.state.compare_exchange_weak(
				current,
				next,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return current & PENDING_REPORT_ARMED == 0,
				Err(observed) => current = observed,
			}
		}
	}

	/// Re-arms reporting only when no crossing scan has advanced the generation
	/// since this clean scan began.
	fn clear_if_unchanged(&self, snapshot: usize) {
		if snapshot & PENDING_REPORT_ARMED == 0 {
			return;
		}
		let _ = self.state.compare_exchange(
			snapshot,
			snapshot & !PENDING_REPORT_ARMED,
			Ordering::Relaxed,
			Ordering::Relaxed,
		);
	}

	/// Whether a backlog observation is currently armed.
	#[cfg(test)]
	pub(crate) fn is_reported(&self) -> bool {
		self.state.load(Ordering::Relaxed) & PENDING_REPORT_ARMED != 0
	}
}

/// Pending candidates a kNN scan has read and not yet scored.
///
/// Together with the cursor page it was filled from, the batch is the whole of
/// a kNN search's pending residency: entries accumulate until a budget is
/// reached, are scored, and are dropped before the scan reads any more.
///
/// The byte budget is the *combined* one. `page_bytes` carries the encoded size
/// of the page the scan is reading from, which stays borrowed while this batch
/// is scored, so the two are charged against [`PENDING_MAX_BYTES`] together and
/// the batch's own share shrinks as the page grows. Both budgets are checked
/// before an entry is admitted, so a batch that reaches the scorer is inside
/// both — except for a single entry too large for the budget on its own, which
/// is scored alone.
///
/// `ids` and `vectors` are parallel: index `i` of one describes the same
/// candidate as index `i` of the other. `ids` is kept as its own contiguous
/// slice because the record prefetch that precedes scoring takes one.
#[derive(Default)]
pub(crate) struct PendingScoreBatch {
	/// Identity of each buffered candidate, in push order.
	pub(crate) ids: Vec<VectorId>,
	/// Vectors of each buffered candidate, in push order.
	pub(crate) vectors: Vec<Vec<SerializedVector>>,
	/// Encoded size of the buffered entries, as the scan read them.
	bytes: usize,
	/// Encoded size of the cursor page the scan is reading from, which is
	/// resident alongside this batch. Zero once that page is released.
	page_bytes: usize,
}

impl PendingScoreBatch {
	/// Whether nothing is buffered for scoring.
	pub(crate) fn is_empty(&self) -> bool {
		self.ids.is_empty()
	}

	/// Drops candidates whose vectors were cleared during legacy coalescing.
	///
	/// Compacts both parallel vectors in place, so large record identities are
	/// neither cloned nor retained by the truthy filter's prefetch cache. The
	/// encoded-byte accounting is left intact for the caller to record as the
	/// residency the original batch occupied.
	pub(crate) fn retain_non_empty_vectors(&mut self) {
		debug_assert_eq!(self.ids.len(), self.vectors.len());
		let Self {
			ids,
			vectors,
			..
		} = self;
		let mut position = 0;
		ids.retain(|_| {
			let keep = !vectors[position].is_empty();
			position += 1;
			keep
		});
		vectors.retain(|vectors| !vectors.is_empty());
		debug_assert_eq!(ids.len(), vectors.len());
	}

	/// Encoded size of the buffered entries. Read by the DiskANN legacy-ownership
	/// probe, the only out-of-band read that shares a scan's residency budget.
	#[cfg(diskann)]
	pub(crate) fn bytes(&self) -> usize {
		self.bytes
	}

	/// Encoded size of the cursor page resident alongside the buffered entries.
	/// Read by the DiskANN legacy-ownership probe.
	#[cfg(diskann)]
	pub(crate) fn page_bytes(&self) -> usize {
		self.page_bytes
	}

	/// Returns whether admitting an entry of `bytes` would take the batch past a
	/// budget, so it must be scored first.
	///
	/// Checked before the push rather than after it, so a scored batch is always
	/// inside both budgets. An empty batch admits the entry whatever its size:
	/// an entry too large for the budget on its own still has to be scored
	/// somewhere, and scoring it alone is the smallest way to do it.
	fn would_exceed(&self, bytes: usize) -> bool {
		!self.is_empty()
			&& (self.ids.len() >= PENDING_MAX_BATCH_KEYS
				|| self.bytes + self.page_bytes + bytes > PENDING_MAX_BYTES)
	}
}

/// One kNN pending scan: the cursor paging, the residency budget, and the
/// accounting the backlog report is derived from.
///
/// One of these covers every range a scan reads — for HNSW the append-keyed and
/// record-keyed ranges, for DiskANN the legacy range and each non-empty shard —
/// so only a scan's very first page is sized before any entry size is known.
/// That matters because a cursor's arena grows to its high-water mark and never
/// shrinks: a page sized before any measurement is the one page that can leave
/// an oversized buffer behind for the rest of the range.
pub(crate) struct PendingScan<'a> {
	/// Index family named by the backlog report.
	engine: &'static str,
	/// Index the backlog report names.
	ikb: &'a IndexKeyBase,
	/// Generation-tagged backlog report shared by every scan of this index.
	backlog_report: &'a PendingBacklogReport,
	/// Report generation observed when this scan opened. A clean scan may clear
	/// only this exact generation, never one produced by a newer scan.
	report_snapshot: usize,
	/// What this scan materialised, for the tests that pin the bound.
	stats: &'a PendingScanStats,
	/// Entries to request for the next page.
	rows: u32,
	/// Entries read across every range this scan has touched.
	entries: usize,
	/// Whether this scan has crossed a materialisation budget.
	armed: bool,
	/// Candidates read and not yet scored.
	pub(crate) batch: PendingScoreBatch,
}

impl<'a> PendingScan<'a> {
	/// Opens the accounting for one kNN pending scan.
	pub(crate) fn new(
		engine: &'static str,
		ikb: &'a IndexKeyBase,
		backlog_report: &'a PendingBacklogReport,
		stats: &'a PendingScanStats,
	) -> Self {
		let report_snapshot = backlog_report.snapshot();
		Self {
			engine,
			ikb,
			backlog_report,
			report_snapshot,
			stats,
			rows: PENDING_PROBE_ROWS,
			entries: 0,
			armed: false,
			batch: PendingScoreBatch::default(),
		}
	}

	/// Entries to request from the next `next_batch` call.
	pub(crate) fn rows(&self) -> u32 {
		self.rows
	}

	/// Entries this scan has read so far.
	#[cfg(diskann)]
	pub(crate) fn entries(&self) -> usize {
		self.entries
	}

	/// Accounts for one cursor page the scan just read.
	///
	/// Sizes the next page from this one, and charges this page's encoded bytes
	/// to the residency the scoring batch shares with it — the page stays
	/// borrowed while the batch it filled is scored. An empty page samples
	/// nothing, so it leaves the row count as it stands (the page that ends one
	/// range must not reset the size the next range starts from) and releases
	/// the residency charge, which is what ends every range.
	pub(crate) fn observe_page(&mut self, read: &ValsBatch<'_>) {
		let bytes = (read.key_bytes + read.value_bytes) as usize;
		self.stats.record_page(read.len(), bytes);
		self.batch.page_bytes = bytes;
		if read.is_empty() {
			return;
		}
		let avg = bytes.div_ceil(read.len()).max(1);
		self.rows = (PENDING_MAX_PAGE_BYTES / avg).clamp(1, PENDING_MAX_ROWS as usize) as u32;
	}

	/// Releases the page charge when its cursor is deliberately dropped before
	/// another read or scoring pass.
	pub(crate) fn release_page(&mut self) {
		self.batch.page_bytes = 0;
	}

	/// Charges one entry the scan has read, reporting a backlog when this entry
	/// is the one that takes the scan past what a single materialisation batch
	/// holds by entry count or would force the buffered batch to roll over by
	/// residency.
	///
	/// **Call this before the entry's cancellation checkpoint.** A scan loop
	/// checks once per entry, and the checkpoint deep-checks — and so can bail —
	/// on entry counts either side of the crossing. Charging afterwards would
	/// therefore leave a window in which the scan has read the entry that
	/// crosses, bails on it, and reports nothing: exactly the search the record
	/// exists to explain. Charging first makes every entry the scan reads
	/// accounted for before the scan can bail on it.
	///
	/// Charged from the raw scan rather than from the scoring batch, so entries
	/// the scan reads and discards — deletions, superseded entries, candidates
	/// outside the allow-list — count towards the depth they cost to read. The
	/// pre-check is deliberately conservative for the same reason: the encoded
	/// entry and its cursor page are already resident, even if decoding later discovers
	/// that the candidate does not need scoring.
	pub(crate) fn charge_entry(&mut self, entry_bytes: usize) {
		self.entries += 1;
		self.stats.record_entry();
		if self.entries > PENDING_MAX_BATCH_KEYS || self.batch.would_exceed(entry_bytes) {
			self.report();
		}
	}

	/// Returns whether the buffered batch must be scored before an entry of
	/// `bytes` is admitted, reporting a backlog on the first rollover.
	///
	/// The rollover predicate is also evaluated by [`Self::charge_entry`] before
	/// cancellation, so a deadline on the crossing entry cannot suppress the
	/// warning. Evaluating the same predicate again here makes the scoring
	/// decision after decoding without maintaining a second byte threshold; the
	/// report is idempotent.
	pub(crate) fn rollover_required(&mut self, bytes: usize) -> bool {
		if !self.batch.would_exceed(bytes) {
			return false;
		}
		self.report();
		true
	}

	/// Buffers one candidate, charging `bytes` (its encoded size) to the budget.
	pub(crate) fn push(&mut self, id: VectorId, vectors: Vec<SerializedVector>, bytes: usize) {
		self.batch.ids.push(id);
		self.batch.vectors.push(vectors);
		self.batch.bytes += bytes;
	}

	/// Records the batch about to be scored and clears its byte charge.
	///
	/// The candidates themselves are drained by the caller, which owns the
	/// scoring; the page charge stays, because the page outlives the batch it
	/// filled and is released by the next [`Self::observe_page`].
	pub(crate) fn begin_scoring(&mut self) {
		self.stats.record_batch(self.batch.ids.len(), self.batch.bytes, self.batch.page_bytes);
		self.batch.bytes = 0;
	}

	/// Records what an out-of-band read added to the scan's residency, for the
	/// tests that pin it. The DiskANN legacy-ownership probe is the only one.
	#[cfg(diskann)]
	pub(crate) fn record_side_read(&self, keys: usize, resident_bytes: usize) {
		self.stats.record_side_read(keys, resident_bytes);
	}

	/// Closes the scan, re-arming the index's report when the scan completed
	/// inside both budgets — the queue is back within what one search can
	/// materialise, so the next crossing is worth a record again.
	/// Re-arming succeeds only when no newer scan advanced the report generation
	/// after this scan opened.
	///
	/// A cancelled scan never reaches this, which is deliberate: it proves
	/// nothing about the queue having drained, so it leaves the flag as it found
	/// it.
	pub(crate) fn finish(&self) {
		if !self.armed {
			self.backlog_report.clear_if_unchanged(self.report_snapshot);
		}
	}

	/// Reports a pending queue deeper than one kNN materialisation batch.
	///
	/// Emitted from inside the scan, at the entry that crosses, rather than once
	/// the scan finishes: a queue deep enough to matter is also deep enough to
	/// exhaust the query deadline, and every path out of a cancelled scan
	/// returns before its end, so a report that ran only at the end would be
	/// missing from exactly the searches it exists to explain. The entry count
	/// is what the scan had read at the crossing, which is a lower bound on the
	/// queue's depth; the crossing is what the record is about.
	///
	/// Fires once per crossing rather than once per query, so a sustained
	/// backlog produces one line.
	fn report(&mut self) {
		if self.armed {
			return;
		}
		self.armed = true;
		if self.backlog_report.arm() {
			warn!(
				index = %self.ikb,
				engine = self.engine,
				pending_entries_read = self.entries,
				"kNN search read more pending updates than one materialisation batch holds; index compaction is not keeping up with writes"
			);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId};

	#[test]
	fn scoring_batch_drops_cleared_candidates_in_place() {
		let mut batch = PendingScoreBatch {
			ids: vec![VectorId::DocId(1), VectorId::DocId(2), VectorId::DocId(3)],
			vectors: vec![vec![], vec![SerializedVector::F32(vec![1.0, 2.0])], vec![]],
			bytes: 42,
			page_bytes: 7,
		};

		batch.retain_non_empty_vectors();

		assert_eq!(batch.ids, vec![VectorId::DocId(2)]);
		assert_eq!(batch.vectors, vec![vec![SerializedVector::F32(vec![1.0, 2.0])]]);
		assert_eq!(batch.bytes, 42, "residency accounting covers the original batch");
		assert_eq!(batch.page_bytes, 7);
	}

	#[test]
	fn stale_scan_cannot_clear_a_newer_backlog_observation() {
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3));
		let report = PendingBacklogReport::default();
		let stats = PendingScanStats::default();

		// Both scans start before the first crossing. The older clean snapshot
		// must not clear the observation the newer scan publishes afterward.
		let older_clean = PendingScan::new("HNSW", &ikb, &report, &stats);
		let mut newer_crossing = PendingScan::new("HNSW", &ikb, &report, &stats);
		newer_crossing.report();
		assert!(report.is_reported());
		older_clean.finish();
		assert!(report.is_reported());

		// The same protection is required when the warning was already armed:
		// every crossing advances the generation even though it emits no second
		// log, so an older clean scan cannot re-arm behind that observation.
		let older_armed = PendingScan::new("HNSW", &ikb, &report, &stats);
		let mut latest_crossing = PendingScan::new("HNSW", &ikb, &report, &stats);
		latest_crossing.report();
		older_armed.finish();
		assert!(report.is_reported());

		// A clean scan that begins after the latest crossing owns that exact
		// generation and may re-arm it.
		PendingScan::new("HNSW", &ikb, &report, &stats).finish();
		assert!(!report.is_reported());
	}
}

/// What the kNN pending scans on one index materialised, for the tests that pin
/// the residency bound.
///
/// Covers both halves of that bound: the scoring batches, and the cursor pages
/// they were filled from. `peak_resident_bytes` is the two together, which is
/// what a concurrent search actually costs.
///
/// Every counter is test-only, so the type is zero-sized outside test builds and
/// each `record_*` compiles away; the index holds one unconditionally so the
/// scan can take a reference to it without the call sites branching on
/// configuration.
///
/// Totals accumulate over the index's lifetime and the peaks are maxima, so a
/// test reads them after the searches it means to account for.
#[derive(Default)]
pub(crate) struct PendingScanStats {
	/// Largest number of entries any single scoring batch held.
	#[cfg(test)]
	peak_batch_entries: AtomicUsize,
	/// Largest encoded byte total any single scoring batch held.
	#[cfg(test)]
	peak_batch_bytes: AtomicUsize,
	/// Number of scoring batches run.
	#[cfg(test)]
	batches: AtomicUsize,
	/// Largest number of entries any single cursor page held.
	#[cfg(test)]
	peak_page_entries: AtomicUsize,
	/// Largest encoded byte total any single cursor page held.
	#[cfg(test)]
	peak_page_bytes: AtomicUsize,
	/// Cursor pages read, including the empty page that ends each range.
	#[cfg(test)]
	pages: AtomicUsize,
	/// Largest page-plus-batch total live at a scoring point.
	#[cfg(test)]
	peak_resident_bytes: AtomicUsize,
	/// Entries charged by [`PendingScan::charge_entry`], which is every entry
	/// every range of the scan read.
	#[cfg(test)]
	entries_read: AtomicUsize,
	/// Keys an out-of-band read resolved alongside a page — DiskANN's legacy
	/// ownership probe.
	#[cfg(test)]
	side_read_keys: AtomicUsize,
	/// Round trips those resolutions cost. One covers a whole cursor page, so
	/// this is what separates a batched check from a per-record one.
	#[cfg(test)]
	side_reads: AtomicUsize,
}

impl PendingScanStats {
	fn record_batch(&self, entries: usize, bytes: usize, page_bytes: usize) {
		#[cfg(test)]
		{
			self.peak_batch_entries.fetch_max(entries, Ordering::Relaxed);
			self.peak_batch_bytes.fetch_max(bytes, Ordering::Relaxed);
			self.peak_resident_bytes.fetch_max(bytes + page_bytes, Ordering::Relaxed);
			self.batches.fetch_add(1, Ordering::Relaxed);
		}
		#[cfg(not(test))]
		let _ = (entries, bytes, page_bytes);
	}

	fn record_page(&self, entries: usize, bytes: usize) {
		#[cfg(test)]
		{
			self.peak_page_entries.fetch_max(entries, Ordering::Relaxed);
			self.peak_page_bytes.fetch_max(bytes, Ordering::Relaxed);
			self.pages.fetch_add(1, Ordering::Relaxed);
		}
		#[cfg(not(test))]
		let _ = (entries, bytes);
	}

	fn record_entry(&self) {
		#[cfg(test)]
		self.entries_read.fetch_add(1, Ordering::Relaxed);
	}

	#[cfg(diskann)]
	fn record_side_read(&self, keys: usize, resident_bytes: usize) {
		#[cfg(test)]
		{
			self.side_read_keys.fetch_add(keys, Ordering::Relaxed);
			self.side_reads.fetch_add(1, Ordering::Relaxed);
			self.peak_resident_bytes.fetch_max(resident_bytes, Ordering::Relaxed);
		}
		#[cfg(not(test))]
		let _ = (keys, resident_bytes);
	}

	#[cfg(test)]
	pub(crate) fn peak_batch_entries(&self) -> usize {
		self.peak_batch_entries.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn peak_batch_bytes(&self) -> usize {
		self.peak_batch_bytes.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn batches(&self) -> usize {
		self.batches.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn peak_page_entries(&self) -> usize {
		self.peak_page_entries.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn peak_page_bytes(&self) -> usize {
		self.peak_page_bytes.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn pages(&self) -> usize {
		self.pages.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn peak_resident_bytes(&self) -> usize {
		self.peak_resident_bytes.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn entries_read(&self) -> usize {
		self.entries_read.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn side_read_keys(&self) -> usize {
		self.side_read_keys.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn side_reads(&self) -> usize {
		self.side_reads.load(Ordering::Relaxed)
	}
}
