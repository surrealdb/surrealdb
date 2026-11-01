//! What the one build protocol does differently for each index kind.
//!
//! The protocol — its states, keys, fences and transitions — is the same for every kind. What a
//! kind changes is confined to the hooks below, and this module is the only place the build
//! dispatches on [`Index`]:
//!
//! * **The baseline policy** ([`Kind::baseline`]): whether the scan's view of a record is all its
//!   baseline, or a COUNT build also merges the queued old states of records the scan passed across
//!   checkpointed spans.
//! * **The doc-ID deferral** ([`Kind::defers_doc_ids`]): whether a queued delete leaves the table's
//!   shared doc-ID mapping for the build to reclaim once no sibling still needs it.
//! * **The per-batch context** ([`Kind::batch_context`]): what a kind loads once per batch rather
//!   than once per record — a full-text index and its analyzer.
//! * **The initial-batch sink** ([`Kind::initial_batch_sink`]): lets a kind consume an initial scan
//!   batch directly, in the build transaction of the batch that read it. The default writes every
//!   record through the kind's ordinary write path. A sink may also run a pass over what it built
//!   once the scan ends, before the scan is marked complete ([`InitialBatchSink::post_scan_step`],
//!   [`Kind::has_post_scan_pass`]).
//!
//! and one step of cleaning an index before its scan: a cleared B-tree, spatial or vector index
//! records the shapes of the values it holds ([`Kind::tracks_value_shapes`]).
//!
//! Every hook a batch calls receives the key range that batch covers ([`BatchRange`]), and none
//! keeps state across batches beyond what its range says, except the COUNT merge, whose progress
//! the builder carries. A scan later split by key range can call them range by range.

use std::sync::atomic::AtomicBool;
#[cfg(diskann)]
use std::sync::atomic::Ordering;

use anyhow::Result;

use crate::catalog::{Index, IndexDefinition, TableId};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::index::IndexOperation;
#[cfg(diskann)]
use crate::idx::trees::diskann::index::{
	BuildTxGuard, DISKANN_DIRECT_MAX_VECTORS, DirectApplyGuard,
};
#[cfg(diskann)]
use crate::idx::trees::store::diskann::SharedDiskAnnIndex;
#[cfg(diskann)]
use crate::idx::trees::vector::SerializedVector;
use crate::kvs::Transaction;
use crate::val::{RecordIdKey, Value};

/// The key range one scan batch covers: the keys of its first and last records, inclusive.
#[derive(Clone, Copy)]
#[cfg_attr(not(diskann), allow(dead_code))]
pub(super) struct BatchRange<'a> {
	pub(super) first: &'a [u8],
	pub(super) last: &'a [u8],
}

/// How a build fixes the state its scan indexes a record from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BaselinePolicy {
	/// The scan indexes a record as it reads it, or as the record's first queued mutation found
	/// it. Replay applies the queued mutations on top.
	Scan,
	/// As [`Self::Scan`], and the build also baselines the records the scan passed that are gone
	/// by the time it reads them, from their first queued mutation, merging those baselines
	/// across checkpointed spans. A COUNT index needs it, because a gone record still counted
	/// once.
	MergeQueuedSpans,
}

/// One index kind, as the build protocol's hooks see it.
#[derive(Clone, Copy)]
pub(super) struct Kind<'a>(&'a IndexDefinition);

impl<'a> Kind<'a> {
	pub(super) fn of(ix: &'a IndexDefinition) -> Self {
		Self(ix)
	}

	/// The baseline policy hook.
	pub(super) fn baseline(self) -> BaselinePolicy {
		match self.0.index {
			Index::Count(_) => BaselinePolicy::MergeQueuedSpans,
			Index::Idx
			| Index::Uniq
			| Index::FullText(_)
			| Index::Hnsw(_)
			| Index::DiskAnn(_)
			| Index::Spatial => BaselinePolicy::Scan,
		}
	}

	/// The doc-ID deferral hook: whether this index consumes the table's shared doc-ID space, so
	/// a queued delete defers the removal of a record's mapping to the build, which reclaims it
	/// once no sibling consumer still building needs it.
	pub(super) fn defers_doc_ids(self) -> bool {
		self.0.uses_shared_doc_ids()
	}

	/// Whether cleaning this index before its scan starts recording the shapes of the values it
	/// holds, so every write the scan admits records them too.
	pub(super) fn tracks_value_shapes(self) -> bool {
		matches!(
			self.0.index,
			Index::Idx | Index::Uniq | Index::Spatial | Index::Hnsw(_) | Index::DiskAnn(_)
		)
	}

	/// Whether batches of this kind's initial scan may be indexed concurrently, each in a
	/// transaction of its own: indexing a record writes only entries for that record and its
	/// doc-ID mapping, and indexing it again writes the same, so batches commit in any order and a
	/// scan resumed before a batch past its checkpoint indexes that batch's records once more
	/// without changing the index.
	#[cfg_attr(all(target_family = "wasm", target_os = "unknown"), allow(dead_code))]
	pub(super) fn scans_in_parallel(self) -> bool {
		matches!(self.0.index, Index::Idx | Index::Uniq)
	}

	/// Whether the initial scan can index a record with no queued mutation without decoding it:
	/// an unguarded COUNT index counts every record, whatever it holds.
	pub(super) fn indexes_records_unread(self) -> bool {
		matches!(self.0.index, Index::Count(_)) && self.0.count_cond.is_none()
	}

	/// Whether this kind's initial-batch sink runs a pass over what it built once the scan ends
	/// ([`InitialBatchSink::post_scan_step`]).
	pub(super) fn has_post_scan_pass(self) -> bool {
		matches!(self.0.index, Index::DiskAnn(_))
	}

	/// The name of the analyzer a full-text index analyses with, for the build to load the one
	/// its statement sees. `None` for every other kind.
	pub(super) fn analyzer(self) -> Option<&'a str> {
		if let Index::FullText(p) = &self.0.index {
			Some(p.analyzer.as_str())
		} else {
			None
		}
	}

	/// The per-batch context hook: what indexing one batch loads once. `analyzer` is the one the
	/// statement handed over, if any.
	pub(super) async fn batch_context(
		&self,
		ctx: &FrozenContext,
		ix_key: (crate::catalog::NamespaceId, crate::catalog::DatabaseId),
		analyzer: Option<std::sync::Arc<crate::catalog::AnalyzerDefinition>>,
		_range: Option<BatchRange<'_>>,
	) -> Result<BatchContext> {
		let fulltext = if let Index::FullText(_) = self.0.index {
			IndexOperation::create_fulltext_index(ctx, ix_key.0, ix_key.1, self.0, analyzer).await?
		} else {
			None
		};
		Ok(BatchContext {
			fulltext,
		})
	}

	/// The initial-batch sink hook. A DiskANN index inserts the batch's records straight into its
	/// graph; every other kind writes each record through its ordinary write path.
	pub(super) async fn initial_batch_sink(
		&self,
		ctx: &FrozenContext,
		tb: TableId,
		ikb: &IndexKeyBase,
	) -> Result<InitialBatchSink> {
		#[cfg(test)]
		if test_sink::is_off(ctx.node_id()) {
			return Ok(InitialBatchSink::Pending);
		}
		match &self.0.index {
			#[cfg(diskann)]
			Index::DiskAnn(p) => {
				let index = ctx
					.get_index_stores()
					.get_index_diskann(self.0.format_version, tb, ikb, p)
					.await?;
				Ok(InitialBatchSink::DiskAnn(index))
			}
			_ => {
				let _ = (ctx, tb, ikb);
				Ok(InitialBatchSink::Pending)
			}
		}
	}
}

/// What one batch of a build loads once, for every record it indexes.
pub(super) struct BatchContext {
	/// The full-text index a full-text build writes, analysing with the statement's analyzer.
	pub(super) fulltext: Option<FullTextIndex>,
}

/// Where the records of an initial scan batch with no queued mutation go.
pub(super) enum InitialBatchSink {
	/// Through the kind's ordinary write path, one record at a time.
	Pending,
	/// Straight into a DiskANN graph, as one frame per batch.
	#[cfg(diskann)]
	DiskAnn(SharedDiskAnnIndex),
}

/// The records a sink takes from one scan batch, cut where a batch would exceed what one frame
/// inserts.
pub(super) struct SinkRecords {
	#[cfg(diskann)]
	records: Vec<(RecordIdKey, Vec<SerializedVector>)>,
	/// The vectors held by `records`.
	#[cfg_attr(not(diskann), allow(dead_code))]
	vectors: usize,
	/// The most vectors one frame inserts.
	#[cfg_attr(not(diskann), allow(dead_code))]
	cap: usize,
}

impl InitialBatchSink {
	/// Whether this sink takes records itself.
	pub(super) fn takes_records(&self) -> bool {
		match self {
			Self::Pending => false,
			#[cfg(diskann)]
			Self::DiskAnn(_) => true,
		}
	}

	/// An empty set of records for this sink, on the node `node`.
	pub(super) fn records(&self, node: uuid::Uuid) -> SinkRecords {
		#[cfg(diskann)]
		let cap = DISKANN_DIRECT_MAX_VECTORS;
		#[cfg(not(diskann))]
		let cap = usize::MAX;
		#[cfg(test)]
		let cap = test_sink::cap(node).unwrap_or(cap);
		#[cfg(not(test))]
		let _ = node;
		SinkRecords {
			#[cfg(diskann)]
			records: Vec::new(),
			vectors: 0,
			cap,
		}
	}

	/// Takes the record `id`, whose indexed values are `values`, into `records` — unless it would
	/// take them past what one frame inserts while they already hold something, in which case the
	/// batch is cut before it and this answers `false`. A record alone over the cap is taken,
	/// so every batch makes progress.
	pub(super) fn take(
		&self,
		records: &mut SinkRecords,
		id: &RecordIdKey,
		values: Option<Vec<Value>>,
	) -> Result<bool> {
		match self {
			Self::Pending => {
				let _ = (records, id, values);
				Ok(false)
			}
			#[cfg(diskann)]
			Self::DiskAnn(index) => {
				let vectors = match values {
					Some(values) => index.vectors_of(values)?,
					None => Vec::new(),
				};
				if !fits(records.vectors, vectors.len(), records.cap) {
					return Ok(false);
				}
				// A record with no vector indexes nothing: taken, so the batch moves past it, but
				// not handed to the frame.
				if !vectors.is_empty() {
					records.vectors += vectors.len();
					records.records.push((id.clone(), vectors));
				}
				Ok(true)
			}
		}
	}

	/// Applies `records` to the index in `ctx`'s transaction, the build transaction of the batch
	/// covering `range`. Returns the frame, still open until the caller commits that transaction,
	/// or `None` when there was nothing to apply.
	///
	/// On an error the frame is already undone, and the transaction is the caller's to cancel.
	///
	/// The frame stops only once the build is `aborted`, or when the memory threshold the scan
	/// checks is crossed: a deadline or cancellation the build's context inherits is not the
	/// frame's to act on, since the build stops at its own checkpoints.
	pub(super) async fn apply<'s>(
		&'s self,
		ctx: &FrozenContext,
		aborted: &AtomicBool,
		range: BatchRange<'_>,
		records: SinkRecords,
	) -> Result<Option<SinkFrame<'s>>> {
		#[cfg(test)]
		test_sink::record_apply(ctx.node_id());
		match self {
			Self::Pending => {
				let _ = (ctx, aborted, range, records);
				Ok(None)
			}
			#[cfg(diskann)]
			Self::DiskAnn(index) => {
				if records.records.is_empty() {
					return Ok(None);
				}
				// Guards the build transaction from before the frame can spawn a task, so a
				// panic from here on cancels it only once the frame's tasks have exited.
				let mut tx_guard = BuildTxGuard::new(ctx.tx());
				let env = SinkEnv {
					inner: &**ctx,
					aborted,
				};
				match index
					.apply_direct(&env, (range.first, range.last), records.records, &mut tx_guard)
					.await
				{
					Ok(frame) => Ok(Some(SinkFrame {
						frame,
						tx_guard,
					})),
					Err(e) => {
						// The frame is undone and its tasks have exited: the caller cancels the
						// transaction inline.
						tx_guard.disarm();
						Err(e)
					}
				}
			}
		}
	}
}

/// The environment a sink frame runs in: the build's write context, done once the build is
/// aborted and failing once the memory threshold is crossed, as the scan's own checks are.
#[cfg(diskann)]
struct SinkEnv<'a> {
	inner: &'a dyn crate::idx::env::IndexEnv,
	aborted: &'a AtomicBool,
}

#[cfg(diskann)]
impl crate::catalog::providers::CancellationProbe for SinkEnv<'_> {
	fn expect_not_timedout(&self) -> crate::catalog::providers::BoxProviderFut<'_, Result<()>> {
		let aborted = self.aborted.load(Ordering::Acquire);
		Box::pin(async move {
			if aborted {
				return Err(common::EngineError::QueryCancelled.into());
			}
			Ok(())
		})
	}
}

#[cfg(diskann)]
impl crate::idx::env::IndexEnv for SinkEnv<'_> {
	fn tx(&self) -> std::sync::Arc<Transaction> {
		self.inner.tx()
	}

	fn is_done(&self, _count: Option<usize>) -> crate::idx::env::BoxEnvFut<'_> {
		let aborted = self.aborted.load(Ordering::Acquire);
		Box::pin(async move {
			if crate::mem::ALLOC.is_beyond_threshold() {
				return Err(anyhow::Error::new(
					crate::kvs::DatastoreError::QueryBeyondMemoryThreshold,
				));
			}
			Ok(aborted)
		})
	}

	fn index_stores(&self) -> &crate::idx::trees::store::IndexStores {
		self.inner.index_stores()
	}

	fn node_id(&self) -> uuid::Uuid {
		self.inner.node_id()
	}

	fn sequences(&self) -> Result<&crate::kvs::sequences::Sequences> {
		self.inner.sequences()
	}

	fn config(&self) -> &crate::idx::config::IdxConfig {
		self.inner.config()
	}

	fn places(&self) -> crate::idx::places::Ticket {
		self.inner.places()
	}
}

/// One step of a sink's post-scan pass.
pub(super) struct PostScanStep<'s> {
	/// The frame the step ran in, until its transaction ends; `None` when it had nothing to do.
	pub(super) frame: Option<SinkFrame<'s>>,
	/// Where the next step starts, or `None` once the pass is done.
	pub(super) next: Option<u64>,
}

impl InitialBatchSink {
	/// One step, from `from`, of the pass the sink runs over what it built once the scan has
	/// fed it every batch, in `ctx`'s transaction. A DiskANN graph re-prunes the adjacency lists
	/// of one chunk of its elements; every other sink has no such pass. The pass keeps no state
	/// beyond `from`, and a step it runs again changes nothing it changed before, so a pass that
	/// is interrupted starts again from the beginning.
	pub(super) async fn post_scan_step<'s>(
		&'s self,
		ctx: &FrozenContext,
		aborted: &AtomicBool,
		from: u64,
	) -> Result<PostScanStep<'s>> {
		match self {
			Self::Pending => {
				let _ = (ctx, aborted, from);
				Ok(PostScanStep {
					frame: None,
					next: None,
				})
			}
			#[cfg(diskann)]
			Self::DiskAnn(index) => {
				let mut tx_guard = BuildTxGuard::new(ctx.tx());
				let env = SinkEnv {
					inner: &**ctx,
					aborted,
				};
				match index.reprune_direct(&env, from, &mut tx_guard).await {
					Ok((frame, next)) => Ok(PostScanStep {
						frame: match frame {
							Some(frame) => Some(SinkFrame {
								frame,
								tx_guard,
							}),
							None => {
								tx_guard.disarm();
								None
							}
						},
						next,
					}),
					Err(e) => {
						tx_guard.disarm();
						Err(e)
					}
				}
			}
		}
	}
}

/// Whether a frame holding `held` vectors takes a record of `adding` more under a cap of `cap`: it
/// does while that keeps it within the cap, and always when it holds none yet.
#[cfg_attr(not(diskann), allow(dead_code))]
pub(super) fn fits(held: usize, adding: usize, cap: usize) -> bool {
	held == 0 || held + adding <= cap
}

/// A sink frame applied in a build transaction that has not committed yet. It holds the index's
/// graph lock until it ends.
pub(super) struct SinkFrame<'s> {
	#[cfg(diskann)]
	frame: DirectApplyGuard<'s>,
	#[cfg(diskann)]
	tx_guard: BuildTxGuard,
	#[cfg(not(diskann))]
	_never: std::marker::PhantomData<&'s ()>,
}

impl SinkFrame<'_> {
	/// Ends the frame once the build transaction has committed.
	pub(super) async fn finish(self) {
		#[cfg(diskann)]
		{
			self.tx_guard.disarm();
			self.frame.finish().await;
		}
	}

	/// Ends the frame when the build transaction will not commit, then cancels `tx`.
	pub(super) async fn abort(self, tx: &Transaction) {
		#[cfg(diskann)]
		{
			self.frame.abort().await;
			let _ = tx.cancel().await;
			self.tx_guard.disarm();
		}
		#[cfg(not(diskann))]
		let _ = tx;
	}
}

/// Test overrides of the initial-batch sink for the builds of one node: a smaller cap, so a test
/// cuts its scan batches without an index over more than a thousand columns, or no sink at all,
/// so a test of the materialiser's pass still gets pendings from a build's scan.
#[cfg(test)]
pub(super) mod test_sink {
	use std::collections::HashMap;
	use std::sync::{LazyLock, Mutex};

	#[derive(Clone, Copy)]
	enum Mode {
		Cap(usize),
		Off,
	}

	static MODES: LazyLock<Mutex<HashMap<uuid::Uuid, Mode>>> = LazyLock::new(Default::default);

	fn mode(node: uuid::Uuid) -> Option<Mode> {
		MODES.lock().unwrap_or_else(|p| p.into_inner()).get(&node).copied()
	}

	pub(in crate::kvs::index) fn cap(node: uuid::Uuid) -> Option<usize> {
		match mode(node) {
			Some(Mode::Cap(cap)) => Some(cap),
			_ => None,
		}
	}

	pub(in crate::kvs::index) fn is_off(node: uuid::Uuid) -> bool {
		matches!(mode(node), Some(Mode::Off))
	}

	fn set(node: uuid::Uuid, mode: Mode) -> Guard {
		MODES.lock().unwrap_or_else(|p| p.into_inner()).insert(node, mode);
		Guard(node)
	}

	/// Caps the sink frames of `node`'s builds at `cap` vectors until the guard drops.
	pub(in crate::kvs::index) fn set_cap(node: uuid::Uuid, cap: usize) -> Guard {
		set(node, Mode::Cap(cap))
	}

	/// Sends every record of `node`'s builds through the kind's write path until the guard drops.
	pub(in crate::kvs::index) fn off(node: uuid::Uuid) -> Guard {
		set(node, Mode::Off)
	}

	pub(in crate::kvs::index) struct Guard(uuid::Uuid);

	impl Drop for Guard {
		fn drop(&mut self) {
			MODES.lock().unwrap_or_else(|p| p.into_inner()).remove(&self.0);
		}
	}

	/// Per node: the scan batch size its builds use in place of their own sizing, and how many
	/// times its builds applied a sink and committed an initial batch.
	#[derive(Clone, Copy, Default)]
	pub(in crate::kvs::index) struct Counts {
		pub(in crate::kvs::index) applied: usize,
		pub(in crate::kvs::index) committed: usize,
	}

	static SCAN: LazyLock<Mutex<HashMap<uuid::Uuid, u32>>> = LazyLock::new(Default::default);
	static COUNTS: LazyLock<Mutex<HashMap<uuid::Uuid, Counts>>> = LazyLock::new(Default::default);

	pub(in crate::kvs::index) fn scan_batch(node: uuid::Uuid) -> Option<u32> {
		SCAN.lock().unwrap_or_else(|p| p.into_inner()).get(&node).copied()
	}

	/// Scans `node`'s builds in batches of `records` until the guard drops.
	pub(in crate::kvs::index) fn set_scan_batch(node: uuid::Uuid, records: u32) -> ScanGuard {
		SCAN.lock().unwrap_or_else(|p| p.into_inner()).insert(node, records);
		ScanGuard(node)
	}

	pub(in crate::kvs::index) struct ScanGuard(uuid::Uuid);

	impl Drop for ScanGuard {
		fn drop(&mut self) {
			SCAN.lock().unwrap_or_else(|p| p.into_inner()).remove(&self.0);
		}
	}

	static HOLD_AFTER: LazyLock<Mutex<HashMap<uuid::Uuid, usize>>> =
		LazyLock::new(Default::default);

	/// Holds `node`'s builds at the batch commit that follows their first `commits` committed
	/// batches, once an injected conflict is armed at the batch site: until then the conflict
	/// is not consumed. The hold starts at a batch boundary, however fast the scan runs.
	pub(in crate::kvs::index) fn hold_after_commits(node: uuid::Uuid, commits: usize) -> HoldGuard {
		HOLD_AFTER.lock().unwrap_or_else(|p| p.into_inner()).insert(node, commits);
		HoldGuard(node)
	}

	pub(in crate::kvs::index) struct HoldGuard(uuid::Uuid);

	impl Drop for HoldGuard {
		fn drop(&mut self) {
			HOLD_AFTER.lock().unwrap_or_else(|p| p.into_inner()).remove(&self.0);
		}
	}

	/// The injected conflict at the initial-batch commit site, deferred while a
	/// [`hold_after_commits`] has not yet seen its commits.
	pub(in crate::kvs::index) fn maybe_hold_batch(node: uuid::Uuid) -> anyhow::Result<()> {
		let after = HOLD_AFTER.lock().unwrap_or_else(|p| p.into_inner()).get(&node).copied();
		if after.is_some_and(|after| counts(node).committed < after) {
			return Ok(());
		}
		crate::kvs::testing::maybe_inject_retryable_conflict(
			crate::kvs::testing::RetryableConflictSite::ConcurrentIndexInitialBatch,
			node,
		)
	}

	pub(in crate::kvs::index) fn record_apply(node: uuid::Uuid) {
		COUNTS.lock().unwrap_or_else(|p| p.into_inner()).entry(node).or_default().applied += 1;
	}

	pub(in crate::kvs::index) fn record_commit(node: uuid::Uuid) {
		COUNTS.lock().unwrap_or_else(|p| p.into_inner()).entry(node).or_default().committed += 1;
	}

	/// What `node`'s builds have counted so far.
	pub(in crate::kvs::index) fn counts(node: uuid::Uuid) -> Counts {
		COUNTS.lock().unwrap_or_else(|p| p.into_inner()).get(&node).copied().unwrap_or_default()
	}
}

#[cfg(test)]
mod tests {
	use super::fits;

	/// T-14 (sink cut): a frame takes records while they fit under the cap, stops before the one
	/// that would take it past, and always takes a record when it holds none, however many vectors
	/// that record has.
	#[test]
	fn a_sink_frame_is_cut_at_the_vector_cap() {
		assert!(fits(0, 5000, 1024), "a first record over the cap is taken alone");
		assert!(fits(1000, 24, 1024));
		assert!(!fits(1000, 25, 1024));
		assert!(fits(1024, 0, 1024), "a record without vectors never cuts the batch");
	}
}
