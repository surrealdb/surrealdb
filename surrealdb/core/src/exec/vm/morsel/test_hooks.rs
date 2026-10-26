//! What tests observe of morsels: how many went to workers or were split off, how many partials
//! and lease faults morsels produced, and entry points that run a producer's rows through a
//! morsel the way a fan-out would.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use super::super::interp::State;
use super::super::partial::Partial;
use super::metrics::MorselMetrics;
use crate::exec::ExecutionContext;
use crate::exec::memory::SessionName;
use crate::exec::memory::runs::BreakerKind;

/// How many morsels this process has handed to workers, for tests that must know a pipeline ran
/// wider than on its own task.
pub(in crate::exec::vm) static HANDED_TO_WORKERS: AtomicUsize = AtomicUsize::new(0);

/// How many Range morsels this process has split off a running morsel.
pub(in crate::exec::vm) static MORSELS_SPLIT: AtomicUsize = AtomicUsize::new(0);

/// How many reads of stored data (records and key ranges) workers of a write that fans out made
/// through the statement's transaction. A write's workers read none.
static TXN_READS_ON_WORKERS: AtomicUsize = AtomicUsize::new(0);

/// How many records the storing stage of a write put from bytes the compute half encoded ahead.
static ENCODED_AHEAD: AtomicUsize = AtomicUsize::new(0);

tokio::task_local! {
	/// Set on the task of a worker that runs the compute half of a write.
	static ON_WRITE_WORKER: ();
}

/// Runs `body` as the task of a worker that runs the compute half of a write.
pub(super) async fn on_write_worker<F: std::future::Future>(body: F) -> F::Output {
	ON_WRITE_WORKER.scope((), body).await
}

/// A read of stored data through the statement's transaction was made: it is counted when a
/// worker of a write made it.
pub(crate) fn note_txn_read_on_worker() {
	if ON_WRITE_WORKER.try_with(|()| ()).is_ok() {
		TXN_READS_ON_WORKERS.fetch_add(1, Ordering::Relaxed);
	}
}

/// How many reads of stored data workers of a write that fans out have made through the
/// statement's transaction.
pub(in crate::exec::vm) fn txn_reads_on_workers() -> usize {
	TXN_READS_ON_WORKERS.load(Ordering::Relaxed)
}

static WRITES_ENDED: LazyLock<Mutex<HashMap<usize, Vec<crate::exec::vm::write::WriteAudit>>>> =
	LazyLock::new(Default::default);

/// A record of a write of the datastore `ctx` runs under ended, and its state held `audit`.
pub(crate) fn note_write_ended(ctx: &ExecutionContext, audit: crate::exec::vm::write::WriteAudit) {
	WRITES_ENDED
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.entry(datastore_of(ctx))
		.or_default()
		.push(audit);
}

/// The state the writes of the datastore `ctx` runs under held as each record ended, in order.
pub(in crate::exec::vm) fn writes_ended(
	ctx: &ExecutionContext,
) -> Vec<crate::exec::vm::write::WriteAudit> {
	WRITES_ENDED
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.get(&datastore_of(ctx))
		.cloned()
		.unwrap_or_default()
}

/// A record was put from bytes its compute half encoded ahead of the store.
pub(crate) fn note_encoded_ahead() {
	ENCODED_AHEAD.fetch_add(1, Ordering::Relaxed);
}

/// How many records have been put from bytes encoded ahead of the store.
pub(in crate::exec::vm) fn encoded_ahead() -> usize {
	ENCODED_AHEAD.load(Ordering::Relaxed)
}

/// What the morsels of one datastore have done, by the datastore, so a test that
/// compares against zero is not disturbed by tests running beside it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::exec::vm) struct Counts {
	/// Morsels handed to workers.
	pub(in crate::exec::vm) handed: usize,
	/// Morsels cut off a range by the policy's first split.
	pub(in crate::exec::vm) split: usize,
	/// Morsels donated by a running morsel at a chunk boundary, as the scans' metrics count them.
	pub(in crate::exec::vm) stolen: usize,
	/// Morsels a running morsel cut off itself because it would outlast its siblings, counted like
	/// `stolen`.
	pub(in crate::exec::vm) resplit: usize,
	/// Entries read from storage by Range morsels, on the coordinator or a worker.
	pub(in crate::exec::vm) scanned: usize,
	/// Chunks Range morsels read their entries in.
	pub(in crate::exec::vm) chunks: usize,
	/// Range morsels that parked with part of their range unread.
	pub(in crate::exec::vm) parked: usize,
	/// Range morsels that parked right after queueing a chunk the fan-out lease refused.
	pub(in crate::exec::vm) parked_after_refusal: usize,
	/// Entries a top-k pushdown rejected before they ran, on the coordinator or a worker.
	pub(in crate::exec::vm) pruned: usize,
	/// Sorted runs morsels built for a sort that keeps every row.
	pub(in crate::exec::vm) runs: usize,
	/// Merges of morsels' groups divided into partitions, and the partitions they ran.
	pub(in crate::exec::vm) partitioned: usize,
	pub(in crate::exec::vm) partitions: usize,
	/// The most row ends any one morsel returned, a maximum and not a sum.
	pub(in crate::exec::vm) most_ends: usize,
	/// Grants of the fan-out lease the query's memory refused.
	pub(in crate::exec::vm) refused: usize,
	/// States made for the workers of writes from a statement's.
	pub(in crate::exec::vm) states: usize,
	/// Batches of a write that ended before their last row because of what their candidates held.
	pub(in crate::exec::vm) cut_short: usize,
	/// The most bytes the candidates of a write held at once, not yet applied, a maximum and not a
	/// sum.
	pub(in crate::exec::vm) most_held: usize,
	/// Morsels a merge that takes results as morsels finish took before a morsel earlier in the
	/// scan had finished.
	pub(in crate::exec::vm) merged_early: usize,
	/// The bytes morsels charged to the fan-out lease for the terms they keep for a merge that
	/// folds them in order, a sum.
	pub(in crate::exec::vm) terms_charged: usize,
	/// The bytes running morsels charged to the fan-out lease for the chunks they queued in their
	/// outboxes, a sum.
	pub(in crate::exec::vm) output_charged: usize,
	/// The chunks running morsels queued in their outboxes that the fan-out lease refused.
	pub(in crate::exec::vm) output_refused: usize,
	/// The rows the coordinator took from running morsels, before they ended.
	pub(in crate::exec::vm) streamed: usize,
	/// Morsels whose results the coordinator held because a morsel before them had not finished.
	pub(in crate::exec::vm) collected_early: usize,
	/// The most results the coordinator held at once for the morsels before them, a maximum and
	/// not a sum.
	pub(in crate::exec::vm) most_results_held: usize,
	/// Times the results of two morsels next to one another were combined before either was next
	/// to what the statement had merged.
	pub(in crate::exec::vm) combined: usize,
	/// Times a scan asked the storage what its range holds as it opened.
	pub(in crate::exec::vm) asked_at_open: usize,
	/// Morsels cut off a range as its scan opened, before an entry of it was read.
	pub(in crate::exec::vm) opened: usize,
	/// Times a scan learned the share of its rows its filters pass from its selectivity probes as
	/// it opened.
	pub(in crate::exec::vm) probed: usize,
}

static COUNTS: LazyLock<Mutex<HashMap<usize, Counts>>> = LazyLock::new(Default::default);

/// What names the datastore `ctx` runs under: its node's compiled units, which no other
/// datastore shares while it lives. The address is free for a later datastore once the units
/// drop, so [`forget_datastore`] clears every record kept under it then.
fn datastore_of(ctx: &ExecutionContext) -> usize {
	ctx.ctx().compiled_units().map_or(0, |units| Arc::as_ptr(units) as *const () as usize)
}

/// Clear what the hooks keep for the datastore whose compiled units lie at `units`: a datastore
/// whose units take that address later starts with nothing recorded.
pub(crate) fn forget_datastore(units: usize) {
	fn clear<T>(map: &Mutex<HashMap<usize, T>>, units: usize) {
		map.lock().unwrap_or_else(|e| e.into_inner()).remove(&units);
	}
	clear(&WRITES_ENDED, units);
	clear(&COUNTS, units);
	clear(&DELAYS, units);
	clear(&METRICS, units);
	clear(&FAULTS, units);
	clear(&SEEDS, units);
}

fn bump(ctx: &ExecutionContext, change: impl FnOnce(&mut Counts)) {
	let mut counts = COUNTS.lock().unwrap_or_else(|e| e.into_inner());
	change(counts.entry(datastore_of(ctx)).or_default());
}

/// A merge took a morsel's results before a morsel earlier in the scan had finished.
pub(super) fn note_terms_charged(ctx: &ExecutionContext, bytes: usize) {
	bump(ctx, |counts| counts.terms_charged += bytes);
}

/// A running morsel charged `bytes` of a chunk it queued in its outbox to the fan-out lease.
pub(super) fn note_output_charged(ctx: &ExecutionContext, bytes: usize) {
	bump(ctx, |counts| counts.output_charged += bytes);
}

/// The fan-out lease refused a chunk a running morsel queued in its outbox.
pub(super) fn note_output_refused(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.output_refused += 1);
}

/// A Range morsel parked right after queueing a chunk the fan-out lease refused.
pub(super) fn note_parked_after_refusal(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.parked_after_refusal += 1);
}

/// The coordinator held the results of a morsel that finished ahead of an earlier one.
pub(super) fn note_collected_early(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.collected_early += 1);
}

/// The coordinator holds `held` results for the morsels before them.
pub(super) fn note_results_held(ctx: &ExecutionContext, held: usize) {
	bump(ctx, |counts| counts.most_results_held = counts.most_results_held.max(held));
}

/// The coordinator combined the results of two morsels next to one another.
pub(super) fn note_combined(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.combined += 1);
}

/// The coordinator took `rows` from a running morsel.
pub(super) fn note_streamed(ctx: &ExecutionContext, rows: usize) {
	bump(ctx, |counts| counts.streamed += rows);
}

pub(super) fn note_merged_early(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.merged_early += 1);
}

/// The delays, in milliseconds, that the morsels of a datastore that go to workers wait before they
/// run, in the order they are claimed or handed on, and how many have been claimed.
#[derive(Default)]
struct Delays {
	milliseconds: Vec<u64>,
	claimed: usize,
}

static DELAYS: LazyLock<Mutex<HashMap<usize, Delays>>> = LazyLock::new(Default::default);

/// Make the `n`th morsel of the datastore `ctx` runs under that goes to a worker wait `delays[n]`
/// milliseconds before it runs, from now on: a worker claims the earliest range morsel that waits,
/// and a batch is held back from the merge from the moment it is handed on, so the delays are by
/// position in the scan, and a delay reverses the order the morsels finish in.
pub(in crate::exec::vm) fn delay_morsels(ctx: &ExecutionContext, delays: &[u64]) {
	DELAYS.lock().unwrap_or_else(|e| e.into_inner()).insert(
		datastore_of(ctx),
		Delays {
			milliseconds: delays.to_vec(),
			claimed: 0,
		},
	);
}

/// The delay set for the morsel claimed next, if there is one; claiming it counts it.
pub(super) fn claim_delay(ctx: &ExecutionContext) -> Option<std::time::Duration> {
	let mut delays = DELAYS.lock().unwrap_or_else(|e| e.into_inner());
	delays.get_mut(&datastore_of(ctx)).and_then(|delays| {
		let delay = delays.milliseconds.get(delays.claimed).copied();
		delays.claimed += 1;
		delay.map(std::time::Duration::from_millis)
	})
}

/// A worker claimed a morsel: wait the delay set for it, if there is one.
pub(super) async fn delay_claimed(ctx: &ExecutionContext) {
	if let Some(delay) = claim_delay(ctx) {
		tokio::time::sleep(delay).await;
	}
}

/// The batches [`run_batch`](super::run::run_batch) ran for each datastore, in the order they
/// began: the rows the batch's paging skips first, `None` for a batch that is not paged, and the
/// rows the batch holds.
static BATCHES: LazyLock<Mutex<HashMap<usize, Vec<BatchRun>>>> = LazyLock::new(Default::default);

/// One batch [`run_batch`](super::run::run_batch) ran: the rows its paging skips first, `None` for
/// a batch that is not paged, and the rows it holds.
type BatchRun = (Option<usize>, usize);

/// A batch of `rows` rows began running, skipping `skip` of them first where it is paged.
pub(super) fn note_batch(ctx: &ExecutionContext, skip: Option<usize>, rows: usize) {
	BATCHES
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.entry(datastore_of(ctx))
		.or_default()
		.push((skip, rows));
}

/// The batches the datastore `ctx` runs under has run, in the order they began, as
/// [`note_batch`] records them.
pub(in crate::exec::vm) fn batches_of(ctx: &ExecutionContext) -> Vec<BatchRun> {
	BATCHES
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.get(&datastore_of(ctx))
		.cloned()
		.unwrap_or_default()
}

/// A morsel was handed to a worker.
pub(super) fn note_handed(ctx: &ExecutionContext) {
	HANDED_TO_WORKERS.fetch_add(1, Ordering::Relaxed);
	if let Some(faults) =
		FAULTS.lock().unwrap_or_else(|e| e.into_inner()).get_mut(&datastore_of(ctx))
		&& faults.grant_refused
	{
		faults.handed_after_refusal += 1;
	}
	bump(ctx, |counts| counts.handed += 1);
}

/// A state was made for a worker of a write from the statement's.
pub(super) fn note_worker_state_made(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.states += 1);
}

/// A batch of a write ended before its last row because of what its candidates held.
pub(super) fn note_cut_short(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.cut_short += 1);
}

/// The candidates of a write, not yet applied, hold `held` bytes.
pub(super) fn note_held(ctx: &ExecutionContext, held: usize) {
	bump(ctx, |counts| counts.most_held = counts.most_held.max(held));
}

/// A scan asked the storage what its range holds as it opened.
pub(super) fn note_asked_at_open(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.asked_at_open += 1);
}

/// A scan learned the share of its rows its filters pass from its selectivity probes as it opened.
pub(super) fn note_probed(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.probed += 1);
}

/// `parts` morsels were cut off a range as its scan opened.
pub(super) fn note_opened(ctx: &ExecutionContext, parts: usize) {
	bump(ctx, |counts| counts.opened += parts);
}

/// What a test seeded for a datastore: the storage's answer of a range a scan opens, and the share
/// of the range's rows its filters pass, known before the scan opens.
#[derive(Clone, Copy, Default)]
struct Seed {
	opened: Option<surrealdb_kvs::Estimate>,
	pass_ppm: Option<u32>,
}

static SEEDS: LazyLock<Mutex<HashMap<usize, Seed>>> = LazyLock::new(Default::default);

/// What a test seeded for the datastore `ctx` runs under, until the answer is dropped.
pub(in crate::exec::vm) struct Seeded(usize);

impl Drop for Seeded {
	fn drop(&mut self) {
		SEEDS.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.0);
	}
}

/// Seed, for every range a scan of the datastore `ctx` runs under opens, what the storage answers
/// of it (`opened`, in place of what its metadata says) and the share of its rows the filters pass
/// (`pass_ppm`, in rows a million).
pub(in crate::exec::vm) fn seed_opened(
	ctx: &ExecutionContext,
	opened: Option<surrealdb_kvs::Estimate>,
	pass_ppm: Option<u32>,
) -> Seeded {
	let datastore = datastore_of(ctx);
	SEEDS.lock().unwrap_or_else(|e| e.into_inner()).insert(
		datastore,
		Seed {
			opened,
			pass_ppm,
		},
	);
	Seeded(datastore)
}

fn seed(ctx: &ExecutionContext) -> Option<Seed> {
	SEEDS.lock().unwrap_or_else(|e| e.into_inner()).get(&datastore_of(ctx)).copied()
}

/// What a test seeded the storage to answer of a range a scan opens, if it did.
pub(super) fn seeded(ctx: &ExecutionContext) -> Option<surrealdb_kvs::Estimate> {
	seed(ctx).and_then(|seed| seed.opened)
}

/// The share of a range's rows a test seeded its filters to pass, as the planner's estimate.
pub(super) fn seeded_selectivity(
	ctx: &ExecutionContext,
) -> Option<crate::exec::ir::iface::Estimate> {
	let ppm = seed(ctx)?.pass_ppm?;
	Some(crate::exec::ir::iface::Estimate {
		rows: None,
		bytes: None,
		selectivity_ppm: Some(ppm),
		exact: false,
		source: crate::exec::ir::iface::EstimateSource::KvRange,
	})
}

/// `parts` morsels were cut off a range by the policy's first split.
pub(super) fn note_split(ctx: &ExecutionContext, parts: usize) {
	MORSELS_SPLIT.fetch_add(parts, Ordering::Relaxed);
	bump(ctx, |counts| counts.split += parts);
}

/// What the scans of each datastore did, summed as they ended.
static METRICS: LazyLock<Mutex<HashMap<usize, MorselMetrics>>> = LazyLock::new(Default::default);

/// A scan of the datastore `ctx` runs under ended with `metrics`.
pub(super) fn note_metrics(ctx: &ExecutionContext, metrics: &MorselMetrics) {
	METRICS
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.entry(datastore_of(ctx))
		.or_default()
		.absorb(metrics);
}

/// What the scans of the datastore `ctx` runs under have done, summed.
pub(in crate::exec::vm) fn metrics_of(ctx: &ExecutionContext) -> MorselMetrics {
	METRICS
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.get(&datastore_of(ctx))
		.cloned()
		.unwrap_or_default()
}

/// A Range morsel read `entries` entries.
pub(super) fn note_scanned(ctx: &ExecutionContext, entries: usize) {
	bump(ctx, |counts| counts.scanned += entries);
}

/// A Range morsel read one chunk of entries.
pub(super) fn note_chunk(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.chunks += 1);
}

/// A Range morsel's top-k pushdown rejected `entries` entries before they ran.
pub(super) fn note_pruned(ctx: &ExecutionContext, entries: usize) {
	if entries > 0 {
		bump(ctx, |counts| counts.pruned += entries);
	}
}

/// A morsel sorted its entries into a run.
pub(super) fn note_run(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.runs += 1);
}

/// The groups of morsels were merged in `partitions` partitions.
pub(super) fn note_partitioned(ctx: &ExecutionContext, partitions: usize) {
	bump(ctx, |counts| {
		counts.partitioned += 1;
		counts.partitions += partitions;
	});
}

/// A morsel returned `ends` row ends.
pub(super) fn note_ends(ctx: &ExecutionContext, ends: usize) {
	bump(ctx, |counts| counts.most_ends = counts.most_ends.max(ends));
}

/// Start counting the most row ends a morsel returns afresh for the datastore `ctx` runs under. A
/// maximum cannot be differenced, and a datastore's key is an address that a later datastore may
/// reuse once the first is dropped.
pub(in crate::exec::vm) fn forget_ends(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.most_ends = 0);
}

/// The fan-out lease of the datastore `ctx` runs under refused a grant its query's memory could not
/// admit.
pub(super) fn note_refused(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.refused += 1);
}

/// A Range morsel parked.
pub(super) fn note_parked(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.parked += 1);
}

/// What the morsels of the datastore `ctx` runs under have done; the morsels donated and re-split
/// are those the scans' metrics count, a worker's as well as the coordinator's.
pub(in crate::exec::vm) fn counts_of(ctx: &ExecutionContext) -> Counts {
	let counted = COUNTS
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.get(&datastore_of(ctx))
		.copied()
		.unwrap_or_default();
	let scans = metrics_of(ctx);
	Counts {
		stolen: scans.stolen as usize,
		resplit: scans.resplit as usize,
		..counted
	}
}

/// The reads each datastore's morsels are made to fail, keyed by datastore as in [`Counts`].
#[derive(Clone, Copy, Debug, Default)]
struct Faults {
	/// A split of a range finds no boundary, because the datastore fails it.
	splits: bool,
	/// A cursor refuses to narrow to the part its morsel keeps.
	narrows: bool,
	/// The fan-out lease refuses its grants once `grants_left` have been made.
	grants: bool,
	/// The grants still made before the lease refuses, while `grants` is set.
	grants_left: usize,
	/// The reads failed since the faults were last set.
	refused: usize,
	/// The fan-out lease has refused a grant since the faults were last set.
	grant_refused: bool,
	/// The batches and workers handed on since the lease refused a grant.
	handed_after_refusal: usize,
}

static FAULTS: LazyLock<Mutex<HashMap<usize, Faults>>> = LazyLock::new(Default::default);

/// Which reads of a datastore fail while this is held.
pub(in crate::exec::vm) struct Failing {
	datastore: usize,
	change: fn(&mut Faults, bool),
}

impl Failing {
	/// How many reads have failed since this was made.
	pub(in crate::exec::vm) fn refused(&self) -> usize {
		FAULTS
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.get(&self.datastore)
			.map_or(0, |faults| faults.refused)
	}

	/// How many batches and workers were handed on after the fan-out lease refused a grant, since
	/// this was made. A refusal revokes the lease, so none is.
	#[cfg(all(feature = "kv-mem", feature = "kv-surrealkv"))]
	pub(in crate::exec::vm) fn handed_after_refusal(&self) -> usize {
		FAULTS
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.get(&self.datastore)
			.map_or(0, |faults| faults.handed_after_refusal)
	}
}

impl Drop for Failing {
	fn drop(&mut self) {
		let mut faults = FAULTS.lock().unwrap_or_else(|e| e.into_inner());
		(self.change)(faults.entry(self.datastore).or_default(), false);
	}
}

fn failing(ctx: &ExecutionContext, change: fn(&mut Faults, bool)) -> Failing {
	let datastore = datastore_of(ctx);
	let mut faults = FAULTS.lock().unwrap_or_else(|e| e.into_inner());
	let entry = faults.entry(datastore).or_default();
	entry.refused = 0;
	entry.grant_refused = false;
	entry.handed_after_refusal = 0;
	change(entry, true);
	drop(faults);
	Failing {
		datastore,
		change,
	}
}

/// Every split of a range the morsels of the datastore `ctx` runs under ask for fails, until the
/// answer is dropped.
pub(in crate::exec::vm) fn fail_splits(ctx: &ExecutionContext) -> Failing {
	failing(ctx, |faults, on| faults.splits = on)
}

/// Every narrowing of a cursor the morsels of the datastore `ctx` runs under ask for fails, until
/// the answer is dropped.
#[cfg(feature = "kv-surrealkv")]
pub(in crate::exec::vm) fn fail_narrows(ctx: &ExecutionContext) -> Failing {
	failing(ctx, |faults, on| faults.narrows = on)
}

/// Every grant of the fan-out lease the morsels of the datastore `ctx` runs under ask for, past the
/// first `after`, is refused, until the answer is dropped. The refusal revokes the lease as a
/// grant the query's memory cannot admit does, whatever the grant's bytes.
#[cfg(all(feature = "kv-mem", feature = "kv-surrealkv"))]
pub(in crate::exec::vm) fn fail_grants(ctx: &ExecutionContext, after: usize) -> Failing {
	let failing = failing(ctx, |faults, on| faults.grants = on);
	FAULTS
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.entry(failing.datastore)
		.or_default()
		.grants_left = after;
	failing
}

/// Whether the read `failing` picks is to fail over the datastore `ctx` runs under, counting it
/// when it is.
fn fails(ctx: &ExecutionContext, failing: fn(&Faults) -> bool) -> bool {
	let mut faults = FAULTS.lock().unwrap_or_else(|e| e.into_inner());
	let Some(faults) = faults.get_mut(&datastore_of(ctx)) else {
		return false;
	};
	let fails = failing(faults);
	if fails {
		faults.refused += 1;
	}
	fails
}

/// Whether a split of a range is to fail.
pub(super) fn split_fails(ctx: &ExecutionContext) -> bool {
	fails(ctx, |faults| faults.splits)
}

/// Whether the narrowing of a cursor is to fail.
pub(super) fn narrow_fails(ctx: &ExecutionContext) -> bool {
	fails(ctx, |faults| faults.narrows)
}

/// Whether a grant of the fan-out lease is to be refused, counting it when it is.
pub(super) fn grant_fails(ctx: &ExecutionContext) -> bool {
	let mut faults = FAULTS.lock().unwrap_or_else(|e| e.into_inner());
	let Some(faults) = faults.get_mut(&datastore_of(ctx)).filter(|faults| faults.grants) else {
		return false;
	};
	if faults.grants_left > 0 {
		faults.grants_left -= 1;
		return false;
	}
	faults.refused += 1;
	faults.grant_refused = true;
	true
}

/// A statement that waits for a place it is never given, which the scans running while it lives see
/// as another statement waiting.
#[cfg(all(feature = "kv-mem", feature = "kv-surrealkv"))]
pub(in crate::exec::vm) struct Waiter(
	#[expect(dead_code, reason = "held for its registration")] super::places::Statement,
);

/// Register a statement with the node's places and leave it waiting for one.
#[cfg(all(feature = "kv-mem", feature = "kv-surrealkv"))]
pub(in crate::exec::vm) fn waiting_statement() -> Waiter {
	let statement = super::places::register();
	let config = crate::exec::config::ExecConfig {
		morsel_oltp_reserve: usize::MAX,
		..Default::default()
	};
	assert!(
		statement.acquire(crate::exec::planner::placement::Policy::from_config(&config)).is_none()
	);
	Waiter(statement)
}

/// How many partials morsels have handed their merge, by the session of the store they spilled to
/// and then by breaker (sort, groups), for tests that must know their own morsels spilled while
/// others run beside them.
static BATCH_PARTIALS: LazyLock<Mutex<HashMap<SessionName, [usize; 2]>>> =
	LazyLock::new(Default::default);

/// Where `kind`'s count sits in a [`BATCH_PARTIALS`] entry.
fn partial_slot(kind: BreakerKind) -> usize {
	usize::from(kind == BreakerKind::Groups)
}

/// `spilled` are the partials a morsel over `state` hands its merge.
pub(super) fn note_partials(state: &State, spilled: &[Partial]) {
	if let Some(store) = state.memory.as_ref().and_then(|memory| memory.store()) {
		let mut counted = BATCH_PARTIALS.lock().unwrap_or_else(|e| e.into_inner());
		for partial in spilled {
			counted.entry(store.session().clone()).or_default()[partial_slot(partial.spec.kind)] +=
				1;
		}
	}
}

/// How many partials of `kind` (a sort or groups) morsels have handed their merge over the store
/// `session` names.
pub(in crate::exec::vm) fn batch_partials(session: &SessionName, kind: BreakerKind) -> usize {
	BATCH_PARTIALS
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.get(session)
		.map_or(0, |counted| counted[partial_slot(kind)])
}

/// A way a morsel's accounting left the lease counting less than its query holds.
#[derive(Clone, Copy)]
pub(in crate::exec::vm) enum LeaseFault {
	/// A morsel's lease reading fell while its buffers became partials.
	Released,
	/// The merge finished while the lease counted less than the statement's sorts and groups hold.
	Uncounted,
}

/// How many times each [`LeaseFault`] (released, uncounted) was seen by the morsels and merges
/// over a store's session.
static LEASE_FAULTS: LazyLock<Mutex<HashMap<SessionName, [usize; 2]>>> =
	LazyLock::new(Default::default);

pub(super) fn note_lease_fault(state: &State, fault: LeaseFault) {
	if let Some(store) = state.memory.as_ref().and_then(|memory| memory.store()) {
		LEASE_FAULTS
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.entry(store.session().clone())
			.or_default()[fault as usize] += 1;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A read of stored data through the transaction is counted where a worker of a write makes it,
	/// and nowhere else, so a worker that read the record-type marker would fail the tests that
	/// assert none was read.
	#[tokio::test]
	async fn a_read_is_counted_on_a_write_worker_only() {
		let before = txn_reads_on_workers();
		note_txn_read_on_worker();
		assert_eq!(txn_reads_on_workers(), before, "a read off the workers was counted");
		on_write_worker(async {
			note_txn_read_on_worker();
		})
		.await;
		assert!(txn_reads_on_workers() > before, "a read on a worker was not counted");
	}
}
