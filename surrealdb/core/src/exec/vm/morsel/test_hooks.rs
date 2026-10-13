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
	/// Range morsels that parked with part of their range unread.
	pub(in crate::exec::vm) parked: usize,
	/// Entries a top-k pushdown rejected before they ran, on the coordinator or a worker.
	pub(in crate::exec::vm) pruned: usize,
	/// Sorted runs morsels built for a sort that keeps every row.
	pub(in crate::exec::vm) runs: usize,
	/// Merges of morsels' groups divided into partitions, and the partitions they ran.
	pub(in crate::exec::vm) partitioned: usize,
	pub(in crate::exec::vm) partitions: usize,
	/// The most row ends any one morsel returned, a maximum and not a sum.
	pub(in crate::exec::vm) most_ends: usize,
}

static COUNTS: LazyLock<Mutex<HashMap<usize, Counts>>> = LazyLock::new(Default::default);

/// What names the datastore `ctx` runs under: its node's compiled units, which no other
/// datastore shares.
fn datastore_of(ctx: &ExecutionContext) -> usize {
	ctx.ctx().compiled_units().map_or(0, |units| Arc::as_ptr(units) as *const () as usize)
}

fn bump(ctx: &ExecutionContext, change: impl FnOnce(&mut Counts)) {
	let mut counts = COUNTS.lock().unwrap_or_else(|e| e.into_inner());
	change(counts.entry(datastore_of(ctx)).or_default());
}

/// A morsel was handed to a worker.
pub(super) fn note_handed(ctx: &ExecutionContext) {
	HANDED_TO_WORKERS.fetch_add(1, Ordering::Relaxed);
	bump(ctx, |counts| counts.handed += 1);
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

/// A Range morsel read `entries` entries.
pub(super) fn note_scanned(ctx: &ExecutionContext, entries: usize) {
	bump(ctx, |counts| counts.scanned += entries);
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

/// A Range morsel parked.
pub(super) fn note_parked(ctx: &ExecutionContext) {
	bump(ctx, |counts| counts.parked += 1);
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
}

static FAULTS: LazyLock<Mutex<HashMap<usize, Faults>>> = LazyLock::new(Default::default);

/// Which reads of a datastore fail while this is held.
#[cfg(feature = "kv-rocksdb")]
pub(in crate::exec::vm) struct Failing {
	datastore: usize,
	change: fn(&mut Faults, bool),
}

#[cfg(feature = "kv-rocksdb")]
impl Failing {
	/// How many reads have failed since this was made.
	pub(in crate::exec::vm) fn refused(&self) -> usize {
		FAULTS
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.get(&self.datastore)
			.map_or(0, |faults| faults.refused)
	}
}

#[cfg(feature = "kv-rocksdb")]
impl Drop for Failing {
	fn drop(&mut self) {
		let mut faults = FAULTS.lock().unwrap_or_else(|e| e.into_inner());
		(self.change)(faults.entry(self.datastore).or_default(), false);
	}
}

#[cfg(feature = "kv-rocksdb")]
fn failing(ctx: &ExecutionContext, change: fn(&mut Faults, bool)) -> Failing {
	let datastore = datastore_of(ctx);
	let mut faults = FAULTS.lock().unwrap_or_else(|e| e.into_inner());
	let entry = faults.entry(datastore).or_default();
	entry.refused = 0;
	change(entry, true);
	drop(faults);
	Failing {
		datastore,
		change,
	}
}

/// Every narrowing of a cursor the morsels of the datastore `ctx` runs under ask for fails, until
/// the answer is dropped.
#[cfg(feature = "kv-rocksdb")]
pub(in crate::exec::vm) fn fail_narrows(ctx: &ExecutionContext) -> Failing {
	failing(ctx, |faults, on| faults.narrows = on)
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
	true
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
