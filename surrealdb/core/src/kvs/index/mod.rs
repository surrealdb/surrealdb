//! Concurrent index build coordination.
//!
//! Concurrent `DEFINE INDEX` can run asynchronously while user writes continue.
//! In a multi-node deployment every node must make the same decisions about
//! which builder owns the work, whether writers should queue mutations, and
//! when queries may use the index. This module keeps those decisions in durable
//! table-scoped keys instead of process-local memory.
//!
//! The durable protocol uses four key families:
//!
//! - `!bs`: one build-state record per index, including phase, owner, generation, report counters,
//!   the initial-scan continuation checkpoint, and error reason.
//! - `!bt`: the generation-scoped writer-admission ticket counter, kept off `!bs` so an admitted
//!   write never invalidates the builder's in-flight batch.
//! - `!bg`: generation-scoped queued mutations that the builder replays.
//! - `!bp`: per-record pointers to the first queued mutation seen during the initial scan, so the
//!   scan indexes the writer-observed old state.
//! - `!br`: writer reservations that keep `Closing` from publishing `Online` until every admitted
//!   writer has either committed its `!bg` entry, released its ticket after transaction close, or
//!   died.
//!
//! Generation numbers fence stale queued work. Builder owner heartbeats fence
//! stale builders. Query planning only sees durable-`Online` indexes, while
//! document writes still see building indexes so they can enqueue mutations.
//! A build in durable `Error` keeps admitting writes the same way, and its
//! queue is never replayed: `REBUILD INDEX` wipes it and rescans the table.
//! The exception is a UNIQUE index in `Error`. Its queue cannot enforce
//! uniqueness, so a write that adds a key to it is refused with
//! `IndexBuildFailed` until the index is rebuilt or removed. Writes that add no
//! key are still admitted.
//! Legacy `!ig`/`!ip` appendings are still drained for committed work from older
//! code paths, but new writes use the durable queue.

mod admission;
mod after_commit;
mod builder;
mod count;
mod doc_ids;
mod kind;
pub(crate) mod materialise;
mod replay;
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
mod scan;
mod state;
mod unseen;

#[cfg(all(test, feature = "kv-mem"))]
mod tests;

use std::time::Duration;

#[cfg(all(test, feature = "kv-mem"))]
pub(crate) use after_commit::unstarted;
pub(crate) use after_commit::{RemoveFailedDefinition, StagedRebuild};
pub(crate) use builder::{
	AbortLocalBuild, BuildCompletion, BuildPass, BuildTasks, CleanUncommittedBuild, IndexBuilder,
	IndexMutation, RebuildStaging, ResumeScope,
};
pub(crate) use state::filter_online_indexes;
// Only the frozen-fixture corpus names this directly; the builder reaches a
// primary appending through its ticket.
#[cfg(test)]
pub(crate) use surrealdb_datastore::values::index_build::PrimaryAppending;
// What a build persists is part of the keyspace, so it is declared below this
// layer; the coordination protocol above reads it from there.
pub(crate) use surrealdb_datastore::values::index_build::{
	Appending, BatchId, BuildGeneration, BuildTicket, BuildTicketMutationSeq, IndexBuildBasis,
	IndexBuildPhase, IndexBuildReportStatus, IndexBuildReservation, IndexBuildState,
	PrimaryAppendingTicket,
};
use web_time::Instant;

use crate::kvs::tx::IndexBuildReservationRelease;

/// How long a writer admission reservation is considered owned by the writer.
const BUILD_RESERVATION_TTL_SECS: i64 = 30;
/// How long a builder may go without heartbeating its durable state before
/// another builder may take ownership of the same generation. This assumes
/// bounded clock skew between nodes; ownership transitions are still fenced by
/// CAS on `(generation, owner)`, so a stale owner cannot publish progress after
/// takeover.
const BUILD_OWNER_LEASE_SECS: i64 = 60;
/// How often a `CONCURRENTLY` build that is waiting for the transaction of the
/// statement that started it renews its owner lease. That transaction can stay
/// open for longer than [`BUILD_OWNER_LEASE_SECS`], and a lapsed lease would
/// let another builder take the generation over and build it from the
/// definition committed before the statement, which a `REBUILD` replaces.
#[cfg(not(test))]
const BUILD_WAITING_HEARTBEAT: Duration = Duration::from_secs(BUILD_OWNER_LEASE_SECS as u64 / 4);
/// Short enough for a test to watch a waiting build renew its lease.
#[cfg(test)]
const BUILD_WAITING_HEARTBEAT: Duration = Duration::from_millis(100);
/// Poll cadence while writer admission waits for `Closing` to become `Online`
/// or `Error`. The caller's context deadline is the only timeout budget.
const BUILD_CLOSING_SLEEP: Duration = Duration::from_millis(100);
/// Total time one closing transaction may spend waiting for the local builders
/// it aborted to stop, before deleting their durable state anyway.
///
/// A builder polls its abort flag per record during the initial scan and once
/// per iteration in every replay, sweep and retry loop, so the wait normally
/// ends in microseconds. The budget only binds where a single uninterruptible
/// stretch runs long: one initial-scan or replay batch's index writes and
/// commit, or one chunk of the doc-ID reclaim sweep, each bounded by
/// [`INDEXING_SCAN_BATCH_SIZE`](crate::kvs::INDEXING_SCAN_BATCH_SIZE) records
/// and carrying no internal checkpoint. A build folds no compaction plan, so
/// no vector graph apply runs inside it. Overrunning the budget falls back to
/// the compare-and-swap on `!bs` as the only ordering against the builder,
/// which is exactly what the wait exists to replace, so the budget is
/// deliberately generous: a cancelled statement stalls for at most this long,
/// whereas falling short strands durable state that nothing collects.
const BUILD_ABORT_STOP_BUDGET: Duration = Duration::from_secs(15);
type IndexBuilding = std::sync::Arc<builder::Building>;

/// Deadline for an abort-wait in a close drain that began at `drain_started_at`.
///
/// The budget is per drain, not per build: a schema transaction that defined
/// several indexes and then rolled back queues one cleanup per index, they run
/// one at a time, and their waits must not multiply into a close the client
/// reads as a hang. Measuring every one of them from the same origin caps the
/// whole drain at a single budget.
pub(crate) fn build_abort_deadline(drain_started_at: Instant) -> Instant {
	drain_started_at + BUILD_ABORT_STOP_BUDGET
}

#[derive(Clone)]
struct AcquiredBuild {
	generation: BuildGeneration,
	phase: IndexBuildPhase,
	initial_complete: bool,
	/// Persisted `INFO FOR INDEX` initial-scan count at the moment ownership was acquired.
	initial_count: usize,
	/// Persisted `INFO FOR INDEX` replayed-update count at the moment ownership was acquired.
	updates_count: usize,
	/// Persisted initial-scan continuation cursor at the moment ownership was
	/// acquired. A same-generation takeover resumes the scan after this record
	/// instead of wiping the partial index data and rescanning.
	initial_cursor: Option<crate::val::RecordIdKey>,
	/// Whether this acquisition staged the generation, rather than taking over one an earlier
	/// build staged.
	staged: bool,
}

struct DurableAdmission {
	generation: BuildGeneration,
	ticket: BuildTicket,
	initial_complete: bool,
	/// Close-time cleanup for the durable reservation committed by admission.
	///
	/// The release is prepared before the `!br` reservation commits. A writer
	/// transaction registers it immediately after admission returns, before any
	/// fence or queue work that can fail, so every committed reservation has an
	/// independent cleanup path.
	release: IndexBuildReservationRelease,
}

enum DurableAdmissionDecision {
	/// The write must be queued for the active durable generation. Boxed: the
	/// admission carries its release, which holds a transaction factory, and
	/// the decision is made once per user transaction and index, after a
	/// commit of its own.
	Admit(Box<DurableAdmission>),
	/// Durable state exists and is already online, so index normally.
	IndexNormally,
	/// Durable state is absent and the catalog no longer names this index, so
	/// there is nothing to maintain and nothing that will replay a queued write.
	Retired,
}

enum DurableAdmissionFence {
	/// The admission still points at the active generation, so queue the write.
	Queue,
	/// The index became online after admission; release the ticket and index now.
	IndexNormally,
}

pub(crate) enum ConsumeResult {
	/// The document has been enqueued to be indexed
	Enqueued,
	/// The index has been built, the document can be indexed normally
	Ignored(Option<Vec<crate::val::Value>>, Option<Vec<crate::val::Value>>),
	/// The index definition came from a stale cache after catalog retirement.
	Retired,
}

const LEGACY_BATCH_ID: BatchId = 0;

enum ExistingPrimaryAppending {
	None,
	Legacy,
	Appending(Appending),
}
