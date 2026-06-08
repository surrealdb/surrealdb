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
//!   and error reason.
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
//! Legacy `!ig`/`!ip` appendings are still drained for committed work from older
//! code paths, but new writes use the durable queue.

mod admission;
mod builder;
mod replay;
mod state;

#[cfg(test)]
mod tests;

use std::time::Duration;

pub(crate) use builder::{IndexBuilder, IndexMutation};
pub(crate) use replay::{Appending, PrimaryAppending, PrimaryAppendingTicket};
pub(crate) use state::{
	IndexBuildPhase, IndexBuildReportStatus, IndexBuildReservation, IndexBuildState,
	filter_online_indexes, index_building_info, retire_durable_index,
};

use crate::kvs::tx::IndexBuildReservationRelease;

/// Monotonically increasing build epoch for a table index.
///
/// Durable appendings, primary appending sentinels, and reservations all carry
/// this value so a replacement build never consumes work left behind by an
/// older build attempt.
pub(crate) type BuildGeneration = u64;
/// Per-generation ordering token assigned to a writer admitted during a build.
///
/// A single user transaction reserves one `BuildTicket` per index it writes to;
/// every indexed mutation in that transaction shares the ticket and is
/// disambiguated by `BuildTicketMutationSeq`.
pub(crate) type BuildTicket = u64;
/// Per-ticket index of an admitted mutation, distinguishing the different
/// `!bg` entries that share the same `(generation, ticket)` reservation.
///
/// The first mutation in a user transaction's batch uses `0`; subsequent
/// mutations use `1`, `2`, ... A `u32` gives a per-user-transaction cap of
/// ~4.3B mutations per index, which is well above any realistic single-txn
/// indexed write count.
pub(crate) type BuildTicketMutationSeq = u32;

/// How long a writer admission reservation is considered owned by the writer.
const BUILD_RESERVATION_TTL_SECS: i64 = 30;
/// How long a builder may go without heartbeating its durable state before
/// another builder may take ownership of the same generation. This assumes
/// bounded clock skew between nodes; ownership transitions are still fenced by
/// CAS on `(generation, owner)`, so a stale owner cannot publish progress after
/// takeover.
const BUILD_OWNER_LEASE_SECS: i64 = 60;
/// Poll cadence while writer admission waits for `Closing` to become `Online`
/// or `Error`. The caller's context deadline is the only timeout budget.
const BUILD_CLOSING_SLEEP: Duration = Duration::from_millis(100);

type IndexBuilding = std::sync::Arc<builder::Building>;

#[derive(Clone, Copy)]
struct AcquiredBuild {
	generation: BuildGeneration,
	phase: IndexBuildPhase,
	initial_complete: bool,
	/// Persisted `INFO FOR INDEX` initial-scan count at the moment ownership was acquired.
	initial_count: usize,
	/// Persisted `INFO FOR INDEX` replayed-update count at the moment ownership was acquired.
	updates_count: usize,
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
	/// The write must be queued for the active durable generation.
	Admit(DurableAdmission),
	/// Durable state exists and is already online, so index normally.
	IndexNormally,
	/// Durable state is absent; the caller must disambiguate legacy from retired catalog state.
	MissingState,
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

pub(crate) type BatchId = u32;
pub(crate) type AppendingId = u32;
const LEGACY_BATCH_ID: BatchId = 0;

enum ExistingPrimaryAppending {
	None,
	Legacy,
	Appending(Appending),
}
