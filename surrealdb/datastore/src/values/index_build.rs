//! What a concurrent index build persists.
//!
//! An index can be built while writes continue, which means the build's progress
//! and the writes it has yet to catch up on both have to survive a node dying
//! mid-build. These are the records that make that possible: the per-index build
//! state that any node can read to learn where a build got to, and the queued
//! writes a builder replays once its initial scan is done.
//!
//! The builder itself — admission, scanning, replay, takeover — reads these
//! downward from the layer above. It needs the engine's document machinery, so it
//! cannot descend; these records cannot rise, because the keyspace binds them.

use chrono::{DateTime, Utc};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_expr::val::{RecordIdKey, Value};
use surrealdb_kvs::impl_kv_value_revisioned;
use uuid::Uuid;

/// Monotonically increasing build epoch for a table index.
///
/// Durable appendings, primary appending sentinels, and reservations all carry
/// this value so a replacement build never consumes work left behind by an
/// older build attempt.
pub type BuildGeneration = u64;

/// Per-generation ordering token assigned to a writer admitted during a build.
///
/// A single user transaction reserves one `BuildTicket` per index it writes to;
/// every indexed mutation in that transaction shares the ticket and is
/// disambiguated by [`BuildTicketMutationSeq`].
pub type BuildTicket = u64;

/// Per-ticket index of an admitted mutation, distinguishing the different
/// `!bg` entries that share the same `(generation, ticket)` reservation.
///
/// The first mutation in a user transaction's batch uses `0`; subsequent
/// mutations use `1`, `2`, ... A `u32` gives a per-user-transaction cap of
/// ~4.3B mutations per index, which is well above any realistic single-txn
/// indexed write count.
pub type BuildTicketMutationSeq = u32;

/// Identifies one batch of appended writes within a generation's queue.
pub type BatchId = u32;

/// Identifies one appended write within a batch.
pub type AppendingId = u32;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndexBuildReportStatus {
	/// Build state was created but no index-data cleanup has started yet.
	Started,
	/// Existing index data is being removed before the initial scan.
	Cleaning,
	/// The builder is scanning records, replaying queued writes, or closing.
	Indexing,
	/// The durable build phase is online and queries may use the index.
	Ready,
	/// The local builder was aborted before completion.
	Aborted,
	/// The durable build phase failed with an optional stored error reason.
	Error,
}

impl IndexBuildReportStatus {
	/// The name `INFO FOR INDEX` reports for this status.
	///
	/// Travels with the shape rather than staying with the reporting code: these
	/// strings are a client-visible spelling of the stored discriminant, so they
	/// belong to the stored form.
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Started => "started",
			Self::Cleaning => "cleaning",
			Self::Indexing => "indexing",
			Self::Ready => "ready",
			Self::Aborted => "aborted",
			Self::Error => "error",
		}
	}
}

/// Cluster-visible lifecycle for an index build generation.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndexBuildPhase {
	/// The builder is scanning records and writers may reserve tickets.
	Building,
	/// Initial indexing has completed and new writer admissions are blocked.
	Closing,
	/// The index has caught up with admitted writes and is queryable.
	Online,
	/// The build was aborted or failed; queries must not use the index.
	///
	/// Writers keep queueing mutations as in `Building`, so a failed build
	/// never blocks user writes; the stale queue is wiped and the table
	/// rescanned when a `REBUILD INDEX` starts the next generation.
	Error,
}

/// Durable per-index build state shared by all nodes.
///
/// The state is the fencing token for the builder and the phase/generation
/// source for writer admission. Writers only update this record on the legacy
/// `next_ticket` path; only builders refresh `owner_heartbeat_at`, which
/// controls lease expiry.
#[revisioned(revision = 4)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexBuildState {
	/// Build epoch. Stale generation-scoped keys are ignored by newer builds.
	pub generation: BuildGeneration,
	/// Current durable lifecycle phase.
	pub phase: IndexBuildPhase,
	/// Concrete builder task that currently owns this generation.
	pub owner: Option<Uuid>,
	/// Next writer ticket for generations that predate the `!bt` counter.
	///
	/// Live generations keep their ticket counter on `!bt` so admission never
	/// writes this record. This field is only read — and only advanced — for a
	/// generation installed before that counter existed, which has no `!bt`;
	/// such a build keeps allocating here until its next generation.
	pub next_ticket: BuildTicket,
	/// Whether initial record scanning has completed for this generation.
	pub initial_complete: bool,
	/// Last durable state update time.
	pub updated_at: DateTime<Utc>,
	/// Last builder-owned lease heartbeat.
	#[revision(start = 3)]
	pub owner_heartbeat_at: Option<DateTime<Utc>>,
	/// Durable error reason visible to every node once the build enters `Error`.
	#[revision(start = 2)]
	pub error: Option<String>,
	/// User-facing status for `INFO FOR INDEX`.
	#[revision(start = 3)]
	pub report_status: Option<IndexBuildReportStatus>,
	/// Number of records indexed during the initial scan.
	#[revision(start = 3)]
	pub initial: Option<u64>,
	/// Number of appended updates replayed after the initial scan.
	#[revision(start = 3)]
	pub updated: Option<u64>,
	/// Best-effort count of pending build updates visible to the builder.
	#[revision(start = 3)]
	pub pending: Option<u64>,
	/// Initial-scan continuation cursor: the id of the last record whose
	/// batch commit is durable for this generation.
	///
	/// The cursor is written in the same transaction as the batch it covers,
	/// so a takeover can resume the scan right after this record instead of
	/// wiping the partial index data and rescanning from the start. `None`
	/// until the first batch commits, and cleared once the scan completes.
	///
	/// Durable persistence goes through `revision` (see
	/// `impl_kv_value_revisioned`); the field is skipped for serde because
	/// `RecordIdKey` does not implement the serde traits.
	///
	/// WARNING: `IndexBuildState` must only ever be persisted through the
	/// revisioned `KVValue` path — never round-trip it through serde. A
	/// serde round-trip silently drops this field, and writing the result
	/// back would reset the checkpoint, forcing the next takeover to wipe
	/// the partial index data and rescan the whole table from zero.
	#[revision(start = 4)]
	#[serde(skip)]
	pub initial_cursor: Option<RecordIdKey>,
}

impl_kv_value_revisioned!(IndexBuildState);

/// Durable admission marker written before the user transaction commits.
///
/// The builder cannot move from `Closing` to `Online` until every reservation
/// for the generation has either been released after transaction close, produced
/// a durable appending that the builder can replay, or expired after its writer
/// node is no longer live.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexBuildReservation {
	/// Node that reserved the ticket.
	pub node: Uuid,
	/// Deadline after which the reservation may be cleaned if the node is dead.
	pub expires_at: DateTime<Utc>,
}

impl_kv_value_revisioned!(IndexBuildReservation);

/// One indexed write queued while a build was still catching up.
#[revisioned(revision = 2)]
#[derive(Debug, PartialEq)]
pub struct Appending {
	/// Values to remove from the index when replaying the write.
	pub old_values: Option<Vec<Value>>,
	/// Values to add to the index when replaying the write.
	pub new_values: Option<Vec<Value>>,
	/// Record id key whose index entries are being replayed.
	pub id: RecordIdKey,
	/// Cached COUNT condition match state `(old_matches, new_matches)`.
	///
	/// Re-evaluating a conditional COUNT predicate during replay can observe a
	/// different document state than the user write observed. Carrying both
	/// booleans makes replay deterministic.
	#[revision(start = 2)]
	pub count_cond_match: Option<(bool, bool)>,
}

impl_kv_value_revisioned!(Appending);

/// Sentinel marking the queue entry that holds a record's writer-observed state.
#[revisioned(revision = 2)]
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PrimaryAppending(
	/// Appending id within the concurrent indexing queue.
	pub AppendingId,
	/// Batch id associated with this append.
	#[revision(start = 2)]
	pub BatchId,
);

impl_kv_value_revisioned!(PrimaryAppending);

/// Pointer from a per-record `!bp` marker to the specific `!bg` entry that
/// holds the writer-observed old state for that record.
///
/// One reservation is allocated per user transaction per index, so the same
/// `ticket` can cover many `!bg` entries. The `mutation_seq` selects the
/// first admitted mutation for the marker's record.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrimaryAppendingTicket {
	pub ticket: BuildTicket,
	pub mutation_seq: BuildTicketMutationSeq,
}

impl_kv_value_revisioned!(PrimaryAppendingTicket);
