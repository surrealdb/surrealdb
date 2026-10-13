//! KV-backed DiskANN index implementation.
//!
//! DiskANN graph mutations are not applied directly from user write transactions. Writes enqueue
//! record-keyed pending updates, and background compaction later applies those updates to the
//! persisted graph. Lookup merges compacted graph results with pending updates so transactionally
//! recent writes remain visible.
//!
//! The persisted graph uses the `!d*` index key families: graph state (`!ds`), element payloads
//! (`!de`), adjacency nodes (`!dn`), record/document mappings (`!di`/`!dd`), vector/document
//! mappings (`!dq`/`!dh`), sharded pending operations (`!dw`) with their per-shard guard (`!dy`),
//! compaction generation (`!dg`), and — for the dual-read migration off the pre-sharding layout —
//! the legacy unsharded pending operations (`!dr`) and their legacy guard (`!dp`).

#[cfg(not(target_family = "wasm"))]
pub(crate) mod cache;
pub(crate) mod docs;
#[cfg(not(target_family = "wasm"))]
mod filter;
#[cfg(not(target_family = "wasm"))]
pub mod index;
#[cfg(not(target_family = "wasm"))]
mod provider;

// Everything the persisted graph, its pending queue and their guard are made of is
// declared below this layer with the rest of the keyspace; the search and the
// compaction that read them stay here.
pub(super) use surrealdb_datastore::values::diskann::DISKANN_PENDING_STATE_SHARDS;
pub(crate) use surrealdb_datastore::values::diskann::{
	DiskAnnElement, DiskAnnNode, DiskAnnPendingState, DiskAnnPendingStateKind,
	DiskAnnRecordPendingUpdate, DiskAnnState,
};
pub(crate) use surrealdb_datastore::values::vector::ElementId;
