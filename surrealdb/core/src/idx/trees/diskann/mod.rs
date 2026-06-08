//! KV-backed DiskANN index implementation.
//!
//! DiskANN graph mutations are not applied directly from user write transactions. Writes enqueue
//! record-keyed pending updates, and background compaction later applies those updates to the
//! persisted graph. Lookup merges compacted graph results with pending updates so transactionally
//! recent writes remain visible.
//!
//! The persisted graph uses the `!d*` index key families: graph state (`!ds`), element payloads
//! (`!de`), adjacency nodes (`!dn`), record/document mappings (`!di`/`!dd`), vector/document
//! mappings (`!dq`/`!dh`), pending operations (`!dr`), compaction generation (`!dg`), and the
//! distributed-safe pending-state guard (`!dp`).

#[cfg(not(target_family = "wasm"))]
pub(crate) mod cache;
pub(crate) mod docs;
#[cfg(not(target_family = "wasm"))]
mod filter;
#[cfg(not(target_family = "wasm"))]
pub mod index;
#[cfg(not(target_family = "wasm"))]
mod provider;

use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use serde::{Deserialize, Serialize};

use crate::idx::seqdocids::DocId;
use crate::idx::trees::vector::SerializedVector;
use crate::kvs::{KVValue, impl_kv_value_revisioned};

/// Unique identifier for a vector element in the DiskANN graph.
pub(crate) type ElementId = u64;

/// Number of KV shards used by the DiskANN pending-state guard.
///
/// Writers update only one shard derived from the record key, avoiding a single
/// hot key while still letting lookup skip pending scans only after every shard
/// has been cleared by compaction.
pub(super) const DISKANN_PENDING_STATE_SHARDS: u16 = 32;

/// Persisted DiskANN graph state.
#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct DiskAnnState {
	/// The graph entry point, if the graph contains a valid element.
	pub(crate) enter_point: Option<ElementId>,
	/// The next available element ID.
	pub(crate) next_element_id: ElementId,
}

impl KVValue for DiskAnnState {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

/// Persisted DiskANN element payload.
#[revisioned(revision = 1)]
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DiskAnnElement {
	/// Serialized vector payload stored in the graph.
	pub(crate) vector: SerializedVector,
	/// Tombstone used by DiskANN delete/release flow before the element is physically removed.
	pub(crate) deleted: bool,
}

impl KVValue for DiskAnnElement {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

/// Persisted DiskANN neighbor list for one element.
#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct DiskAnnNode {
	/// Outgoing graph neighbors for this element.
	pub(crate) neighbors: Vec<ElementId>,
}

impl KVValue for DiskAnnNode {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

/// Persisted summary of whether DiskANN pending operations may exist.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum DiskAnnPendingStateKind {
	/// Compaction has confirmed that no committed `!dr` keys exist.
	Empty,
	/// Compaction saw an empty pending range once, but lookup must still scan conservatively.
	MaybeEmpty,
	/// Writers have committed pending updates that lookup must merge.
	NonEmpty,
}

/// Persisted summary of whether DiskANN pending operations may exist.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DiskAnnPendingState {
	/// Conservative pending-state kind for this shard.
	pub(crate) kind: DiskAnnPendingStateKind,
	/// Monotonic version used by compaction to detect concurrent shard updates.
	pub(crate) generation: u64,
}

impl_kv_value_revisioned!(DiskAnnPendingState);

/// Coalesced pending vector state for a single DiskANN indexed record.
#[revisioned(revision = 1)]
pub(crate) struct DiskAnnRecordPendingUpdate {
	/// Existing internal document ID, if the record has already reached the graph.
	pub(crate) doc_id: Option<DocId>,
	/// Vectors currently represented in the graph for this pending record.
	pub(crate) old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should represent the record after compaction.
	pub(crate) new_vectors: Vec<SerializedVector>,
}

impl_kv_value_revisioned!(DiskAnnRecordPendingUpdate);
