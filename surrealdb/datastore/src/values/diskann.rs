//! What a DiskANN index persists.
//!
//! DiskANN keeps the graph on disk and applies writes lazily: a write enqueues a
//! per-record pending update, and compaction later folds it into the persisted
//! graph. So the stored form is in three parts - the graph itself (state, element
//! payloads, adjacency), the vector-to-documents mapping, and the pending queue
//! with the sharded guard that tells lookup whether it may skip scanning it.
//!
//! The search, the compaction and the pruning heuristics stay above.
//!
//! Nothing here is behind the `diskann` build gate. That cfg is set by the engine
//! crate's build script for the targets the index supports; the stored shapes are
//! target-independent, and gating them here would make them vanish rather than
//! compile.
//!
//! As with the HNSW bucket, [`DiskAnnElementHashedDocs`] keeps its field private
//! so the one-entry-per-distinct-vector rule stays with the data it constrains.

use anyhow::Result;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use serde::{Deserialize, Serialize};
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_kvs::value::KVValue;

use super::ids::{DocId, Ids64};
pub use super::vector::ElementId;
use super::vector::SerializedVector;

/// Number of KV shards used by the DiskANN pending-state guard.
///
/// Writers update only one shard derived from the record key, avoiding a single
/// hot key while still letting lookup skip pending scans only after every shard
/// has been cleared by compaction.
pub const DISKANN_PENDING_STATE_SHARDS: u16 = 32;

/// Persisted DiskANN graph state.
#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct DiskAnnState {
	/// The graph entry point, if the graph contains a valid element.
	pub enter_point: Option<ElementId>,
	/// The next available element ID.
	pub next_element_id: ElementId,
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
pub struct DiskAnnElement {
	/// Serialized vector payload stored in the graph.
	pub vector: SerializedVector,
	/// Tombstone used by DiskANN delete/release flow before the element is physically removed.
	pub deleted: bool,
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
pub struct DiskAnnNode {
	/// Outgoing graph neighbors for this element.
	pub neighbors: Vec<ElementId>,
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
pub enum DiskAnnPendingStateKind {
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
pub struct DiskAnnPendingState {
	/// Conservative pending-state kind for this shard.
	pub kind: DiskAnnPendingStateKind,
	/// Monotonic version used by compaction to detect concurrent shard updates.
	pub generation: u64,
}

impl_kv_value_revisioned!(DiskAnnPendingState);

/// Coalesced pending vector state for a single DiskANN indexed record.
#[revisioned(revision = 1)]
pub struct DiskAnnRecordPendingUpdate {
	/// Existing internal document ID, if the record has already reached the graph.
	pub doc_id: Option<DocId>,
	/// Vectors currently represented in the graph for this pending record.
	pub old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should represent the record after compaction.
	pub new_vectors: Vec<SerializedVector>,
}

impl_kv_value_revisioned!(DiskAnnRecordPendingUpdate);

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub struct DiskAnnElementDocs {
	/// Graph element ID that owns this exact vector.
	pub e_id: ElementId,
	/// Compact document IDs currently sharing the vector.
	pub docs: Ids64,
}

impl DiskAnnElementDocs {
	pub fn new(element_id: ElementId, d: DocId) -> Self {
		Self {
			e_id: element_id,
			docs: Ids64::One(d),
		}
	}
}

/// Soft cap on hashed-vector collision-bucket size. Real-world hash collisions across
/// distinct full-fidelity vectors are vanishingly rare; if a bucket grows past this size we
/// emit a `warn!` because every lookup of the bucket is O(bucket-size) full-vector compares
/// (see [`DiskAnnElementHashedDocs::get_docs`]) and an adversarial or buggy hash distribution
/// would otherwise silently scale the cost of every KNN search.
const HASHED_BUCKET_WARN_THRESHOLD: usize = 16;

#[revisioned(revision = 1)]
pub struct DiskAnnElementHashedDocs {
	/// Collision bucket keyed by vector hash; each entry retains the full vector for
	/// disambiguation.
	vectors: Vec<(SerializedVector, DiskAnnElementDocs)>,
}

/// Result of removing one document ID from a hashed vector collision bucket.
pub enum RemoveResult {
	/// The whole hash bucket became empty and its graph element should be removed.
	Empty(ElementId),
	/// The doc set of one bucket entry shrank, but the entry (and its graph element)
	/// still has other docs sharing it. Caller must evict the cached doc set.
	BucketShrunk {
		e_id: ElementId,
	},
	/// One entry in the bucket was removed entirely (its last doc went away). The
	/// graph element must be removed from the upstream graph; the rest of the bucket
	/// is intact and must be persisted back to KV.
	EntryRemoved {
		e_id: ElementId,
	},
	/// The requested vector/document pair was not present.
	Unchanged,
}

impl DiskAnnElementHashedDocs {
	pub fn new(element_id: ElementId, vec: SerializedVector, doc_id: DocId) -> Self {
		Self {
			vectors: vec![(vec, DiskAnnElementDocs::new(element_id, doc_id))],
		}
	}

	pub fn get_element_docs(&mut self, vec: &SerializedVector) -> Option<&mut DiskAnnElementDocs> {
		self.vectors.iter_mut().find_map(|(vector, ed)| {
			if *vec == *vector {
				Some(ed)
			} else {
				None
			}
		})
	}

	pub fn get_docs(self, vec: &SerializedVector) -> Option<(ElementId, Ids64)> {
		for (vector, ed) in self.vectors {
			if vector == *vec {
				return Some((ed.e_id, ed.docs));
			}
		}
		None
	}

	pub fn add(&mut self, element_id: ElementId, vec: SerializedVector, doc_id: DocId) {
		self.vectors.push((vec, DiskAnnElementDocs::new(element_id, doc_id)));
		// Real-world vector-hash collisions across distinct full-fidelity vectors are
		// vanishingly rare; warn loudly if we ever cross the soft cap so an unexpected hash
		// distribution doesn't silently scale KNN search by bucket size. Fire on every
		// power-of-two crossing at or above the threshold (16, 32, 64, …) so an operator
		// sees runaway growth, not just the first crossing.
		let len = self.vectors.len();
		if len >= HASHED_BUCKET_WARN_THRESHOLD && len.is_power_of_two() {
			tracing::warn!(
				bucket_size = len,
				new_element_id = element_id,
				"DiskANN hashed-vector collision bucket exceeded soft warn threshold; \
				 every lookup of this hash now does {len} full-vector compares",
			);
		}
	}

	pub fn remove(&mut self, vec: &SerializedVector, doc_id: DocId) -> RemoveResult {
		let mut action = None;
		for (i, (vector, ed)) in self.vectors.iter_mut().enumerate() {
			if *vector == *vec
				&& let Some(new_docs) = ed.docs.remove(doc_id)
			{
				if new_docs.is_empty() {
					action = Some((i, ed.e_id));
					break;
				}
				let e_id = ed.e_id;
				ed.docs = new_docs;
				return RemoveResult::BucketShrunk {
					e_id,
				};
			}
		}
		if let Some((i, e_id)) = action {
			self.vectors.remove(i);
			if self.vectors.is_empty() {
				return RemoveResult::Empty(e_id);
			}
			return RemoveResult::EntryRemoved {
				e_id,
			};
		}
		RemoveResult::Unchanged
	}
}

impl KVValue for DiskAnnElementHashedDocs {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	fn kv_decode_value(mut bytes: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}

impl KVValue for DiskAnnElementDocs {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut bytes: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}
