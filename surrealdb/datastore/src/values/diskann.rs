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
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use surrealdb_expr::val::RecordIdKey;
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

/// Persisted state of the pre-shared doc-ID allocator used by DiskANN before
/// format version 1. Read only, so legacy indexes remain queryable.
#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct DiskAnnDocsState {
	/// Recycled doc-IDs available for allocation.
	pub available: RoaringTreemap,
	/// The next never-used doc-ID.
	pub next_doc_id: DocId,
}

impl KVValue for DiskAnnDocsState {
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

/// A node's writer lease on one DiskANN pending-state guard shard (`!dz`).
///
/// Held by a node whose writers commit pending updates into the shard without
/// arming its guard. No node steps the guard while a lease on its shard
/// holds; one holds until its node releases it or it expires.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiskAnnWriterLease {
	/// The datastore instance that took the lease, so a node id reused by a
	/// later process does not pass for this one.
	pub process: uuid::Uuid,
	/// The instant the lease stops holding unless its node renews it first.
	pub expires_at: chrono::DateTime<chrono::Utc>,
}

impl_kv_value_revisioned!(DiskAnnWriterLease);

impl DiskAnnWriterLease {
	/// A lease another process took, expired or not, as the tests of the
	/// writer-lease protocol plant one.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn foreign(expired: bool) -> Self {
		let ttl = chrono::Duration::seconds(if expired {
			-1
		} else {
			60
		});
		Self {
			process: uuid::Uuid::new_v4(),
			expires_at: chrono::Utc::now() + ttl,
		}
	}

	/// This lease expiring `secs` seconds from now, or having expired that long
	/// ago for a negative `secs`, as the tests of its renewal and retirement
	/// plant it.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn expiring_in(&self, secs: i64) -> Self {
		Self {
			process: self.process,
			expires_at: chrono::Utc::now() + chrono::Duration::seconds(secs),
		}
	}

	/// Whether the lease holds for at least `secs` more seconds.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn holds_for(&self, secs: i64) -> bool {
		self.expires_at > chrono::Utc::now() + chrono::Duration::seconds(secs)
	}
}

/// Coalesced pending vector state for a single DiskANN indexed record.
///
/// `id` is the record the entry belongs to, kept here because the revisioned
/// encoding keeps it exactly where the entry's key cannot. `None` means an
/// entry written before this field existed; what reads it, and what the key
/// can and cannot spell, is `surrealdb_idx::trees::pending`.
///
/// A key spells its id under `IndexFormat`, which drops a number's kind, so
/// records whose ids differ only there (`pt:[1]` and `pt:[1dec]`) share one key,
/// and one shard. The entry holds each of them: its own fields describe one
/// record, and `others` every other record sharing the key, each with its own
/// state. [`Self::into_records`] reads them as one entry each.
#[revisioned(revision = 3)]
pub struct DiskAnnRecordPendingUpdate {
	/// Existing internal document ID, if the record has already reached the graph.
	pub doc_id: Option<DocId>,
	/// Vectors currently represented in the graph for this pending record.
	pub old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should represent the record after compaction.
	pub new_vectors: Vec<SerializedVector>,
	/// The record this entry belongs to, as it is stored.
	#[revision(start = 2)]
	pub id: Option<RecordIdKey>,
	/// The other records whose ids share this entry's key, each with its own
	/// pending state.
	#[revision(start = 3)]
	pub others: Vec<DiskAnnRecordPendingMember>,
}

/// The pending state of one more record sharing a
/// [`DiskAnnRecordPendingUpdate`]'s key: the same fields, for a record whose id
/// is always known.
#[revisioned(revision = 1)]
pub struct DiskAnnRecordPendingMember {
	/// Existing internal document ID, if the record has already reached the graph.
	pub doc_id: Option<DocId>,
	/// Vectors currently represented in the graph for this pending record.
	pub old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should represent the record after compaction.
	pub new_vectors: Vec<SerializedVector>,
	/// The record, as it is stored.
	pub id: RecordIdKey,
}

impl DiskAnnRecordPendingUpdate {
	/// The entry's records, one entry each: its own fields, then every other
	/// record sharing its key, each carrying its id and no others.
	pub fn into_records(mut self) -> impl Iterator<Item = Self> {
		let others = std::mem::take(&mut self.others);
		std::iter::once(self).chain(others.into_iter().map(|m| Self {
			doc_id: m.doc_id,
			old_vectors: m.old_vectors,
			new_vectors: m.new_vectors,
			id: Some(m.id),
			others: Vec::new(),
		}))
	}
}

impl surrealdb_kvs::value::KVValue for DiskAnnRecordPendingUpdate {
	type KeyContext = ();

	/// An entry naming one record is written at revision 2, which a release
	/// predating `others` reads; see
	/// [`super::hnsw::pending_revision_2_when_alone`].
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let bytes = revision::to_vec(self)?;
		Ok(super::hnsw::pending_revision_2_when_alone(bytes, self.others.is_empty()))
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(revision::from_slice(bytes)?)
	}
}

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
