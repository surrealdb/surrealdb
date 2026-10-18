//! What an HNSW index persists.
//!
//! The graph itself is rebuilt in memory from these records: the state record says
//! where traversal starts and how many chunks each layer occupies, and the
//! vector-to-documents mapping says which records a graph element stands for. When
//! vectors are deduplicated by content, one bucket holds every distinct vector
//! sharing a hash, each with its own element and document set.
//!
//! The traversal, the neighbour heuristics and the distance maths stay above; only
//! the stored records are here.
//!
//! One invariant is enforced from inside this module rather than by the engine
//! above, and is why [`ElementHashedDocs`] keeps its field private: a bucket holds
//! at most one entry per distinct vector, and never an entry whose document set is
//! empty. Break it and a lookup returns whichever duplicate happens to sort first
//! - a wrong element, a wrong neighbour set, and no error anywhere.

use std::sync::Arc;

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

#[revisioned(revision = 1)]
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct LayerState {
	pub version: u64,
	pub chunks: u32,
}

/// Persisted state of the HNSW graph, stored in the key-value store.
///
/// Tracks the current entry point, element ID counter, and per-layer state.
/// This state is loaded at startup and saved after each mutation to ensure
/// consistency across concurrent transactions.
#[revisioned(revision = 1)]
#[derive(Default, Serialize, Deserialize)]
pub struct HnswState {
	/// The entry point element for graph traversal, or `None` if the graph is empty.
	pub enter_point: Option<ElementId>,
	/// The next available element ID for new insertions.
	pub next_element_id: ElementId,
	/// State of layer 0 (the base layer containing all elements).
	pub layer0: LayerState,
	/// State of the upper layers (layers 1..N with progressively fewer elements).
	pub layers: Vec<LayerState>,
}

impl KVValue for HnswState {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

/// Persisted state of the pre-shared doc-ID allocator used by HNSW before
/// format version 1. Read only, so legacy indexes remain queryable.
#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct HnswDocsState {
	/// Recycled doc-IDs available for allocation.
	pub available: RoaringTreemap,
	/// The next never-used doc-ID.
	pub next_doc_id: DocId,
}

impl KVValue for HnswDocsState {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

/// Coalesced pending vector state for a single record.
///
/// This value is stored under the record-keyed `!hr` pending key. `doc_id`
/// records the current graph document mapping when one already exists,
/// `old_vectors` is the graph baseline to remove, and `new_vectors` is the
/// latest desired indexed state for that record.
///
/// `id` is the record the entry belongs to, kept here because the revisioned
/// encoding keeps it exactly where the entry's key cannot. `None` means an
/// entry written before this field existed; what reads it, and what the key
/// can and cannot spell, is `surrealdb_idx::trees::pending`.
///
/// A key spells its id under `IndexFormat`, which drops a number's kind, so
/// records whose ids differ only there (`pt:[1]` and `pt:[1dec]`) share one key.
/// The entry holds each of them: its own fields describe one record, and
/// `others` every other record sharing the key, each with its own state.
/// [`Self::into_records`] reads them as one entry each.
#[revisioned(revision = 3)]
pub struct HnswRecordPendingUpdate {
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
	pub others: Vec<HnswRecordPendingMember>,
}

/// The pending state of one more record sharing an [`HnswRecordPendingUpdate`]'s
/// key: the same fields, for a record whose id is always known.
#[revisioned(revision = 1)]
pub struct HnswRecordPendingMember {
	/// Existing internal document ID, if the record has already reached the graph.
	pub doc_id: Option<DocId>,
	/// Vectors currently represented in the graph for this pending record.
	pub old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should represent the record after compaction.
	pub new_vectors: Vec<SerializedVector>,
	/// The record, as it is stored.
	pub id: RecordIdKey,
}

impl HnswRecordPendingUpdate {
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

impl KVValue for HnswRecordPendingUpdate {
	type KeyContext = ();

	/// An entry naming one record is written at revision 2, which a release
	/// predating `others` reads; see [`pending_revision_2_when_alone`].
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let bytes = revision::to_vec(self)?;
		Ok(pending_revision_2_when_alone(bytes, self.others.is_empty()))
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		Ok(revision::from_slice(bytes)?)
	}
}

/// The revision-2 encoding of a revision-3 pending entry whose `others` is
/// empty, from its revision-3 encoding.
///
/// Revision 2 is revision 3 without the trailing `others`, and both revision
/// numbers and an empty list's length encode as one byte: the revision-3
/// bytes are `[3, fields.., 0]` and the revision-2 ones `[2, fields..]`. So an
/// entry naming one record keeps the bytes a release predating `others`
/// writes and reads, and only an entry several records share is written at
/// revision 3.
pub(crate) fn pending_revision_2_when_alone(mut bytes: Vec<u8>, alone: bool) -> Vec<u8> {
	if alone && bytes.first() == Some(&3) && bytes.last() == Some(&0) {
		bytes[0] = 2;
		bytes.pop();
	}
	bytes
}

/// A pending vector update queued under the append-keyed `!hp` layout.
///
/// One entry per queued change, where [`HnswRecordPendingUpdate`] above holds one
/// coalesced entry per record. Nothing writes it: it is decoded so that an index
/// holding entries under this layout still drains, and every entry compaction
/// consumes is one fewer that will ever be read.
#[revisioned(revision = 1)]
pub struct VectorPendingUpdate {
	/// Identifies the document being updated (by doc ID if known, or record key if new).
	pub id: VectorId,
	/// The previous vectors to remove from the index (empty for new documents).
	pub old_vectors: Vec<SerializedVector>,
	/// The new vectors to insert into the index (empty for deletions).
	pub new_vectors: Vec<SerializedVector>,
}

impl_kv_value_revisioned!(VectorPendingUpdate);

/// Identifies a vector's owning document, either by its internal doc ID or its record key.
///
/// When a document is first indexed, its doc ID may not yet be assigned, so the
/// record key is used. Once the pending update is applied, the doc ID is resolved.
///
/// Two record keys are one id exactly when they address one record:
/// `RecordIdKey`'s own equality compares numbers across kinds, under which
/// `[1]` and `[1dec]` are one, so `Eq`, `Ord` and `Hash` here compare a key
/// holding a number by its lossless encoding. A key holding no number compares
/// as `RecordIdKey` does.
#[revisioned(revision = 1)]
#[derive(Debug, Clone)]
pub enum VectorId {
	/// A previously resolved internal document ID.
	DocId(DocId),
	/// A record key for a document whose doc ID has not yet been resolved.
	RecordKey(Arc<RecordIdKey>),
}

impl PartialEq for VectorId {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == std::cmp::Ordering::Equal
	}
}

impl Eq for VectorId {}

impl PartialOrd for VectorId {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for VectorId {
	/// Doc ids before record keys; record keys as
	/// [`RecordIdKey::cmp_by_record_address`] orders them.
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		match (self, other) {
			(Self::DocId(a), Self::DocId(b)) => a.cmp(b),
			(Self::DocId(_), Self::RecordKey(_)) => std::cmp::Ordering::Less,
			(Self::RecordKey(_), Self::DocId(_)) => std::cmp::Ordering::Greater,
			(Self::RecordKey(a), Self::RecordKey(b)) => a.cmp_by_record_address(b),
		}
	}
}

impl std::hash::Hash for VectorId {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		match self {
			Self::DocId(d) => (0u8, d).hash(state),
			Self::RecordKey(k) => {
				1u8.hash(state);
				k.hash_by_record_address(state);
			}
		}
	}
}

/// Contains the mapping between an element ID and the document IDs that share the same vector.
#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub struct ElementDocs {
	pub e_id: ElementId,
	pub docs: Ids64,
}

impl ElementDocs {
	pub fn new(element_id: ElementId, d: DocId) -> Self {
		Self {
			e_id: element_id,
			docs: Ids64::One(d),
		}
	}
}

/// Contains a list of vectors and their associated document IDs that share the same hash.
#[revisioned(revision = 1)]
pub struct ElementHashedDocs {
	vectors: Vec<(SerializedVector, ElementDocs)>,
}

/// Result of removing a document from an [`ElementHashedDocs`] entry.
pub enum RemoveResult {
	/// The vector has no remaining documents; the element should be removed from the graph.
	Empty(ElementId),
	/// A document set changed without removing the graph element.
	Updated(ElementId, Ids64),
	/// A colliding vector was removed while other vectors remain in the hash bucket.
	RemovedElement(ElementId),
	/// The document was not found; no changes were made.
	Unchanged,
}

impl ElementHashedDocs {
	pub fn new(element_id: ElementId, vec: SerializedVector, doc_id: DocId) -> Self {
		let vectors = vec![(vec, ElementDocs::new(element_id, doc_id))];
		Self {
			vectors,
		}
	}

	pub fn get_element_docs(&mut self, vec: &SerializedVector) -> Option<&mut ElementDocs> {
		for (vector, ed) in self.vectors.iter_mut() {
			if *vec == *vector {
				return Some(ed);
			}
		}
		None
	}

	/// Returns the documents for the given vector if it exists in the list.
	pub fn get_docs(self, vec: &SerializedVector) -> Option<Ids64> {
		for (vector, ed) in self.vectors {
			if vector == *vec {
				return Some(ed.docs);
			}
		}
		None
	}

	pub fn add(&mut self, element_id: ElementId, vec: SerializedVector, doc_id: DocId) {
		self.vectors.push((vec, ElementDocs::new(element_id, doc_id)));
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
				ed.docs = new_docs;
				// The partition has been updated, but this vector has still connected document(s)
				return RemoveResult::Updated(ed.e_id, ed.docs.clone());
			}
		}
		if let Some((i, e_id)) = action {
			// There are no more documents for this vector, remove it
			self.vectors.remove(i);
			if self.vectors.is_empty() {
				// The vector partition is empty, remove the element and the hash entry
				return RemoveResult::Empty(e_id);
			}
			return RemoveResult::RemovedElement(e_id);
		}
		RemoveResult::Unchanged
	}
}
impl KVValue for ElementHashedDocs {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	fn kv_decode_value(mut bytes: &[u8], _: ()) -> Result<Self>
	where
		Self: Sized,
	{
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}

impl KVValue for ElementDocs {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}
