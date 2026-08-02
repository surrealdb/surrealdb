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

use anyhow::Result;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use serde::{Deserialize, Serialize};
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

/// Coalesced pending vector state for a single record.
///
/// This value is stored under the record-keyed `!hr` pending key. The key
/// identifies the record; `doc_id` records the current graph document mapping
/// when one already exists. `old_vectors` is the graph baseline to remove, and
/// `new_vectors` is the latest desired indexed state for that record.
#[revisioned(revision = 1)]
pub struct HnswRecordPendingUpdate {
	/// Existing internal document ID, if the record has already reached the graph.
	pub doc_id: Option<DocId>,
	/// Vectors currently represented in the graph for this pending record.
	pub old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should represent the record after compaction.
	pub new_vectors: Vec<SerializedVector>,
}

impl_kv_value_revisioned!(HnswRecordPendingUpdate);

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
