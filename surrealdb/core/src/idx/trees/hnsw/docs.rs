use anyhow::Result;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::idx::IndexKeyBase;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::vector::{SerializedVector, Vector};
use crate::kvs::{KVValue, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName};

pub(in crate::idx) struct HnswDocs {
	tb: TableName,
	ikb: IndexKeyBase,
	state_updated: bool,
	state: HnswDocsState,
}

#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct HnswDocsState {
	available: RoaringTreemap,
	next_doc_id: DocId,
}

impl HnswDocs {
	pub(in crate::idx) async fn new(
		tx: &Transaction,
		tb: TableName,
		ikb: IndexKeyBase,
	) -> Result<Self> {
		let state_key = ikb.new_hd_root_key();
		let state = tx.get(&state_key, None).await?.unwrap_or_default();
		Ok(Self {
			tb,
			ikb,
			state_updated: false,
			state,
		})
	}

	pub(super) async fn resolve(&mut self, tx: &Transaction, id: &RecordIdKey) -> Result<DocId> {
		if let Some(doc_id) = tx.get(&self.ikb.new_hi_key(id.clone()), None).await? {
			Ok(doc_id)
		} else {
			let doc_id = self.next_doc_id();
			let id_key = self.ikb.new_hi_key(id.clone());
			tx.set(&id_key, &doc_id, None).await?;
			let doc_key = self.ikb.new_hd_key(doc_id);
			tx.set(&doc_key, id, None).await?;
			Ok(doc_id)
		}
	}

	fn next_doc_id(&mut self) -> DocId {
		self.state_updated = true;
		if let Some(doc_id) = self.state.available.iter().next() {
			self.state.available.remove(doc_id);
			doc_id
		} else {
			let doc_id = self.state.next_doc_id;
			self.state.next_doc_id += 1;
			doc_id
		}
	}

	pub(in crate::idx) async fn get_thing(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<RecordId>> {
		let doc_key = self.ikb.new_hd_key(doc_id);
		if let Some(id) = tx.get(&doc_key, None).await? {
			Ok(Some(RecordId {
				table: self.tb.clone(),
				key: id,
			}))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn remove(
		&mut self,
		tx: &Transaction,
		id: RecordIdKey,
	) -> Result<Option<DocId>> {
		let id_key = self.ikb.new_hi_key(id);
		if let Some(doc_id) = tx.get(&id_key, None).await? {
			let doc_key = self.ikb.new_hd_key(doc_id);
			tx.del(&doc_key).await?;
			tx.del(&id_key).await?;
			self.state.available.insert(doc_id);
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &Transaction) -> Result<()> {
		if self.state_updated {
			let state_key = self.ikb.new_hd_root_key();
			tx.set(&state_key, &self.state, None).await?;
			self.state_updated = true;
		}
		Ok(())
	}
}

impl KVValue for HnswDocsState {
	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> anyhow::Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val.as_slice())?)
	}
}

/// Contains the mapping between an element ID and the document IDs that share the same vector.
#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub(crate) struct ElementDocs {
	e_id: ElementId,
	docs: Ids64,
}

impl ElementDocs {
	fn new(element_id: ElementId, d: DocId) -> Self {
		Self {
			e_id: element_id,
			docs: Ids64::One(d),
		}
	}
}

/// Contains a list of vectors and their associated document IDs that share the same hash.
#[revisioned(revision = 1)]
pub(crate) struct ElementHashedDocs {
	vectors: Vec<(SerializedVector, ElementDocs)>,
}

enum RemoveResult {
	Empty(ElementId),
	Updated(Option<ElementId>),
	Unchanged,
}

impl ElementHashedDocs {
	fn new(element_id: ElementId, vec: SerializedVector, doc_id: DocId) -> Self {
		let vectors = vec![(vec, ElementDocs::new(element_id, doc_id))];
		Self {
			vectors,
		}
	}

	fn get_element_docs(&mut self, vec: &SerializedVector) -> Option<&mut ElementDocs> {
		for (vector, ed) in self.vectors.iter_mut() {
			if *vec == *vector {
				return Some(ed);
			}
		}
		None
	}

	/// Returns the documents for the given vector if it exists in the list.
	fn get_docs(self, vec: &SerializedVector) -> Option<Ids64> {
		for (vector, ed) in self.vectors {
			if vector == *vec {
				return Some(ed.docs);
			}
		}
		None
	}

	fn add(&mut self, element_id: ElementId, vec: SerializedVector, doc_id: DocId) {
		self.vectors.push((vec, ElementDocs::new(element_id, doc_id)));
	}

	fn remove(&mut self, vec: &SerializedVector, doc_id: DocId) -> RemoveResult {
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
				return RemoveResult::Updated(None);
			}
		}
		if let Some((i, e_id)) = action {
			// There are no more documents for this vector, remove it
			self.vectors.remove(i);
			if self.vectors.is_empty() {
				// The vector partition is empty, remove the element and the hash entry
				return RemoveResult::Empty(e_id);
			}
			return RemoveResult::Updated(Some(e_id));
		}
		RemoveResult::Unchanged
	}
}
impl KVValue for ElementHashedDocs {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self>
	where
		Self: Sized,
	{
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes.as_slice())?)
	}
}

impl KVValue for ElementDocs {
	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(bytes: Vec<u8>) -> anyhow::Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes.as_slice())?)
	}
}

/// Manages the mapping between vectors and document IDs in the HNSW index.
pub(in crate::idx) struct VecDocs {
	ikb: IndexKeyBase,
	use_hashed_vector: bool,
}

impl VecDocs {
	pub(super) fn new(ikb: IndexKeyBase, use_hashed_vector: bool) -> Self {
		Self {
			ikb,
			use_hashed_vector,
		}
	}

	/// Retrieves document IDs for a given vector using its hash.
	async fn get_hashed_docs(
		&self,
		tx: &Transaction,
		ser_vec: SerializedVector,
	) -> Result<Option<Ids64>> {
		let hash = ser_vec.compute_hash();
		let key = self.ikb.new_hh_key(hash);
		// We search first in the new hash structure
		if let Some(ehd) = tx.get(&key, None).await?
			&& let Some(docs) = ehd.get_docs(&ser_vec)
		{
			return Ok(Some(docs));
		}
		Ok(None)
	}

	/// Retrieves document IDs for a given vector.
	pub(super) async fn get_docs(&self, tx: &Transaction, pt: &Vector) -> Result<Option<Ids64>> {
		let ser_vec: SerializedVector = pt.into();
		if self.use_hashed_vector {
			return self.get_hashed_docs(tx, ser_vec).await;
		}
		// Otherwise we search in the structure
		let key = self.ikb.new_hv_key(&ser_vec);
		if let Some(ed) = tx.get(&key, None).await? {
			return Ok(Some(ed.docs));
		}
		Ok(None)
	}

	/// Inserts a vector and its associated document ID using its hash.
	async fn insert_hashed(
		&self,
		tx: &Transaction,
		o: Vector,
		ser_vec: SerializedVector,
		doc_id: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let key = self.ikb.new_hh_key(ser_vec.compute_hash());
		match tx.get(&key, None).await? {
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = h.insert(tx, o).await?;
				let ehd = ElementHashedDocs::new(element_id, ser_vec, doc_id);
				tx.set(&key, &ehd, None).await?;
			}
			Some(mut ehd) => {
				if let Some(ed) = ehd.get_element_docs(&ser_vec) {
					// We already have the vector
					if let Some(docs) = ed.docs.insert(doc_id) {
						ed.docs = docs;
						tx.set(&key, &ehd, None).await?;
					};
				} else {
					//  We don't have the vector, we insert it in the graph
					let element_id = h.insert(tx, o).await?;
					ehd.add(element_id, ser_vec, doc_id);
					tx.set(&key, &ehd, None).await?;
				}
			}
		};
		Ok(())
	}

	/// Inserts a vector and its associated document ID.
	pub(super) async fn insert(
		&self,
		tx: &Transaction,
		vec: Vector,
		doc_id: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let ser_vec = SerializedVector::from(&vec);
		if self.use_hashed_vector {
			return self.insert_hashed(tx, vec, ser_vec, doc_id, h).await;
		}
		let key = self.ikb.new_hv_key(&ser_vec);
		if let Some(ed) = match tx.get(&key, None).await? {
			Some(mut ed) => {
				// We already have the vector
				ed.docs.insert(doc_id).map(|new_docs| {
					ed.docs = new_docs;
					ed
				})
			}
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = h.insert(tx, vec).await?;
				let ed = ElementDocs::new(element_id, doc_id);
				Some(ed)
			}
		} {
			tx.set(&key, &ed, None).await?;
		}
		Ok(())
	}

	/// Removes a vector and its associated document ID using its hash.
	pub(super) async fn hashed_remove(
		&self,
		tx: &Transaction,
		ser_vec: SerializedVector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let key = self.ikb.new_hh_key(ser_vec.compute_hash());
		if let Some(mut ed) = tx.get(&key, None).await? {
			match ed.remove(&ser_vec, d) {
				RemoveResult::Empty(deleted_element_id) => {
					tx.del(&key).await?;
					h.remove(tx, deleted_element_id).await?;
				}
				RemoveResult::Updated(deleted_element_id) => {
					tx.set(&key, &ed, None).await?;
					if let Some(deleted_element_id) = deleted_element_id {
						h.remove(tx, deleted_element_id).await?;
					}
				}
				RemoveResult::Unchanged => {
					// The element was not existing or already deleted
				}
			}
		}
		Ok(())
	}

	/// Removes a vector and its associated document ID.
	pub(super) async fn remove(
		&self,
		tx: &Transaction,
		o: &Vector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let ser_vec = o.into();
		if self.use_hashed_vector {
			return self.hashed_remove(tx, ser_vec, d, h).await;
		}
		let key = self.ikb.new_hv_key(&ser_vec);
		if let Some(mut ed) = tx.get(&key, None).await?
			&& let Some(new_docs) = ed.docs.remove(d)
		{
			if new_docs.is_empty() {
				tx.del(&key).await?;
				h.remove(tx, ed.e_id).await?;
			} else {
				ed.docs = new_docs;
				tx.set(&key, &ed, None).await?;
			}
		};
		Ok(())
	}
}
