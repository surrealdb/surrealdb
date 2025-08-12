use std::sync::Arc;

use anyhow::Result;
use revision::{Revisioned, revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::vector::{SerializedVector, Vector};
use crate::kvs::{KVValue, Transaction};
use crate::val::{RecordId, RecordIdKey};

pub(in crate::idx) struct HnswDocs {
	tb: String,
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
		tb: String,
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
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> anyhow::Result<Self> {
		Ok(Self::deserialize_revisioned(&mut val.as_slice())?)
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub(crate) struct ElementDocs {
	e_id: ElementId,
	docs: Ids64,
}

impl KVValue for ElementDocs {
	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> anyhow::Result<Self> {
		Ok(Self::deserialize_revisioned(&mut val.as_slice())?)
	}
}

pub(in crate::idx) struct VecDocs {
	ikb: IndexKeyBase,
}

impl VecDocs {
	pub(super) fn new(ikb: IndexKeyBase) -> Self {
		Self {
			ikb,
		}
	}

	pub(super) async fn get_docs(&self, tx: &Transaction, pt: &Vector) -> Result<Option<Ids64>> {
		let key = self.ikb.new_hv_key(Arc::new(pt.into()));
		if let Some(ed) = tx.get(&key, None).await? {
			Ok(Some(ed.docs))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn insert(
		&self,
		tx: &Transaction,
		o: Vector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let ser_vec = Arc::new(SerializedVector::from(&o));
		let key = self.ikb.new_hv_key(ser_vec);
		if let Some(ed) = match tx.get(&key, None).await? {
			Some(mut ed) => {
				// We already have the vector
				ed.docs.insert(d).map(|new_docs| {
					ed.docs = new_docs;
					ed
				})
			}
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = h.insert(tx, o).await?;
				let ed = ElementDocs {
					e_id: element_id,
					docs: Ids64::One(d),
				};
				Some(ed)
			}
		} {
			tx.set(&key, &ed, None).await?;
		}
		Ok(())
	}

	pub(super) async fn remove(
		&self,
		tx: &Transaction,
		o: &Vector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let key = self.ikb.new_hv_key(Arc::new(o.into()));
		if let Some(mut ed) = tx.get(&key, None).await? {
			if let Some(new_docs) = ed.docs.remove(d) {
				if new_docs.is_empty() {
					tx.del(&key).await?;
					h.remove(tx, ed.e_id).await?;
				} else {
					ed.docs = new_docs;
					tx.set(&key, &ed, None).await?;
				}
			}
		};
		Ok(())
	}
}
