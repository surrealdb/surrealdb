use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::vector::SharedVector;
use crate::idx::{IndexKeyBase, VersionedStore};
use crate::kvs::{Key, Transaction, Val};
use crate::sql::{Id, Thing};
use derive::Store;
use revision::{revisioned, Revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::sync::Arc;

pub(in crate::idx) struct HnswDocs {
	tb: String,
	ikb: IndexKeyBase,
	state_key: Key,
	state_updated: bool,
	state: State,
}

#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize, Store)]
#[non_exhaustive]
struct State {
	available: RoaringTreemap,
	next_doc_id: DocId,
}

impl VersionedStore for State {}

impl HnswDocs {
	pub(in crate::idx) async fn new(
		tx: &Transaction,
		tb: String,
		ikb: IndexKeyBase,
	) -> Result<Self, Error> {
		let state_key = ikb.new_hd_key(None);
		let state = if let Some(k) = tx.get(state_key.clone()).await? {
			VersionedStore::try_from(k)?
		} else {
			State::default()
		};
		Ok(Self {
			tb,
			ikb,
			state_updated: false,
			state_key,
			state,
		})
	}

	pub(super) async fn resolve(&mut self, tx: &Transaction, id: Id) -> Result<DocId, Error> {
		let id_key = self.ikb.new_hi_key(id.clone());
		if let Some(v) = tx.get(id_key.clone()).await? {
			let doc_id = u64::from_be_bytes(v.try_into().unwrap());
			Ok(doc_id)
		} else {
			let doc_id = self.next_doc_id();
			tx.set(id_key, doc_id.to_be_bytes()).await?;
			let doc_key = self.ikb.new_hd_key(Some(doc_id));
			tx.set(doc_key, id).await?;
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
	) -> Result<Option<Thing>, Error> {
		let doc_key = self.ikb.new_hd_key(Some(doc_id));
		if let Some(val) = tx.get(doc_key).await? {
			let id: Id = val.into();
			Ok(Some(Thing::from((self.tb.to_owned(), id))))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn remove(
		&mut self,
		tx: &Transaction,
		id: Id,
	) -> Result<Option<DocId>, Error> {
		let id_key = self.ikb.new_hi_key(id);
		if let Some(v) = tx.get(id_key.clone()).await? {
			let doc_id = u64::from_be_bytes(v.try_into().unwrap());
			let doc_key = self.ikb.new_hd_key(Some(doc_id));
			tx.del(doc_key).await?;
			tx.del(id_key).await?;
			self.state.available.insert(doc_id);
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &Transaction) -> Result<(), Error> {
		if self.state_updated {
			tx.set(self.state_key.clone(), VersionedStore::try_into(&self.state)?).await?;
			self.state_updated = true;
		}
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
#[non_exhaustive]
struct ElementDocs {
	e_id: ElementId,
	docs: Ids64,
}

impl VersionedStore for ElementDocs {}

pub(in crate::idx) struct VecDocs {
	ikb: IndexKeyBase,
}

impl VecDocs {
	pub(super) fn new(ikb: IndexKeyBase) -> Self {
		Self {
			ikb,
		}
	}

	pub(super) async fn get_docs(
		&self,
		tx: &Transaction,
		pt: &SharedVector,
	) -> Result<Option<Ids64>, Error> {
		let key = self.ikb.new_hv_key(Arc::new(pt.deref().into()));
		if let Some(val) = tx.get(key).await? {
			let ed: ElementDocs = VersionedStore::try_from(val)?;
			Ok(Some(ed.docs))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn insert(
		&self,
		tx: &Transaction,
		o: SharedVector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<(), Error> {
		let key = self.ikb.new_hv_key(Arc::new(o.deref().into()));
		if let Some(ed) = match tx.get(key.clone()).await? {
			Some(val) => {
				// We already have the vector
				let mut ed: ElementDocs = VersionedStore::try_from(val)?;
				ed.docs.insert(d).map(|new_docs| {
					ed.docs = new_docs;
					ed
				})
			}
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = h.insert(o);
				let ed = ElementDocs {
					e_id: element_id,
					docs: Ids64::One(d),
				};
				Some(ed)
			}
		} {
			let val: Val = VersionedStore::try_into(&ed)?;
			tx.set(key, val).await?;
		}
		Ok(())
	}

	pub(super) async fn remove(
		&self,
		tx: &Transaction,
		o: SharedVector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<(), Error> {
		let key = self.ikb.new_hv_key(Arc::new(o.deref().into()));
		if let Some(val) = tx.get(key.clone()).await? {
			let mut ed = ElementDocs::deserialize_revisioned(&mut val.as_slice())?;
			if let Some(new_docs) = ed.docs.remove(d) {
				if new_docs.is_empty() {
					tx.del(key).await?;
					h.remove(ed.e_id);
				} else {
					ed.docs = new_docs;
					let mut val = Vec::new();
					ed.serialize_revisioned(&mut val)?;
					tx.set(key, val).await?;
				}
			}
		};
		Ok(())
	}
}
