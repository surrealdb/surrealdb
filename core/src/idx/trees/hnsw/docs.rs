use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::vector::SharedVector;
use crate::idx::{IndexKeyBase, VersionedStore};
use crate::kvs::{Key, Transaction};
use crate::sql::{Id, Thing};
use ahash::HashMap;
use derive::Store;
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;

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

pub(in crate::idx) struct VecDocs {
	ikb: IndexKeyBase,
	map: HashMap<SharedVector, (Ids64, ElementId)>,
}

impl VecDocs {
	pub(super) fn new(ikb: IndexKeyBase) -> Self {
		Self {
			ikb,
			map: HashMap::default(),
		}
	}

	pub(super) fn get_docs(&self, pt: &SharedVector) -> Option<&Ids64> {
		self.map.get(pt).map(|(doc_ids, _)| doc_ids)
	}

	pub(super) fn insert(&mut self, o: SharedVector, d: DocId, h: &mut HnswFlavor) {
		match self.map.entry(o) {
			Entry::Occupied(mut e) => {
				let (docs, element_id) = e.get_mut();
				if let Some(new_docs) = docs.insert(d) {
					let element_id = *element_id;
					e.insert((new_docs, element_id));
				}
			}
			Entry::Vacant(e) => {
				let o = e.key().clone();
				let element_id = h.insert(o);
				e.insert((Ids64::One(d), element_id));
			}
		}
	}

	pub(super) fn remove(&mut self, o: SharedVector, d: DocId, h: &mut HnswFlavor) {
		if let Entry::Occupied(mut e) = self.map.entry(o) {
			let (docs, e_id) = e.get_mut();
			if let Some(new_docs) = docs.remove(d) {
				let e_id = *e_id;
				if new_docs.is_empty() {
					e.remove();
					h.remove(e_id);
				} else {
					e.insert((new_docs, e_id));
				}
			}
		}
	}
}
