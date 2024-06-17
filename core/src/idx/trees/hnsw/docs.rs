use crate::ctx::Context;
use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::{IndexKeyBase, VersionedStore};
use crate::kvs::{Key, Transaction};
use crate::sql::{Id, Thing};
use derive::Store;
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

pub(in crate::idx) struct HnswDocs {
	tb: String,
	ikb: IndexKeyBase,
	#[allow(unused)]
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
	pub async fn new(tx: &mut Transaction, tb: String, ikb: IndexKeyBase) -> Result<Self, Error> {
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

	pub(super) async fn resolve(&mut self, tx: &mut Transaction, id: Id) -> Result<DocId, Error> {
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
		ctx: &Context<'_>,
		doc_id: DocId,
	) -> Result<Option<Thing>, Error> {
		let doc_key = self.ikb.new_hd_key(Some(doc_id));
		if let Some(val) = ctx.tx_lock().await.get(doc_key).await? {
			let id: Id = val.into();
			Ok(Some(Thing::from((self.tb.to_owned(), id))))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn remove(
		&mut self,
		tx: &mut Transaction,
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
}
