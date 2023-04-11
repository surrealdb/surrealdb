use crate::err::Error;
use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::{BTree, Statistics};
use crate::idx::{BaseStateKey, Domain, IndexId, SerdeState, DOC_IDS_DOMAIN, DOC_KEYS_DOMAIN};
use crate::kvs::{Key, Transaction};
use derive::Key;
use nom::AsBytes;
use serde::{Deserialize, Serialize};

pub(super) type DocId = u64;

pub(super) struct DocIds {
	state_key: Key,
	index_id: IndexId,
	state: State,
	updated: bool,
}

#[derive(Serialize, Deserialize, Key)]
struct DocKey {
	domain: Domain,
	index_id: IndexId,
	doc_id: DocId,
}

#[derive(Serialize, Deserialize)]
struct State {
	btree: BTree,
	next_doc_id: DocId,
}

impl State {
	fn new(index_id: IndexId, btree_order: usize) -> Self {
		Self {
			btree: BTree::new(DOC_IDS_DOMAIN, index_id, btree_order),
			next_doc_id: 0,
		}
	}
}

impl SerdeState for State {}

impl DocIds {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_id: IndexId,
		default_btree_order: usize,
	) -> Result<Self, Error> {
		let state_key: Key = BaseStateKey::new(DOC_IDS_DOMAIN, index_id).into();
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::new(index_id, default_btree_order)
		};
		Ok(Self {
			state_key,
			state,
			updated: false,
			index_id,
		})
	}

	pub(super) async fn resolve_doc_id(
		&mut self,
		tx: &mut Transaction,
		key: &str,
	) -> Result<DocId, Error> {
		let key = key.into();
		if let Some(doc_id) = self.state.btree.search::<TrieKeys>(tx, &key).await? {
			Ok(doc_id)
		} else {
			let doc_id = self.state.next_doc_id;
			let doc_key: Key = DocKey {
				domain: DOC_KEYS_DOMAIN,
				index_id: self.index_id,
				doc_id,
			}
			.into();
			tx.set(doc_key, key.as_bytes().to_vec()).await?;
			self.state.btree.insert::<TrieKeys>(tx, key, doc_id).await?;
			self.state.next_doc_id += 1;
			self.updated = true;
			Ok(doc_id)
		}
	}

	pub(super) async fn get_doc_key(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<String>, Error> {
		let doc_key: Key = DocKey {
			domain: DOC_KEYS_DOMAIN,
			index_id: self.index_id,
			doc_id,
		}
		.into();
		if let Some(val) = tx.get(doc_key).await? {
			Ok(Some(String::from_utf8(val)?))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		self.state.btree.statistics::<TrieKeys>(tx).await
	}

	pub(super) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		if self.updated || self.state.btree.is_updated() {
			tx.set(self.state_key, self.state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::docids::DocIds;
	use crate::kvs::Datastore;

	#[tokio::test]
	async fn test_resolve_doc_id() {
		const BTREE_ORDER: usize = 75;

		let ds = Datastore::new("memory").await.unwrap();

		// Resolve a first doc key
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut d = DocIds::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		let doc_id = d.resolve_doc_id(&mut tx, "Foo").await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".to_string()));
		d.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(doc_id, 0);

		// Resolve the same doc key
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut d = DocIds::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		let doc_id = d.resolve_doc_id(&mut tx, "Foo").await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".to_string()));
		d.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(doc_id, 0);

		// Resolve another single doc key
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut d = DocIds::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		let doc_id = d.resolve_doc_id(&mut tx, "Bar").await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 2);
		assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".to_string()));
		d.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(doc_id, 1);

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut d = DocIds::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo").await.unwrap(), 0);
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello").await.unwrap(), 2);
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar").await.unwrap(), 1);
		assert_eq!(d.resolve_doc_id(&mut tx, "World").await.unwrap(), 3);
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
		d.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut d = DocIds::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo").await.unwrap(), 0);
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar").await.unwrap(), 1);
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello").await.unwrap(), 2);
		assert_eq!(d.resolve_doc_id(&mut tx, "World").await.unwrap(), 3);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".to_string()));
		assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".to_string()));
		assert_eq!(d.get_doc_key(&mut tx, 2).await.unwrap(), Some("Hello".to_string()));
		assert_eq!(d.get_doc_key(&mut tx, 3).await.unwrap(), Some("World".to_string()));
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
		d.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}
}
