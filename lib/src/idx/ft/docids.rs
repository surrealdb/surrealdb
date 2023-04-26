use crate::err::Error;
use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::{BTree, KeyProvider, NodeId, Statistics};
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

pub(crate) type DocId = u64;

pub(super) struct DocIds {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<DocIdsKeyProvider>,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
	updated: bool,
}

impl DocIds {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		default_btree_order: usize,
	) -> Result<Self, Error> {
		let keys = DocIdsKeyProvider {
			index_key_base: index_key_base.clone(),
		};
		let state_key: Key = keys.get_state_key();
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::new(default_btree_order)
		};
		Ok(Self {
			state_key,
			index_key_base,
			btree: BTree::new(keys, state.btree),
			available_ids: state.available_ids,
			next_doc_id: state.next_doc_id,
			updated: false,
		})
	}

	fn get_next_doc_id(&mut self) -> DocId {
		// We check first if there is any available id
		if let Some(available_ids) = &mut self.available_ids {
			if let Some(available_id) = available_ids.iter().next() {
				available_ids.remove(available_id);
				if available_ids.is_empty() {
					self.available_ids = None;
				}
				return available_id;
			}
		}
		// If not, we use the sequence
		let doc_id = self.next_doc_id;
		self.next_doc_id += 1;
		doc_id
	}

	/// Returns the doc_id for the given doc_key.
	/// If the doc_id does not exists, a new one is created, and associated to the given key.
	pub(super) async fn resolve_doc_id(
		&mut self,
		tx: &mut Transaction,
		doc_key: &str,
	) -> Result<DocId, Error> {
		let doc_key: Vec<u8> = doc_key.into();
		if let Some(doc_id) = self.btree.search::<TrieKeys>(tx, &doc_key).await? {
			Ok(doc_id)
		} else {
			let doc_id = self.get_next_doc_id();
			tx.set(self.index_key_base.new_bi_key(doc_id), doc_key.clone()).await?;
			self.btree.insert::<TrieKeys>(tx, doc_key, doc_id).await?;
			self.updated = true;
			Ok(doc_id)
		}
	}

	pub(super) async fn remove_doc(
		&mut self,
		tx: &mut Transaction,
		doc_key: &str,
	) -> Result<Option<DocId>, Error> {
		let doc_key: Key = doc_key.into();
		if let Some(doc_id) = self.btree.delete::<TrieKeys>(tx, doc_key).await? {
			tx.del(self.index_key_base.new_bi_key(doc_id)).await?;
			if let Some(available_ids) = &mut self.available_ids {
				available_ids.insert(doc_id);
			} else {
				let mut available_ids = RoaringTreemap::new();
				available_ids.insert(doc_id);
				self.available_ids = Some(available_ids);
			}
			self.updated = true;
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn get_doc_key(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<String>, Error> {
		let doc_id_key = self.index_key_base.new_bi_key(doc_id);
		if let Some(val) = tx.get(doc_id_key).await? {
			Ok(Some(String::from_utf8(val)?))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		self.btree.statistics::<TrieKeys>(tx).await
	}

	pub(super) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		if self.updated || self.btree.is_updated() {
			let state = State {
				btree: self.btree.get_state().clone(),
				available_ids: self.available_ids,
				next_doc_id: self.next_doc_id,
			};
			tx.set(self.state_key, state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(Serialize, Deserialize)]
struct State {
	btree: btree::State,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl SerdeState for State {}

impl State {
	fn new(default_btree_order: usize) -> Self {
		Self {
			btree: btree::State::new(default_btree_order),
			available_ids: None,
			next_doc_id: 0,
		}
	}
}

struct DocIdsKeyProvider {
	index_key_base: IndexKeyBase,
}

impl KeyProvider for DocIdsKeyProvider {
	fn get_node_key(&self, node_id: NodeId) -> Key {
		self.index_key_base.new_bd_key(Some(node_id))
	}
	fn get_state_key(&self) -> Key {
		self.index_key_base.new_bd_key(None)
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::docids::DocIds;
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};

	const BTREE_ORDER: usize = 75;

	async fn get_doc_ids(ds: &Datastore) -> (Transaction, DocIds) {
		let mut tx = ds.transaction(true, false).await.unwrap();
		let d = DocIds::new(&mut tx, IndexKeyBase::default(), BTREE_ORDER).await.unwrap();
		(tx, d)
	}

	async fn finish(mut tx: Transaction, d: DocIds) {
		d.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_resolve_doc_id() {
		let ds = Datastore::new("memory").await.unwrap();

		// Resolve a first doc key
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		let doc_id = d.resolve_doc_id(&mut tx, "Foo").await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".to_string()));
		finish(tx, d).await;
		assert_eq!(doc_id, 0);

		// Resolve the same doc key
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		let doc_id = d.resolve_doc_id(&mut tx, "Foo").await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".to_string()));
		finish(tx, d).await;
		assert_eq!(doc_id, 0);

		// Resolve another single doc key
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		let doc_id = d.resolve_doc_id(&mut tx, "Bar").await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 2);
		assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".to_string()));
		finish(tx, d).await;
		assert_eq!(doc_id, 1);

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo").await.unwrap(), 0);
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello").await.unwrap(), 2);
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar").await.unwrap(), 1);
		assert_eq!(d.resolve_doc_id(&mut tx, "World").await.unwrap(), 3);
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
		finish(tx, d).await;

		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo").await.unwrap(), 0);
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar").await.unwrap(), 1);
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello").await.unwrap(), 2);
		assert_eq!(d.resolve_doc_id(&mut tx, "World").await.unwrap(), 3);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".to_string()));
		assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".to_string()));
		assert_eq!(d.get_doc_key(&mut tx, 2).await.unwrap(), Some("Hello".to_string()));
		assert_eq!(d.get_doc_key(&mut tx, 3).await.unwrap(), Some("World".to_string()));
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
		finish(tx, d).await;
	}

	#[tokio::test]
	async fn test_remove_doc() {
		let ds = Datastore::new("memory").await.unwrap();

		// Create two docs
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo").await.unwrap(), 0);
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar").await.unwrap(), 1);
		finish(tx, d).await;

		// Remove doc 1
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Dummy").await.unwrap(), None);
		assert_eq!(d.remove_doc(&mut tx, "Foo").await.unwrap(), Some(0));
		finish(tx, d).await;

		// Check 'Foo' has been removed
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Foo").await.unwrap(), None);
		finish(tx, d).await;

		// Insert a new doc - should take the available id 1
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello").await.unwrap(), 0);
		finish(tx, d).await;

		// Remove doc 2
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Dummy").await.unwrap(), None);
		assert_eq!(d.remove_doc(&mut tx, "Bar").await.unwrap(), Some(1));
		finish(tx, d).await;

		// Check 'Bar' has been removed
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Foo").await.unwrap(), None);
		finish(tx, d).await;

		// Insert a new doc - should take the available id 2
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "World").await.unwrap(), 1);
		finish(tx, d).await;
	}
}
