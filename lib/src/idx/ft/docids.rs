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
		default_btree_order: u32,
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

	pub(super) async fn get_doc_id(
		&self,
		tx: &mut Transaction,
		doc_key: Key,
	) -> Result<Option<DocId>, Error> {
		self.btree.search::<TrieKeys>(tx, &doc_key).await
	}

	/// Returns the doc_id for the given doc_key.
	/// If the doc_id does not exists, a new one is created, and associated to the given key.
	pub(super) async fn resolve_doc_id(
		&mut self,
		tx: &mut Transaction,
		doc_key: Key,
	) -> Result<Resolved, Error> {
		if let Some(doc_id) = self.btree.search::<TrieKeys>(tx, &doc_key).await? {
			Ok(Resolved::Existing(doc_id))
		} else {
			let doc_id = self.get_next_doc_id();
			tx.set(self.index_key_base.new_bi_key(doc_id), doc_key.clone()).await?;
			self.btree.insert::<TrieKeys>(tx, doc_key, doc_id).await?;
			self.updated = true;
			Ok(Resolved::New(doc_id))
		}
	}

	pub(super) async fn remove_doc(
		&mut self,
		tx: &mut Transaction,
		doc_key: Key,
	) -> Result<Option<DocId>, Error> {
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
	) -> Result<Option<Key>, Error> {
		let doc_id_key = self.index_key_base.new_bi_key(doc_id);
		if let Some(val) = tx.get(doc_id_key).await? {
			Ok(Some(val))
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
	fn new(default_btree_order: u32) -> Self {
		Self {
			btree: btree::State::new(default_btree_order),
			available_ids: None,
			next_doc_id: 0,
		}
	}
}

#[derive(Debug, PartialEq)]
pub(super) enum Resolved {
	New(DocId),
	Existing(DocId),
}

impl Resolved {
	pub(super) fn doc_id(&self) -> &DocId {
		match self {
			Resolved::New(doc_id) => doc_id,
			Resolved::Existing(doc_id) => doc_id,
		}
	}

	pub(super) fn was_existing(&self) -> bool {
		match self {
			Resolved::New(_) => false,
			Resolved::Existing(_) => true,
		}
	}
}

#[derive(Clone)]
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
	use crate::idx::ft::docids::{DocIds, Resolved};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};

	const BTREE_ORDER: u32 = 7;

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
		let doc_id = d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".into()));
		finish(tx, d).await;
		assert_eq!(doc_id, Resolved::New(0));

		// Resolve the same doc key
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		let doc_id = d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".into()));
		finish(tx, d).await;
		assert_eq!(doc_id, Resolved::Existing(0));

		// Resolve another single doc key
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		let doc_id = d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap();
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 2);
		assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".into()));
		finish(tx, d).await;
		assert_eq!(doc_id, Resolved::New(1));

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap(), Resolved::Existing(0));
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello".into()).await.unwrap(), Resolved::New(2));
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap(), Resolved::Existing(1));
		assert_eq!(d.resolve_doc_id(&mut tx, "World".into()).await.unwrap(), Resolved::New(3));
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
		finish(tx, d).await;

		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap(), Resolved::Existing(0));
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap(), Resolved::Existing(1));
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello".into()).await.unwrap(), Resolved::Existing(2));
		assert_eq!(d.resolve_doc_id(&mut tx, "World".into()).await.unwrap(), Resolved::Existing(3));
		assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".into()));
		assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".into()));
		assert_eq!(d.get_doc_key(&mut tx, 2).await.unwrap(), Some("Hello".into()));
		assert_eq!(d.get_doc_key(&mut tx, 3).await.unwrap(), Some("World".into()));
		assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
		finish(tx, d).await;
	}

	#[tokio::test]
	async fn test_remove_doc() {
		let ds = Datastore::new("memory").await.unwrap();

		// Create two docs
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap(), Resolved::New(0));
		assert_eq!(d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap(), Resolved::New(1));
		finish(tx, d).await;

		// Remove doc 1
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Dummy".into()).await.unwrap(), None);
		assert_eq!(d.remove_doc(&mut tx, "Foo".into()).await.unwrap(), Some(0));
		finish(tx, d).await;

		// Check 'Foo' has been removed
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Foo".into()).await.unwrap(), None);
		finish(tx, d).await;

		// Insert a new doc - should take the available id 1
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "Hello".into()).await.unwrap(), Resolved::New(0));
		finish(tx, d).await;

		// Remove doc 2
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Dummy".into()).await.unwrap(), None);
		assert_eq!(d.remove_doc(&mut tx, "Bar".into()).await.unwrap(), Some(1));
		finish(tx, d).await;

		// Check 'Bar' has been removed
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.remove_doc(&mut tx, "Foo".into()).await.unwrap(), None);
		finish(tx, d).await;

		// Insert a new doc - should take the available id 2
		let (mut tx, mut d) = get_doc_ids(&ds).await;
		assert_eq!(d.resolve_doc_id(&mut tx, "World".into()).await.unwrap(), Resolved::New(1));
		finish(tx, d).await;
	}
}
