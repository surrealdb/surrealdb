use crate::err::Error;
use crate::idx::trees::bkeys::FstKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeStore};
use crate::idx::trees::store::{IndexStores, StoreProvider, StoreRights, TreeNodeProvider};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction};
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
pub(crate) type TermId = u64;

pub(super) struct Terms {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<FstKeys>,
	index_stores: IndexStores,
	tree_node_provider: TreeNodeProvider,
	store_provider: StoreProvider,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
	updated: bool,
}

impl Terms {
	pub(super) async fn new(
		index_stores: IndexStores,
		store_provider: StoreProvider,
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		default_btree_order: u32,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bt_key(None);
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::new(default_btree_order)
		};
		let tree_node_provider = TreeNodeProvider::Terms(index_key_base.clone());
		Ok(Self {
			index_stores,
			state_key,
			index_key_base,
			btree: BTree::new(state.btree),
			store_provider,
			tree_node_provider,
			available_ids: state.available_ids,
			next_term_id: state.next_term_id,
			updated: false,
		})
	}

	async fn get_store(&self, rights: StoreRights) -> BTreeStore<FstKeys> {
		self.index_stores
			.get_store_btree_fst(
				self.tree_node_provider.clone(),
				self.store_provider,
				rights,
				20, // TODO: Replace by configuration
			)
			.await
	}

	fn get_next_term_id(&mut self) -> TermId {
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
		let term_id = self.next_term_id;
		self.next_term_id += 1;
		term_id
	}

	pub(super) async fn resolve_term_id(
		&mut self,
		tx: &mut Transaction,
		term: &str,
	) -> Result<TermId, Error> {
		let term_key = term.into();
		{
			let mut store = self.get_store(StoreRights::Read).await;
			if let Some(term_id) = self.btree.search(tx, &mut store, &term_key).await? {
				return Ok(term_id);
			}
		}
		let term_id = self.get_next_term_id();
		tx.set(self.index_key_base.new_bu_key(term_id), term_key.clone()).await?;
		let mut store = self.get_store(StoreRights::Write).await;
		self.btree.insert(tx, &mut store, term_key, term_id).await?;
		self.updated = true;
		Ok(term_id)
	}

	pub(super) async fn get_term_id(
		&self,
		tx: &mut Transaction,
		term: &str,
	) -> Result<Option<TermId>, Error> {
		let mut store = self.get_store(StoreRights::Read).await;
		self.btree.search(tx, &mut store, &term.into()).await
	}

	pub(super) async fn remove_term_id(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
	) -> Result<(), Error> {
		let term_id_key = self.index_key_base.new_bu_key(term_id);
		if let Some(term_key) = tx.get(term_id_key.clone()).await? {
			let mut store = self.get_store(StoreRights::Write).await;
			self.btree.delete(tx, &mut store, term_key.clone()).await?;
			tx.del(term_id_key).await?;
			if let Some(available_ids) = &mut self.available_ids {
				available_ids.insert(term_id);
			} else {
				let mut available_ids = RoaringTreemap::new();
				available_ids.insert(term_id);
				self.available_ids = Some(available_ids);
			}
			self.updated = true;
		}
		Ok(())
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<BStatistics, Error> {
		let mut store = self.get_store(StoreRights::Read).await;
		self.btree.statistics(tx, &mut store).await
	}

	pub(super) async fn finish(&mut self, tx: &mut Transaction) -> Result<(), Error> {
		let updated = self.get_store(StoreRights::Write).await.finish(tx).await?;
		if self.updated || updated {
			let state = State {
				btree: self.btree.get_state().clone(),
				available_ids: self.available_ids.take(),
				next_term_id: self.next_term_id,
			};
			tx.set(self.state_key.clone(), state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(Serialize, Deserialize)]
#[revisioned(revision = 1)]
struct State {
	btree: BState,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
}

impl VersionedSerdeState for State {}

impl State {
	fn new(default_btree_order: u32) -> Self {
		Self {
			btree: BState::new(default_btree_order),
			available_ids: None,
			next_term_id: 0,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::TermFrequency;
	use crate::idx::ft::terms::Terms;
	use crate::idx::trees::store::{IndexStores, StoreProvider};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, TransactionType::*};
	use rand::{thread_rng, Rng};
	use std::collections::HashSet;

	fn random_term(key_length: usize) -> String {
		thread_rng()
			.sample_iter(&rand::distributions::Alphanumeric)
			.take(key_length)
			.map(char::from)
			.collect()
	}

	fn unique_terms(key_length: usize, count: usize) -> HashSet<String> {
		let mut set = HashSet::new();
		while set.len() < count {
			set.insert(random_term(key_length));
		}
		set
	}

	#[tokio::test]
	async fn test_resolve_terms() {
		const BTREE_ORDER: u32 = 7;

		let idx = IndexKeyBase::default();

		for sp in [StoreProvider::Transaction, StoreProvider::Memory] {
			let ds = Datastore::new("memory").await.unwrap();
			let ixs = IndexStores::default();

			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let mut t =
					Terms::new(ixs.clone(), sp, &mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
				t.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}

			// Resolve a first term
			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let mut t =
					Terms::new(ixs.clone(), sp, &mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
				assert_eq!(t.resolve_term_id(&mut tx, "C").await.unwrap(), 0);
				assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 1);
				t.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}

			// Resolve a second term
			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let mut t =
					Terms::new(ixs.clone(), sp, &mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
				assert_eq!(t.resolve_term_id(&mut tx, "D").await.unwrap(), 1);
				assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 2);
				t.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}

			// Resolve two existing terms with new frequencies
			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let mut t =
					Terms::new(ixs.clone(), sp, &mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
				assert_eq!(t.resolve_term_id(&mut tx, "C").await.unwrap(), 0);
				assert_eq!(t.resolve_term_id(&mut tx, "D").await.unwrap(), 1);

				assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 2);
				t.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}

			// Resolve one existing terms and two new terms
			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let mut t = Terms::new(ixs, sp, &mut tx, idx.clone(), BTREE_ORDER).await.unwrap();

				assert_eq!(t.resolve_term_id(&mut tx, "A").await.unwrap(), 2);
				assert_eq!(t.resolve_term_id(&mut tx, "C").await.unwrap(), 0);
				assert_eq!(t.resolve_term_id(&mut tx, "E").await.unwrap(), 3);

				assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 4);
				t.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}
		}
	}

	#[tokio::test]
	async fn test_deletion() {
		const BTREE_ORDER: u32 = 7;

		let idx = IndexKeyBase::default();

		for sp in [StoreProvider::Transaction, StoreProvider::Memory] {
			let ds = Datastore::new("memory").await.unwrap();
			let ixs = IndexStores::default();

			let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
			let mut t = Terms::new(ixs, sp, &mut tx, idx.clone(), BTREE_ORDER).await.unwrap();

			// Check removing an non-existing term id returns None
			assert!(t.remove_term_id(&mut tx, 0).await.is_ok());

			// Create few terms
			t.resolve_term_id(&mut tx, "A").await.unwrap();
			t.resolve_term_id(&mut tx, "C").await.unwrap();
			t.resolve_term_id(&mut tx, "E").await.unwrap();

			for term in ["A", "C", "E"] {
				let term_id = t.get_term_id(&mut tx, term).await.unwrap();
				if let Some(term_id) = term_id {
					t.remove_term_id(&mut tx, term_id).await.unwrap();
					assert_eq!(t.get_term_id(&mut tx, term).await.unwrap(), None);
				} else {
					panic!("Term ID not found: {}", term);
				}
			}

			// Check id recycling
			assert_eq!(t.resolve_term_id(&mut tx, "B").await.unwrap(), 0);
			assert_eq!(t.resolve_term_id(&mut tx, "D").await.unwrap(), 1);

			t.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}
	}

	fn random_term_freq_vec(term_count: usize) -> Vec<(String, TermFrequency)> {
		let mut i = 1;
		let mut vec = Vec::with_capacity(term_count);
		for term in unique_terms(5, term_count) {
			vec.push((term, i));
			i += 1;
		}
		vec
	}

	async fn test_resolve_100_docs_with_50_words_one_by_one(sp: StoreProvider) {
		let ds = Datastore::new("memory").await.unwrap();
		for _ in 0..100 {
			let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
			let ixs = IndexStores::default();
			let mut t = Terms::new(ixs, sp, &mut tx, IndexKeyBase::default(), 100).await.unwrap();
			let terms_string = random_term_freq_vec(50);
			for (term, _) in terms_string {
				t.resolve_term_id(&mut tx, &term).await.unwrap();
			}
			t.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_resolve_100_docs_with_50_words_one_by_one_memory() {
		test_resolve_100_docs_with_50_words_one_by_one(StoreProvider::Memory)
	}

	#[tokio::test]
	async fn test_resolve_100_docs_with_50_words_one_by_one_transaction() {
		test_resolve_100_docs_with_50_words_one_by_one(StoreProvider::Transaction)
	}

	async fn test_resolve_100_docs_with_50_words_batch_of_10(sp: StoreProvider) {
		let ds = Datastore::new("memory").await.unwrap();
		for _ in 0..10 {
			let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
			let ixs = IndexStores::default();
			let mut t = Terms::new(ixs, sp, &mut tx, IndexKeyBase::default(), 100).await.unwrap();
			for _ in 0..10 {
				let terms_string = random_term_freq_vec(50);
				for (term, _) in terms_string {
					t.resolve_term_id(&mut tx, &term).await.unwrap();
				}
			}
			t.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_resolve_100_docs_with_50_words_batch_of_10_memory() {
		test_resolve_100_docs_with_50_words_batch_of_10(StoreProvider::Memory)
	}

	#[tokio::test]
	async fn test_resolve_100_docs_with_50_words_batch_of_10_transaction() {
		test_resolve_100_docs_with_50_words_batch_of_10(StoreProvider::Transaction)
	}
}
