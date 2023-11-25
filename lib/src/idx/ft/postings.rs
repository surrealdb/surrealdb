use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeNode, BTreeStore};
use crate::idx::trees::store::memory::ShardedTreeMemoryMap;
use crate::idx::trees::store::{IndexStores, StoreProvider, StoreRights, TreeNodeProvider};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction};
use crate::{mem_store_read_lock, mem_store_write_lock};

pub(super) type TermFrequency = u64;

pub(super) struct Postings {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<TrieKeys>,
	index_stores: IndexStores,
	mem_store: Option<ShardedTreeMemoryMap<BTreeNode<TrieKeys>>>,
	tree_node_provider: TreeNodeProvider,
	store_provider: StoreProvider,
}

impl Postings {
	pub(super) async fn new(
		ixs: IndexStores,
		sp: StoreProvider,
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		order: u32,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bp_key(None);
		let state: BState = if let Some(val) = tx.get(state_key.clone()).await? {
			BState::try_from_val(val)?
		} else {
			BState::new(order)
		};
		let tree_node_provider = TreeNodeProvider::Postings(index_key_base.clone());
		let mem_store = ixs.get_mem_store_btree_trie(&tree_node_provider, sp).await;
		Ok(Self {
			index_stores: ixs,
			state_key,
			index_key_base,
			mem_store,
			tree_node_provider,
			btree: BTree::new(state),
			store_provider: sp,
		})
	}

	async fn get_store(&self, rights: StoreRights) -> BTreeStore<TrieKeys> {
		self.index_stores
			.get_store_btree_trie(
				self.tree_node_provider.clone(),
				self.store_provider,
				rights,
				20, // TODO: Replace by configuration
			)
			.await
	}

	pub(super) async fn update_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
		term_freq: TermFrequency,
	) -> Result<(), Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		let mut store = self.get_store(StoreRights::Write).await;
		let mut mem = mem_store_write_lock!(self.mem_store);
		self.btree.insert(tx, &mut mem, &mut store, key, term_freq).await?;
		store.finish(tx).await?;
		Ok(())
	}

	pub(super) async fn get_term_frequency(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		let mut store = self.get_store(StoreRights::Read).await;
		let mem = mem_store_read_lock!(self.mem_store);
		let res = self.btree.search(tx, &mem, &mut store, &key).await?;
		store.finish(tx).await?;
		Ok(res)
	}

	pub(super) async fn remove_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		let mut store = self.get_store(StoreRights::Write).await;
		let mut mem = mem_store_write_lock!(self.mem_store);
		let res = self.btree.delete(tx, &mut mem, &mut store, key).await?;
		store.finish(tx).await?;
		Ok(res)
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<BStatistics, Error> {
		let mut store = self.get_store(StoreRights::Read).await;
		let mem = mem_store_read_lock!(self.mem_store);
		let res = self.btree.statistics(tx, &mem, &mut store).await?;
		store.finish(tx).await?;
		Ok(res)
	}

	pub(super) async fn finish(&self, tx: &mut Transaction) -> Result<(), Error> {
		self.btree.get_state().finish(tx, &self.state_key).await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::Postings;
	use crate::idx::trees::store::{IndexStores, StoreProvider};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, TransactionType::*};
	use test_log::test;

	#[test(tokio::test)]
	async fn test_postings() {
		const DEFAULT_BTREE_ORDER: u32 = 5;

		for sp in [StoreProvider::Transaction, StoreProvider::Memory] {
			let ds = Datastore::new("memory").await.unwrap();
			let ixs = IndexStores::default();

			{
				// Check empty state
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let mut p = Postings::new(
					ixs.clone(),
					sp,
					&mut tx,
					IndexKeyBase::default(),
					DEFAULT_BTREE_ORDER,
				)
				.await
				.unwrap();

				assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);

				// Add postings
				p.update_posting(&mut tx, 1, 2, 3).await.unwrap();
				p.update_posting(&mut tx, 1, 4, 5).await.unwrap();

				p.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}

			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let mut p =
					Postings::new(ixs, sp, &mut tx, IndexKeyBase::default(), DEFAULT_BTREE_ORDER)
						.await
						.unwrap();
				assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 2);

				assert_eq!(p.get_term_frequency(&mut tx, 1, 2).await.unwrap(), Some(3));
				assert_eq!(p.get_term_frequency(&mut tx, 1, 4).await.unwrap(), Some(5));

				// Check removal of doc 2
				assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), Some(3));
				// Again the same
				assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), None);
				// Remove doc 4
				assert_eq!(p.remove_posting(&mut tx, 1, 4).await.unwrap(), Some(5));

				// The underlying b-tree should be empty now
				assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);
				tx.commit().await.unwrap();
			}
		}
	}
}
