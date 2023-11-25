use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeNode, BTreeStore, Payload};
use crate::idx::trees::store::memory::ShardedTreeMemoryMap;
use crate::idx::trees::store::{IndexStores, StoreProvider, StoreRights, TreeNodeProvider};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction};
use crate::{mem_store_read_lock, mem_store_write_lock};

pub(super) type DocLength = u64;

pub(super) struct DocLengths {
	index_stores: IndexStores,
	mem_store: Option<ShardedTreeMemoryMap<BTreeNode<TrieKeys>>>,
	tree_node_provider: TreeNodeProvider,
	store_provider: StoreProvider,
	state_key: Key,
	btree: BTree<TrieKeys>,
}

impl DocLengths {
	pub(super) async fn new(
		ixs: IndexStores,
		sp: StoreProvider,
		tx: &mut Transaction,
		ikb: IndexKeyBase,
		default_btree_order: u32,
	) -> Result<Self, Error> {
		let state_key: Key = ikb.new_bl_key(None);
		let state: BState = if let Some(val) = tx.get(state_key.clone()).await? {
			BState::try_from_val(val)?
		} else {
			BState::new(default_btree_order)
		};
		let tree_node_provider = TreeNodeProvider::DocLengths(ikb);
		let mem_store = ixs.get_mem_store_btree_trie(&tree_node_provider, sp).await;
		Ok(Self {
			index_stores: ixs,
			store_provider: sp,
			mem_store,
			tree_node_provider,
			state_key,
			btree: BTree::new(state),
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

	pub(super) async fn get_doc_length(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<DocLength>, Error> {
		let mut store = self.get_store(StoreRights::Read).await;
		let mem = mem_store_read_lock!(self.mem_store);
		let res = self.btree.search(tx, &mem, &mut store, &doc_id.to_be_bytes().to_vec()).await?;
		store.finish(tx).await?;
		Ok(res)
	}

	pub(super) async fn set_doc_length(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
		doc_length: DocLength,
	) -> Result<(), Error> {
		let mut store = self.get_store(StoreRights::Write).await;
		let mut mem = mem_store_write_lock!(self.mem_store);
		self.btree
			.insert(tx, &mut mem, &mut store, doc_id.to_be_bytes().to_vec(), doc_length)
			.await?;
		store.finish(tx).await?;
		Ok(())
	}

	pub(super) async fn remove_doc_length(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<Payload>, Error> {
		let mut store = self.get_store(StoreRights::Write).await;
		let mut mem = mem_store_write_lock!(self.mem_store);
		let res =
			self.btree.delete(tx, &mut mem, &mut store, doc_id.to_be_bytes().to_vec()).await?;
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
	use crate::idx::ft::doclength::DocLengths;
	use crate::idx::trees::store::{IndexStores, StoreProvider};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, TransactionType::*};

	#[tokio::test]
	async fn test_doc_lengths() {
		for sp in [StoreProvider::Transaction, StoreProvider::Memory] {
			const BTREE_ORDER: u32 = 7;

			let ds = Datastore::new("memory").await.unwrap();
			let ixs = IndexStores::default();

			let mut tx = ds.transaction(Write, Optimistic).await.unwrap();

			{
				// Check empty state
				let l =
					DocLengths::new(ixs.clone(), sp, &mut tx, IndexKeyBase::default(), BTREE_ORDER)
						.await
						.unwrap();
				assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 0);
				let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
				assert_eq!(dl, None);
			}

			{
				// Set a doc length
				let mut l =
					DocLengths::new(ixs.clone(), sp, &mut tx, IndexKeyBase::default(), BTREE_ORDER)
						.await
						.unwrap();
				l.set_doc_length(&mut tx, 99, 199).await.unwrap();
				assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
				let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
				l.finish(&mut tx).await.unwrap();
				assert_eq!(dl, Some(199));
			}

			{
				// Update doc length
				let mut l =
					DocLengths::new(ixs.clone(), sp, &mut tx, IndexKeyBase::default(), BTREE_ORDER)
						.await
						.unwrap();
				l.set_doc_length(&mut tx, 99, 299).await.unwrap();
				assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
				let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
				l.finish(&mut tx).await.unwrap();
				assert_eq!(dl, Some(299));
			}

			{
				// Remove doc lengths
				let mut l =
					DocLengths::new(ixs.clone(), sp, &mut tx, IndexKeyBase::default(), BTREE_ORDER)
						.await
						.unwrap();
				assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), Some(299));
				assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), None);
			}
			tx.commit().await.unwrap()
		}
	}
}
