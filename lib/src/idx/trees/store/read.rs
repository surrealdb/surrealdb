use crate::err::Error;
use crate::idx::trees::store::memory::TreeMemoryMap;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::Transaction;
use quick_cache::sync::Cache;
use quick_cache::GuardResult;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard};

pub enum TreeReadStore<'a, N>
where
	N: TreeNode + Debug,
{
	/// caches every read nodes
	Transaction(TreeTransactionRead<N>),
	/// Nodes are stored in memory with a read guard lock
	Memory(TreeMemoryRead<'a, N>),
}

pub(super) struct TreeTransactionRead<N>
where
	N: TreeNode,
{
	keys: TreeNodeProvider,
	cache: Cache<NodeId, Arc<StoredNode<N>>>,
}

impl<N> TreeTransactionRead<N>
where
	N: TreeNode + Debug,
{
	pub(super) fn new(keys: TreeNodeProvider, size: usize) -> Self {
		Self {
			keys,
			cache: Cache::new(size),
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self.cache.get_value_or_guard(&node_id, None) {
			GuardResult::Value(v) => Ok(v),
			GuardResult::Guard(g) => {
				let n = Arc::new(self.keys.load::<N>(tx, node_id).await?);
				g.insert(n.clone())?;
				Ok(n)
			}
			GuardResult::Timeout => Err(Error::Unreachable),
		}
	}
}

pub(super) struct TreeMemoryRead<'a, N>
where
	N: TreeNode + Debug,
{
	nodes: RwLockReadGuard<'a, TreeMemoryMap<N>>,
}

impl<'a, N> TreeMemoryRead<'a, N>
where
	N: TreeNode + Debug,
{
	pub(super) fn new(nodes: Arc<RwLock<TreeMemoryMap<N>>>) -> Self {
		Self {
			nodes: nodes.read(),
		}
	}

	pub(super) fn get_node(&self, node_id: NodeId) -> Result<Arc<StoredNode<N>>, Error> {
		self.nodes.get(&node_id).ok_or(Error::Unreachable).cloned()
	}
}
