use crate::err::Error;
use crate::idx::trees::store::memory::TreeMemoryMap;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::Transaction;
use quick_cache::sync::Cache;
use quick_cache::GuardResult;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLockReadGuard;

pub struct TreeTransactionRead<N>
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
				g.insert(n.clone()).ok();
				Ok(n)
			}
			GuardResult::Timeout => Err(Error::Unreachable),
		}
	}
}

pub(super) struct TreeMemoryRead {}

impl TreeMemoryRead {
	pub(super) fn get_node<N>(
		mem: &Option<RwLockReadGuard<'_, TreeMemoryMap<N>>>,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error>
	where
		N: TreeNode + Debug,
	{
		if let Some(nodes) = mem {
			nodes.get(&node_id).ok_or(Error::Unreachable).cloned()
		} else {
			Err(Error::Unreachable)
		}
	}
}
