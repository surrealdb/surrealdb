use crate::err::Error;
use crate::idx::trees::store::cache::TreeCache;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use hashbrown::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::mem;
use std::sync::Arc;

#[non_exhaustive]
pub struct TreeWrite<N>
where
	N: TreeNode + Debug + Clone,
{
	np: TreeNodeProvider,
	cache: Arc<TreeCache<N>>,
	cached: HashSet<NodeId>,
	nodes: HashMap<NodeId, StoredNode<N>>,
	updated: HashSet<NodeId>,
	removed: HashMap<NodeId, Key>,
	#[cfg(debug_assertions)]
	out: HashSet<NodeId>,
}

impl<N> TreeWrite<N>
where
	N: TreeNode + Clone + Debug + Display,
{
	pub(super) fn new(np: TreeNodeProvider, cache: Arc<TreeCache<N>>) -> Self {
		Self {
			np,
			cache,
			cached: HashSet::new(),
			nodes: HashMap::new(),
			updated: HashSet::new(),
			removed: HashMap::new(),
			#[cfg(debug_assertions)]
			out: HashSet::new(),
		}
	}

	pub(super) async fn get_node_mut(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		#[cfg(debug_assertions)]
		{
			self.out.insert(node_id);
			if self.removed.contains_key(&node_id) {
				return Err(Error::Unreachable("TreeTransactionWrite::get_node_mut"));
			}
		}
		if let Some(n) = self.nodes.remove(&node_id) {
			return Ok(n);
		}
		let r = self.cache.get_node(tx, node_id).await?;
		self.cached.insert(node_id);
		Ok(StoredNode::new(r.n.clone(), r.id, r.key.clone(), r.size))
	}

	pub(super) fn set_node(&mut self, node: StoredNode<N>, updated: bool) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		self.out.remove(&node.id);

		if updated {
			self.updated.insert(node.id);
			self.cached.remove(&node.id);
		}
		if self.removed.contains_key(&node.id) {
			return Err(Error::Unreachable("TreeTransactionWrite::set_node(2)"));
		}
		self.nodes.insert(node.id, node);
		Ok(())
	}

	pub(super) fn new_node(&mut self, id: NodeId, node: N) -> StoredNode<N> {
		#[cfg(debug_assertions)]
		self.out.insert(id);

		StoredNode::new(node, id, self.np.get_key(id), 0)
	}

	pub(super) fn remove_node(&mut self, node_id: NodeId, node_key: Key) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			if self.nodes.contains_key(&node_id) {
				return Err(Error::Unreachable("TreeTransactionWrite::remove_node"));
			}
			self.out.remove(&node_id);
		}
		self.updated.remove(&node_id);
		self.cached.remove(&node_id);
		self.removed.insert(node_id, node_key);
		Ok(())
	}

	pub(super) async fn finish(
		&mut self,
		tx: &mut Transaction,
	) -> Result<Option<TreeCache<N>>, Error> {
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				return Err(Error::Unreachable("TreeTransactionWrite::finish(1)"));
			}
		}
		if self.updated.is_empty() && self.removed.is_empty() {
			return Ok(None);
		}
		// Create a new cache hydrated with non-updated and non-removed previous cache entries.
		let new_cache = self.cache.next_generation(&self.updated, &self.removed).await;

		let updated = mem::take(&mut self.updated);
		for node_id in updated {
			if let Some(mut node) = self.nodes.remove(&node_id) {
				node.n.prepare_save();
				self.np.save(tx, &mut node).await?;
				// Update the cache with updated entries.
				new_cache.set_node(node).await;
			} else {
				return Err(Error::Unreachable("TreeTransactionWrite::finish(2)"));
			}
		}
		let removed = mem::take(&mut self.removed);
		for (node_id, node_key) in removed {
			tx.del(node_key).await?;
			new_cache.remove_node(&node_id).await;
		}

		Ok(Some(new_cache))
	}
}

#[cfg(debug_assertions)]
impl<N> Drop for TreeWrite<N>
where
	N: TreeNode + Debug + Clone,
{
	fn drop(&mut self) {
		if !self.updated.is_empty() {
			warn!("TreeWrite::finish not called?: updated not empty: {:?}", self.updated);
		}
		if !self.removed.is_empty() {
			warn!("TreeWrite::finish not called?: removed not empty: {:?}", self.removed);
		}
	}
}

#[non_exhaustive]
pub struct TreeRead<N>
where
	N: TreeNode + Debug + Clone,
{
	cache: Arc<TreeCache<N>>,
}

impl<N> TreeRead<N>
where
	N: TreeNode + Debug + Clone,
{
	pub(super) fn new(cache: Arc<TreeCache<N>>) -> Self {
		Self {
			cache,
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		let r = self.cache.get_node(tx, node_id).await?;
		#[cfg(debug_assertions)]
		debug!("GET: {}", node_id);
		Ok(r)
	}
}
