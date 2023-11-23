use crate::err::Error;
use crate::idx::trees::store::memory::TreeMemoryMap;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLockWriteGuard;

pub(super) struct TreeTransactionWrite<N>
where
	N: TreeNode + Debug,
{
	np: TreeNodeProvider,
	nodes: HashMap<NodeId, Arc<StoredNode<N>>>,
	updated: HashSet<NodeId>,
	removed: HashMap<NodeId, Key>,
	#[cfg(debug_assertions)]
	out: HashSet<NodeId>,
}

impl<N: Debug> TreeTransactionWrite<N>
where
	N: TreeNode,
{
	pub(super) fn new(keys: TreeNodeProvider) -> Self {
		Self {
			np: keys,
			nodes: HashMap::new(),
			updated: HashSet::new(),
			removed: HashMap::new(),
			#[cfg(debug_assertions)]
			out: HashSet::new(),
		}
	}

	pub(super) async fn get_node(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		#[cfg(debug_assertions)]
		{
			debug!("GET: {}", node_id);
			self.out.insert(node_id);
		}
		if let Some(n) = self.nodes.remove(&node_id) {
			return Ok(n);
		}
		Ok(Arc::new(self.np.load::<N>(tx, node_id).await?))
	}

	pub(super) fn set_node(
		&mut self,
		node: Arc<StoredNode<N>>,
		updated: bool,
	) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("SET: {} {} {:?}", node.id, updated, node.n);
			self.out.remove(&node.id);
		}
		if updated {
			self.updated.insert(node.id);
		}
		if self.removed.contains_key(&node.id) {
			return Err(Error::Unreachable);
		}
		self.nodes.insert(node.id, node);
		Ok(())
	}

	pub(super) fn new_node(&mut self, id: NodeId, node: N) -> Arc<StoredNode<N>> {
		#[cfg(debug_assertions)]
		{
			debug!("NEW: {}", id);
			self.out.insert(id);
		}
		Arc::new(StoredNode::new(node, id, self.np.get_key(id), 0))
	}

	pub(super) fn remove_node(&mut self, node_id: NodeId, node_key: Key) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("REMOVE: {}", node_id);
			if self.nodes.contains_key(&node_id) {
				return Err(Error::Unreachable);
			}
			self.out.remove(&node_id);
		}
		self.updated.remove(&node_id);
		self.removed.insert(node_id, node_key);
		Ok(())
	}

	pub(super) async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		let update = !self.updated.is_empty() || !self.removed.is_empty();
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				debug!("OUT: {:?}", self.out);
				return Err(Error::Unreachable);
			}
		}
		for node_id in &self.updated {
			if let Some(node) = self.nodes.remove(node_id) {
				self.np.save(tx, Arc::try_unwrap(node)?).await?;
			} else {
				return Err(Error::Unreachable);
			}
		}
		self.updated.clear();
		let node_ids: Vec<NodeId> = self.removed.keys().copied().collect();
		for node_id in node_ids {
			if let Some(node_key) = self.removed.remove(&node_id) {
				tx.del(node_key).await?;
			}
		}
		Ok(update)
	}
}

pub(super) struct TreeMemoryWrite {
	#[cfg(debug_assertions)]
	out: HashSet<NodeId>,
}

impl TreeMemoryWrite {
	pub(super) fn new() -> Self {
		Self {
			#[cfg(debug_assertions)]
			out: HashSet::new(),
		}
	}

	pub(super) fn get_node<N>(
		&mut self,
		nodes: &mut RwLockWriteGuard<TreeMemoryMap<N>>,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error>
	where
		N: TreeNode + Debug,
	{
		#[cfg(debug_assertions)]
		{
			debug!("GET: {}", node_id);
			self.out.insert(node_id);
		}
		if let Some(n) = nodes.remove(&node_id) {
			Ok(n)
		} else {
			Err(Error::Unreachable)
		}
	}

	pub(super) fn set_node<N>(
		&mut self,
		nodes: &mut TreeMemoryMap<N>,
		node: Arc<StoredNode<N>>,
	) -> Result<(), Error>
	where
		N: TreeNode + Debug,
	{
		#[cfg(debug_assertions)]
		{
			debug!("SET: {} {:?}", node.id, node.n);
			self.out.remove(&node.id);
		}
		nodes.lock().insert(node.id, node);
		Ok(())
	}

	pub(super) fn new_node<N>(&mut self, id: NodeId, node: N) -> Arc<StoredNode<N>>
	where
		N: TreeNode + Debug,
	{
		#[cfg(debug_assertions)]
		{
			debug!("NEW: {}", id);
			self.out.insert(id);
		}
		Arc::new(StoredNode::new(node, id, vec![], 0))
	}

	pub(super) fn remove_node<N>(
		&mut self,
		nodes: &mut TreeMemoryMap<N>,
		node_id: NodeId,
	) -> Result<(), Error>
	where
		N: TreeNode + Debug,
	{
		#[cfg(debug_assertions)]
		{
			debug!("REMOVE: {}", node_id);
			if nodes.contains_key(&node_id) {
				return Err(Error::Unreachable);
			}
			self.out.remove(&node_id);
		}
		nodes.remove(&node_id);
		Ok(())
	}

	pub(super) fn finish(&mut self) -> Result<bool, Error> {
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				debug!("OUT: {:?}", self.out);
				return Err(Error::Unreachable);
			}
		}
		Ok(true)
	}
}
