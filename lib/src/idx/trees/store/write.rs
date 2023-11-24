use crate::err::Error;
use crate::idx::trees::store::memory::TreeMemoryMap;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLockWriteGuard;

pub struct TreeTransactionWrite<N>
where
	N: TreeNode + Debug,
{
	np: TreeNodeProvider,
	nodes: HashMap<NodeId, StoredNode<N>>,
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

	pub(super) async fn get_node_mut(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		#[cfg(debug_assertions)]
		{
			debug!("GET: {}", node_id);
			self.out.insert(node_id);
		}
		if let Some(n) = self.nodes.remove(&node_id) {
			return Ok(n);
		}
		self.np.load::<N>(tx, node_id).await
	}

	pub(super) fn set_node(&mut self, node: StoredNode<N>, updated: bool) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("SET: {} {} {:?}", node.id, updated, node.n);
			self.out.remove(&node.id);
		}
		if updated {
			self.updated.insert(node.id);
		}
		if self.removed.contains_key(&node.id) {
			return Err(Error::Unreachable("TreeTransactionWrite::set_node"));
		}
		self.nodes.insert(node.id, node);
		Ok(())
	}

	pub(super) fn new_node(&mut self, id: NodeId, node: N) -> StoredNode<N> {
		#[cfg(debug_assertions)]
		{
			debug!("NEW: {}", id);
			self.out.insert(id);
		}
		StoredNode::new(node, id, self.np.get_key(id), 0)
	}

	pub(super) fn remove_node(&mut self, node_id: NodeId, node_key: Key) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("REMOVE: {}", node_id);
			if self.nodes.contains_key(&node_id) {
				return Err(Error::Unreachable("TreeTransactionWrite::remove_node"));
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
				return Err(Error::Unreachable("TreeTransactionWrite::finish(1)"));
			}
		}
		for node_id in &self.updated {
			if let Some(node) = self.nodes.remove(node_id) {
				self.np.save(tx, node).await?;
			} else {
				return Err(Error::Unreachable("TreeTransactionWrite::finish(2)"));
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

pub struct TreeMemoryWrite {
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

	pub(super) fn get_node_mut<N>(
		&mut self,
		nodes: &mut Option<RwLockWriteGuard<'_, TreeMemoryMap<N>>>,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error>
	where
		N: TreeNode + Debug,
	{
		if let Some(ref mut nodes) = nodes {
			#[cfg(debug_assertions)]
			{
				debug!("GET: {}", node_id);
				self.out.insert(node_id);
			}
			if let Some(n) = nodes.remove(&node_id) {
				let n =
					Arc::into_inner(n).ok_or(Error::Unreachable("TreeMemoryWrite::get_node(1)"))?;
				Ok(n)
			} else {
				Err(Error::Unreachable("TreeMemoryWrite::get_node(2)"))
			}
		} else {
			Err(Error::Unreachable("TreeMemoryWrite::get_node(3)"))
		}
	}

	pub(super) fn set_node<N>(
		&mut self,
		nodes: &mut Option<RwLockWriteGuard<'_, TreeMemoryMap<N>>>,
		node: StoredNode<N>,
	) -> Result<(), Error>
	where
		N: TreeNode + Debug,
	{
		if let Some(ref mut nodes) = nodes {
			#[cfg(debug_assertions)]
			{
				debug!("SET: {} {:?}", node.id, node.n);
				self.out.remove(&node.id);
			}
			nodes.insert(node.id, Arc::new(node));
			Ok(())
		} else {
			Err(Error::Unreachable("TreeMemoryWrite::set_node"))
		}
	}

	pub(super) fn new_node<N>(&mut self, id: NodeId, node: N) -> StoredNode<N>
	where
		N: TreeNode + Debug,
	{
		#[cfg(debug_assertions)]
		{
			debug!("NEW: {}", id);
			self.out.insert(id);
		}
		StoredNode::new(node, id, vec![], 0)
	}

	pub(super) fn remove_node<N>(
		&mut self,
		nodes: &mut Option<RwLockWriteGuard<'_, TreeMemoryMap<N>>>,
		node_id: NodeId,
	) -> Result<(), Error>
	where
		N: TreeNode + Debug,
	{
		if let Some(ref mut nodes) = nodes {
			#[cfg(debug_assertions)]
			{
				debug!("REMOVE: {}", node_id);
				if nodes.contains_key(&node_id) {
					return Err(Error::Unreachable("TreeMemoryWrite::remove_node(1)"));
				}
				self.out.remove(&node_id);
			}
			nodes.remove(&node_id);
			Ok(())
		} else {
			Err(Error::Unreachable("TreeMemoryWrite::remove_node(2)"))
		}
	}

	pub(super) fn finish(&mut self) -> Result<bool, Error> {
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				debug!("OUT: {:?}", self.out);
				return Err(Error::Unreachable("TreeMemoryWrite::finish"));
			}
		}
		Ok(true)
	}
}
