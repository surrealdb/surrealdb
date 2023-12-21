use crate::err::Error;
use crate::idx::trees::bkeys::BKeys;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeStore};
use crate::idx::VersionedSerdeState;
use crate::kvs::{Key, Transaction, Val};
use crate::sql::{Object, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io::Cursor;
use std::marker::PhantomData;
pub type Payload = u64;

type BStoredNode<BK> = StoredNode<BTreeNode<BK>>;
pub(in crate::idx) type BTreeNodeStore<BK> = TreeNodeStore<BTreeNode<BK>>;

pub struct BTree<BK>
where
	BK: BKeys,
{
	state: BState,
	full_size: u32,
	bk: PhantomData<BK>,
}

#[derive(Clone, Serialize, Deserialize)]
#[revisioned(revision = 1)]
pub struct BState {
	minimum_degree: u32,
	root: Option<NodeId>,
	next_node_id: NodeId,
	#[serde(skip)]
	updated: bool,
}

impl VersionedSerdeState for BState {}

impl BState {
	pub fn new(minimum_degree: u32) -> Self {
		assert!(minimum_degree >= 2, "Minimum degree should be >= 2");
		Self {
			minimum_degree,
			root: None,
			next_node_id: 0,
			updated: false,
		}
	}

	fn set_root(&mut self, node_id: Option<NodeId>) {
		if node_id.ne(&self.root) {
			self.root = node_id;
			self.updated = true;
		}
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.next_node_id;
		self.next_node_id += 1;
		self.updated = true;
		new_node_id
	}

	pub(in crate::idx) async fn finish(
		&self,
		tx: &mut Transaction,
		key: &Key,
	) -> Result<(), Error> {
		if self.updated {
			tx.set(key.clone(), self.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(Debug, Default, PartialEq)]
pub(in crate::idx) struct BStatistics {
	pub(in crate::idx) keys_count: u64,
	pub(in crate::idx) max_depth: u32,
	pub(in crate::idx) nodes_count: u32,
	pub(in crate::idx) total_size: u64,
}

impl From<BStatistics> for Value {
	fn from(stats: BStatistics) -> Self {
		let mut res = Object::default();
		res.insert("keys_count".to_owned(), Value::from(stats.keys_count));
		res.insert("max_depth".to_owned(), Value::from(stats.max_depth));
		res.insert("nodes_count".to_owned(), Value::from(stats.nodes_count));
		res.insert("total_size".to_owned(), Value::from(stats.total_size));
		Value::from(res)
	}
}

#[derive(Debug)]
pub enum BTreeNode<BK>
where
	BK: BKeys,
{
	Internal(BK, Vec<NodeId>),
	Leaf(BK),
}

impl<BK> TreeNode for BTreeNode<BK>
where
	BK: BKeys,
{
	fn try_from_val(val: Val) -> Result<Self, Error> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(val);
		let node_type: u8 = bincode::deserialize_from(&mut c)?;
		let keys = BK::read_from(&mut c)?;
		match node_type {
			1u8 => {
				let child: Vec<NodeId> = bincode::deserialize_from(c)?;
				Ok(BTreeNode::Internal(keys, child))
			}
			2u8 => Ok(BTreeNode::Leaf(keys)),
			_ => Err(Error::CorruptedIndex),
		}
	}

	fn try_into_val(&mut self) -> Result<Val, Error> {
		self.keys_mut().compile();
		let mut c: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		match self {
			BTreeNode::Internal(keys, child) => {
				bincode::serialize_into(&mut c, &1u8)?;
				keys.write_to(&mut c)?;
				bincode::serialize_into(&mut c, &child)?;
			}
			BTreeNode::Leaf(keys) => {
				bincode::serialize_into(&mut c, &2u8)?;
				keys.write_to(&mut c)?;
			}
		};
		Ok(c.into_inner())
	}
}

impl<BK> BTreeNode<BK>
where
	BK: BKeys,
{
	fn keys(&self) -> &BK {
		match self {
			BTreeNode::Internal(keys, _) => keys,
			BTreeNode::Leaf(keys) => keys,
		}
	}

	fn keys_mut(&mut self) -> &mut BK {
		match self {
			BTreeNode::Internal(keys, _) => keys,
			BTreeNode::Leaf(keys) => keys,
		}
	}

	fn append(&mut self, key: Key, payload: Payload, node: BTreeNode<BK>) -> Result<(), Error> {
		match self {
			BTreeNode::Internal(keys, children) => {
				if let BTreeNode::Internal(append_keys, mut append_children) = node {
					keys.insert(key, payload);
					keys.append(append_keys);
					children.append(&mut append_children);
					Ok(())
				} else {
					Err(Error::CorruptedIndex)
				}
			}
			BTreeNode::Leaf(keys) => {
				if let BTreeNode::Leaf(append_keys) = node {
					keys.insert(key, payload);
					keys.append(append_keys);
					Ok(())
				} else {
					Err(Error::CorruptedIndex)
				}
			}
		}
	}
}

struct SplitResult {
	left_node_id: NodeId,
	right_node_id: NodeId,
	median_key: Key,
}

impl<BK> BTree<BK>
where
	BK: BKeys + Debug,
{
	pub fn new(state: BState) -> Self {
		Self {
			full_size: state.minimum_degree * 2 - 1,
			state,
			bk: PhantomData,
		}
	}

	pub async fn search(
		&self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		searched_key: &Key,
	) -> Result<Option<Payload>, Error> {
		let mut next_node = self.state.root;
		while let Some(node_id) = next_node.take() {
			let current = store.get_node(tx, node_id).await?;
			if let Some(payload) = current.n.keys().get(searched_key) {
				store.set_node(current, false)?;
				return Ok(Some(payload));
			}
			if let BTreeNode::Internal(keys, children) = &current.n {
				let child_idx = keys.get_child_idx(searched_key);
				next_node.replace(children[child_idx]);
			}
			store.set_node(current, false)?;
		}
		Ok(None)
	}

	pub async fn insert(
		&mut self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		key: Key,
		payload: Payload,
	) -> Result<(), Error> {
		if let Some(root_id) = self.state.root {
			// We already have a root node
			let root = store.get_node(tx, root_id).await?;
			if root.n.keys().len() == self.full_size {
				// The root node is full, let's split it
				let new_root_id = self.state.new_node_id();
				let new_root = store
					.new_node(new_root_id, BTreeNode::Internal(BK::default(), vec![root_id]))?;
				self.state.set_root(Some(new_root.id));
				self.split_child(store, new_root, 0, root).await?;
				self.insert_non_full(tx, store, new_root_id, key, payload).await?;
			} else {
				// The root node has place, let's insert the value
				let root_id = root.id;
				store.set_node(root, false)?;
				self.insert_non_full(tx, store, root_id, key, payload).await?;
			}
		} else {
			// We don't have a root node, let's create id
			let new_root_id = self.state.new_node_id();
			let new_root_node =
				store.new_node(new_root_id, BTreeNode::Leaf(BK::with_key_val(key, payload)?))?;
			store.set_node(new_root_node, true)?;
			self.state.set_root(Some(new_root_id));
		}
		Ok(())
	}

	async fn insert_non_full(
		&mut self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		node_id: NodeId,
		key: Key,
		payload: Payload,
	) -> Result<(), Error> {
		let mut next_node_id = Some(node_id);
		while let Some(node_id) = next_node_id.take() {
			let mut node = store.get_node(tx, node_id).await?;
			let key: Key = key.clone();
			match &mut node.n {
				BTreeNode::Leaf(keys) => {
					keys.insert(key, payload);
					store.set_node(node, true)?;
				}
				BTreeNode::Internal(keys, children) => {
					if keys.get(&key).is_some() {
						keys.insert(key, payload);
						store.set_node(node, true)?;
						return Ok(());
					}
					let child_idx = keys.get_child_idx(&key);
					let child = store.get_node(tx, children[child_idx]).await?;
					let next_id = if child.n.keys().len() == self.full_size {
						let split_result = self.split_child(store, node, child_idx, child).await?;
						if key.gt(&split_result.median_key) {
							split_result.right_node_id
						} else {
							split_result.left_node_id
						}
					} else {
						let child_id = child.id;
						store.set_node(node, false)?;
						store.set_node(child, false)?;
						child_id
					};
					next_node_id.replace(next_id);
				}
			}
		}
		Ok(())
	}

	async fn split_child(
		&mut self,
		store: &mut BTreeNodeStore<BK>,
		mut parent_node: BStoredNode<BK>,
		idx: usize,
		child_node: BStoredNode<BK>,
	) -> Result<SplitResult, Error> {
		let (left_node, right_node, median_key, median_payload) = match child_node.n {
			BTreeNode::Internal(keys, children) => self.split_internal_node(keys, children)?,
			BTreeNode::Leaf(keys) => self.split_leaf_node(keys)?,
		};
		let right_node_id = self.state.new_node_id();
		match parent_node.n {
			BTreeNode::Internal(ref mut keys, ref mut children) => {
				keys.insert(median_key.clone(), median_payload);
				children.insert(idx + 1, right_node_id);
			}
			BTreeNode::Leaf(ref mut keys) => {
				keys.insert(median_key.clone(), median_payload);
			}
		};
		// Save the mutated split child with half the (lower) keys
		let left_node_id = child_node.id;
		let left_node = store.new_node(left_node_id, left_node)?;
		store.set_node(left_node, true)?;
		// Save the new child with half the (upper) keys
		let right_node = store.new_node(right_node_id, right_node)?;
		store.set_node(right_node, true)?;
		// Save the parent node
		store.set_node(parent_node, true)?;
		Ok(SplitResult {
			left_node_id,
			right_node_id,
			median_key,
		})
	}

	fn split_internal_node(
		&mut self,
		keys: BK,
		mut left_children: Vec<NodeId>,
	) -> Result<(BTreeNode<BK>, BTreeNode<BK>, Key, Payload), Error> {
		let r = keys.split_keys()?;
		let right_children = left_children.split_off(r.median_idx + 1);
		let left_node = BTreeNode::Internal(r.left, left_children);
		let right_node = BTreeNode::Internal(r.right, right_children);
		Ok((left_node, right_node, r.median_key, r.median_payload))
	}

	fn split_leaf_node(
		&mut self,
		keys: BK,
	) -> Result<(BTreeNode<BK>, BTreeNode<BK>, Key, Payload), Error> {
		let r = keys.split_keys()?;
		let left_node = BTreeNode::Leaf(r.left);
		let right_node = BTreeNode::Leaf(r.right);
		Ok((left_node, right_node, r.median_key, r.median_payload))
	}

	pub(in crate::idx) async fn delete(
		&mut self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		key_to_delete: Key,
	) -> Result<Option<Payload>, Error> {
		let mut deleted_payload = None;

		if let Some(root_id) = self.state.root {
			let mut next_node = Some((true, key_to_delete, root_id));

			while let Some((is_main_key, key_to_delete, node_id)) = next_node.take() {
				let mut node = store.get_node(tx, node_id).await?;
				match &mut node.n {
					BTreeNode::Leaf(keys) => {
						// CLRS: 1
						if let Some(payload) = keys.get(&key_to_delete) {
							if is_main_key {
								deleted_payload = Some(payload);
							}
							keys.remove(&key_to_delete);
							if keys.len() == 0 {
								// The node is empty, we can delete it
								store.remove_node(node.id, node.key)?;
								// Check if this was the root node
								if Some(node_id) == self.state.root {
									self.state.set_root(None);
								}
							} else {
								store.set_node(node, true)?;
							}
						} else {
							store.set_node(node, false)?;
						}
					}
					BTreeNode::Internal(keys, children) => {
						// CLRS: 2
						if let Some(payload) = keys.get(&key_to_delete) {
							if is_main_key {
								deleted_payload = Some(payload);
							}
							next_node.replace(
								self.deleted_from_internal(
									tx,
									store,
									keys,
									children,
									key_to_delete,
								)
								.await?,
							);
							store.set_node(node, true)?;
						} else {
							// CLRS: 3
							let (node_update, is_main_key, key_to_delete, next_stored_node) = self
								.deleted_traversal(
									tx,
									store,
									keys,
									children,
									key_to_delete,
									is_main_key,
								)
								.await?;
							if keys.len() == 0 {
								if let Some(root_id) = self.state.root {
									// Delete the old root node
									if root_id != node.id {
										return Err(Error::Unreachable);
									}
								}
								store.remove_node(node_id, node.key)?;
								self.state.set_root(Some(next_stored_node));
							} else if node_update {
								store.set_node(node, true)?;
							} else {
								store.set_node(node, false)?;
							}
							next_node.replace((is_main_key, key_to_delete, next_stored_node));
						}
					}
				}
			}
		}
		Ok(deleted_payload)
	}

	async fn deleted_from_internal(
		&mut self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		key_to_delete: Key,
	) -> Result<(bool, Key, NodeId), Error> {
		let left_idx = keys.get_child_idx(&key_to_delete);
		let left_id = children[left_idx];
		let mut left_node = store.get_node(tx, left_id).await?;
		if left_node.n.keys().len() >= self.state.minimum_degree {
			// CLRS: 2a -> left_node is named `y` in the book
			let (key_prim, payload_prim) = self.find_highest(tx, store, left_node).await?;
			keys.remove(&key_to_delete);
			keys.insert(key_prim.clone(), payload_prim);
			return Ok((false, key_prim, left_id));
		}

		let right_idx = left_idx + 1;
		let right_id = children[right_idx];
		let right_node = store.get_node(tx, right_id).await?;
		if right_node.n.keys().len() >= self.state.minimum_degree {
			store.set_node(left_node, false)?;
			// CLRS: 2b -> right_node is name `z` in the book
			let (key_prim, payload_prim) = self.find_lowest(tx, store, right_node).await?;
			keys.remove(&key_to_delete);
			keys.insert(key_prim.clone(), payload_prim);
			return Ok((false, key_prim, right_id));
		}

		// CLRS: 2c
		// Merge children
		// The payload is set to 0. The value does not matter, as the key will be deleted after anyway.
		left_node.n.append(key_to_delete.clone(), 0, right_node.n)?;
		store.set_node(left_node, true)?;
		store.remove_node(right_id, right_node.key)?;
		keys.remove(&key_to_delete);
		children.remove(right_idx);
		Ok((false, key_to_delete, left_id))
	}

	async fn find_highest(
		&mut self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		node: StoredNode<BTreeNode<BK>>,
	) -> Result<(Key, Payload), Error> {
		let mut next_node = Some(node);
		while let Some(node) = next_node.take() {
			match &node.n {
				BTreeNode::Internal(_, c) => {
					let id = c[c.len() - 1];
					store.set_node(node, false)?;
					let node = store.get_node(tx, id).await?;
					next_node.replace(node);
				}
				BTreeNode::Leaf(k) => {
					let (key, payload) = k.get_last_key().ok_or(Error::Unreachable)?;
					store.set_node(node, false)?;
					return Ok((key, payload));
				}
			}
		}
		Err(Error::Unreachable)
	}

	async fn find_lowest(
		&mut self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		node: StoredNode<BTreeNode<BK>>,
	) -> Result<(Key, Payload), Error> {
		let mut next_node = Some(node);
		while let Some(node) = next_node.take() {
			match &node.n {
				BTreeNode::Internal(_, c) => {
					let id = c[0];
					store.set_node(node, false)?;
					let node = store.get_node(tx, id).await?;
					next_node.replace(node);
				}
				BTreeNode::Leaf(k) => {
					let (key, payload) = k.get_first_key().ok_or(Error::Unreachable)?;
					#[cfg(debug_assertions)]
					debug!("Find lowest: {} - node: {}", String::from_utf8_lossy(&key), node.id);
					store.set_node(node, false)?;
					return Ok((key, payload));
				}
			}
		}
		Err(Error::Unreachable)
	}

	async fn deleted_traversal(
		&mut self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		key_to_delete: Key,
		is_main_key: bool,
	) -> Result<(bool, bool, Key, NodeId), Error> {
		// CLRS 3a
		let child_idx = keys.get_child_idx(&key_to_delete);
		let child_id = children[child_idx];
		let child_stored_node = store.get_node(tx, child_id).await?;
		if child_stored_node.n.keys().len() < self.state.minimum_degree {
			// right child (successor)
			if child_idx < children.len() - 1 {
				let right_child_stored_node = store.get_node(tx, children[child_idx + 1]).await?;
				return if right_child_stored_node.n.keys().len() >= self.state.minimum_degree {
					Self::delete_adjust_successor(
						store,
						keys,
						child_idx,
						key_to_delete,
						is_main_key,
						child_stored_node,
						right_child_stored_node,
					)
					.await
				} else {
					// CLRS 3b successor
					Self::merge_nodes(
						store,
						keys,
						children,
						child_idx,
						key_to_delete,
						is_main_key,
						child_stored_node,
						right_child_stored_node,
					)
					.await
				};
			}

			// left child (predecessor)
			if child_idx > 0 {
				let child_idx = child_idx - 1;
				let left_child_stored_node = store.get_node(tx, children[child_idx]).await?;
				return if left_child_stored_node.n.keys().len() >= self.state.minimum_degree {
					Self::delete_adjust_predecessor(
						store,
						keys,
						child_idx,
						key_to_delete,
						is_main_key,
						child_stored_node,
						left_child_stored_node,
					)
					.await
				} else {
					// CLRS 3b predecessor
					Self::merge_nodes(
						store,
						keys,
						children,
						child_idx,
						key_to_delete,
						is_main_key,
						left_child_stored_node,
						child_stored_node,
					)
					.await
				};
			}
		}

		store.set_node(child_stored_node, false)?;
		Ok((false, true, key_to_delete, child_id))
	}

	async fn delete_adjust_successor(
		store: &mut BTreeNodeStore<BK>,
		keys: &mut BK,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut child_stored_node: BStoredNode<BK>,
		mut right_child_stored_node: BStoredNode<BK>,
	) -> Result<(bool, bool, Key, NodeId), Error> {
		if let Some((ascending_key, ascending_payload)) =
			right_child_stored_node.n.keys().get_first_key()
		{
			right_child_stored_node.n.keys_mut().remove(&ascending_key);
			if let Some(descending_key) = keys.get_key(child_idx) {
				if let Some(descending_payload) = keys.remove(&descending_key) {
					child_stored_node.n.keys_mut().insert(descending_key, descending_payload);
					if let BTreeNode::Internal(_, rc) = &mut right_child_stored_node.n {
						if let BTreeNode::Internal(_, lc) = &mut child_stored_node.n {
							lc.push(rc.remove(0))
						}
					}
					keys.insert(ascending_key, ascending_payload);
					let child_id = child_stored_node.id;
					store.set_node(child_stored_node, true)?;
					store.set_node(right_child_stored_node, true)?;
					return Ok((true, is_main_key, key_to_delete, child_id));
				}
			}
		}
		// If we reach this point, something was wrong in the BTree
		Err(Error::CorruptedIndex)
	}

	async fn delete_adjust_predecessor(
		store: &mut BTreeNodeStore<BK>,
		keys: &mut BK,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut child_stored_node: BStoredNode<BK>,
		mut left_child_stored_node: BStoredNode<BK>,
	) -> Result<(bool, bool, Key, NodeId), Error> {
		if let Some((ascending_key, ascending_payload)) =
			left_child_stored_node.n.keys().get_last_key()
		{
			left_child_stored_node.n.keys_mut().remove(&ascending_key);
			if let Some(descending_key) = keys.get_key(child_idx) {
				if let Some(descending_payload) = keys.remove(&descending_key) {
					child_stored_node.n.keys_mut().insert(descending_key, descending_payload);
					if let BTreeNode::Internal(_, lc) = &mut left_child_stored_node.n {
						if let BTreeNode::Internal(_, rc) = &mut child_stored_node.n {
							rc.insert(0, lc.remove(lc.len() - 1));
						}
					}
					keys.insert(ascending_key, ascending_payload);
					let child_id = child_stored_node.id;
					store.set_node(child_stored_node, true)?;
					store.set_node(left_child_stored_node, true)?;
					return Ok((true, is_main_key, key_to_delete, child_id));
				}
			}
		}
		// If we reach this point, something was wrong in the BTree
		Err(Error::CorruptedIndex)
	}

	#[allow(clippy::too_many_arguments)]
	async fn merge_nodes(
		store: &mut BTreeNodeStore<BK>,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut left_child: BStoredNode<BK>,
		right_child: BStoredNode<BK>,
	) -> Result<(bool, bool, Key, NodeId), Error> {
		if let Some(descending_key) = keys.get_key(child_idx) {
			if let Some(descending_payload) = keys.remove(&descending_key) {
				children.remove(child_idx + 1);
				let left_id = left_child.id;
				left_child.n.append(descending_key, descending_payload, right_child.n)?;
				store.set_node(left_child, true)?;
				store.remove_node(right_child.id, right_child.key)?;
				return Ok((true, is_main_key, key_to_delete, left_id));
			}
		}
		// If we reach this point, something was wrong in the BTree
		Err(Error::CorruptedIndex)
	}

	pub(in crate::idx) async fn statistics(
		&self,
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
	) -> Result<BStatistics, Error> {
		let mut stats = BStatistics::default();
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, 1));
		}
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let stored = store.get_node(tx, node_id).await?;
			stats.keys_count += stored.n.keys().len() as u64;
			if depth > stats.max_depth {
				stats.max_depth = depth;
			}
			stats.nodes_count += 1;
			stats.total_size += stored.size as u64;
			if let BTreeNode::Internal(_, children) = &stored.n {
				let depth = depth + 1;
				for child_id in children.iter() {
					node_queue.push_front((*child_id, depth));
				}
			};
			store.set_node(stored, false)?;
		}
		Ok(stats)
	}

	pub(in crate::idx) fn get_state(&self) -> &BState {
		&self.state
	}
}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::trees::bkeys::{BKeys, FstKeys, TrieKeys};
	use crate::idx::trees::btree::{
		BState, BStatistics, BStoredNode, BTree, BTreeNode, BTreeNodeStore, Payload,
	};
	use crate::idx::trees::store::{
		NodeId, TreeNode, TreeNodeProvider, TreeNodeStore, TreeStoreType,
	};
	use crate::idx::VersionedSerdeState;
	use crate::kvs::TransactionType::*;
	use crate::kvs::{Datastore, Key, LockType::*, Transaction};
	use rand::prelude::SliceRandom;
	use rand::thread_rng;
	use std::collections::{BTreeMap, HashMap, VecDeque};
	use std::fmt::Debug;
	use std::sync::Arc;
	use test_log::test;
	use tokio::sync::Mutex;

	#[test]
	fn test_btree_state_serde() {
		let s = BState::new(3);
		let val = s.try_to_val().unwrap();
		let s: BState = BState::try_from_val(val).unwrap();
		assert_eq!(s.minimum_degree, 3);
		assert_eq!(s.root, None);
		assert_eq!(s.next_node_id, 0);
	}

	#[test]
	fn test_node_serde_internal() {
		let mut node = BTreeNode::Internal(FstKeys::default(), vec![]);
		node.keys_mut().compile();
		let val = node.try_into_val().unwrap();
		let _: BTreeNode<FstKeys> = BTreeNode::try_from_val(val).unwrap();
	}

	#[test]
	fn test_node_serde_leaf() {
		let mut node = BTreeNode::Leaf(TrieKeys::default());
		let val = node.try_into_val().unwrap();
		let _: BTreeNode<TrieKeys> = BTreeNode::try_from_val(val).unwrap();
	}

	async fn insertions_test<F, BK>(
		tx: &mut Transaction,
		store: &mut BTreeNodeStore<BK>,
		t: &mut BTree<BK>,
		samples_size: usize,
		sample_provider: F,
	) where
		F: Fn(usize) -> (Key, Payload),
		BK: BKeys + Debug,
	{
		for i in 0..samples_size {
			let (key, payload) = sample_provider(i);
			// Insert the sample
			t.insert(tx, store, key.clone(), payload).await.unwrap();
			// Check we can find it
			assert_eq!(t.search(tx, store, &key).await.unwrap(), Some(payload));
		}
	}

	fn get_key_value(idx: usize) -> (Key, Payload) {
		(format!("{}", idx).into(), (idx * 10) as Payload)
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_sequential_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let mut t = BTree::new(BState::new(5));
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		insertions_test::<_, FstKeys>(&mut tx, &mut s, &mut t, 100, get_key_value).await;
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		assert_eq!(
			t.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
				.await
				.unwrap(),
			BStatistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 22,
				total_size: 1691,
			}
		);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_sequential_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let mut t = BTree::new(BState::new(6));
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		insertions_test::<_, TrieKeys>(&mut tx, &mut s, &mut t, 100, get_key_value).await;
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		assert_eq!(
			t.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
				.await
				.unwrap(),
			BStatistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 18,
				total_size: 1656,
			}
		);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_random_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let mut t = BTree::new(BState::new(8));
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, FstKeys>(&mut tx, &mut s, &mut t, 100, |i| get_key_value(samples[i]))
			.await;
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		let s = t
			.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
			.await
			.unwrap();
		assert_eq!(s.keys_count, 100);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_random_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let mut t = BTree::new(BState::new(75));
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, TrieKeys>(&mut tx, &mut s, &mut t, 100, |i| get_key_value(samples[i]))
			.await;
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		let s = t
			.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
			.await
			.unwrap();
		assert_eq!(s.keys_count, 100);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_large_order_sequential_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let mut t = BTree::new(BState::new(60));
		insertions_test::<_, FstKeys>(&mut tx, &mut s, &mut t, 10000, get_key_value).await;
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		assert_eq!(
			t.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
				.await
				.unwrap(),
			BStatistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 158,
				total_size: 57486,
			}
		);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_large_order_sequential_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let mut t = BTree::new(BState::new(60));
		insertions_test::<_, TrieKeys>(&mut tx, &mut s, &mut t, 10000, get_key_value).await;
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		assert_eq!(
			t.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
				.await
				.unwrap(),
			BStatistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 158,
				total_size: 75206,
			}
		);
		tx.cancel().await.unwrap();
	}

	const REAL_WORLD_TERMS: [&str; 30] = [
		"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "the", "fast",
		"fox", "jumped", "over", "the", "lazy", "dog", "the", "dog", "sat", "there", "and", "did",
		"nothing", "the", "other", "animals", "sat", "there", "watching",
	];

	async fn test_btree_read_world_insertions<BK>(default_minimum_degree: u32) -> BStatistics
	where
		BK: BKeys + Debug,
	{
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let mut t = BTree::new(BState::new(default_minimum_degree));
		insertions_test::<_, BK>(&mut tx, &mut s, &mut t, REAL_WORLD_TERMS.len(), |i| {
			(REAL_WORLD_TERMS[i].as_bytes().to_vec(), i as Payload)
		})
		.await;
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		let statistics = t
			.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
			.await
			.unwrap();
		tx.cancel().await.unwrap();
		statistics
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(4).await;
		assert_eq!(
			s,
			BStatistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 5,
				total_size: 421,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(100).await;
		assert_eq!(
			s,
			BStatistics {
				keys_count: 17,
				max_depth: 1,
				nodes_count: 1,
				total_size: 189,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(6).await;
		assert_eq!(
			s,
			BStatistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 3,
				total_size: 339,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(100).await;
		assert_eq!(
			s,
			BStatistics {
				keys_count: 17,
				max_depth: 1,
				nodes_count: 1,
				total_size: 229,
			}
		);
	}

	// This is the examples from the chapter B-Trees in CLRS:
	// https://en.wikipedia.org/wiki/Introduction_to_Algorithms
	const CLRS_EXAMPLE: [(&str, Payload); 23] = [
		("a", 1),
		("c", 3),
		("g", 7),
		("j", 10),
		("k", 11),
		("m", 13),
		("n", 14),
		("o", 15),
		("p", 16),
		("t", 20),
		("u", 21),
		("x", 24),
		("y", 25),
		("z", 26),
		("v", 22),
		("d", 4),
		("e", 5),
		("r", 18),
		("s", 19), // (a) Initial tree
		("b", 2),  // (b) B inserted
		("q", 17), // (c) Q inserted
		("l", 12), // (d) L inserted
		("f", 6),  // (e) F inserted
	];

	#[test(tokio::test)]
	// This check node splitting. CLRS: Figure 18.7, page 498.
	async fn clrs_insertion_test() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::<TrieKeys>::new(BState::new(3));
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		for (key, payload) in CLRS_EXAMPLE {
			t.insert(&mut tx, &mut s, key.into(), payload).await.unwrap();
		}
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		let s = t
			.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
			.await
			.unwrap();
		assert_eq!(s.keys_count, 23);
		assert_eq!(s.max_depth, 3);
		assert_eq!(s.nodes_count, 10);
		// There should be one record per node
		assert_eq!(10, tx.scan(vec![]..vec![0xf], 100).await.unwrap().len());

		let nodes_count = t
			.inspect_nodes(&mut tx, |count, depth, node_id, node| match count {
				0 => {
					assert_eq!(depth, 1);
					assert_eq!(node_id, 7);
					check_is_internal_node(node.n, vec![("p", 16)], vec![1, 8]);
				}
				1 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 1);
					check_is_internal_node(
						node.n,
						vec![("c", 3), ("g", 7), ("m", 13)],
						vec![0, 9, 2, 3],
					);
				}
				2 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 8);
					check_is_internal_node(node.n, vec![("t", 20), ("x", 24)], vec![4, 6, 5]);
				}
				3 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 0);
					check_is_leaf_node(node.n, vec![("a", 1), ("b", 2)]);
				}
				4 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 9);
					check_is_leaf_node(node.n, vec![("d", 4), ("e", 5), ("f", 6)]);
				}
				5 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 2);
					check_is_leaf_node(node.n, vec![("j", 10), ("k", 11), ("l", 12)]);
				}
				6 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 3);
					check_is_leaf_node(node.n, vec![("n", 14), ("o", 15)]);
				}
				7 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 4);
					check_is_leaf_node(node.n, vec![("q", 17), ("r", 18), ("s", 19)]);
				}
				8 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 6);
					check_is_leaf_node(node.n, vec![("u", 21), ("v", 22)]);
				}
				9 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 5);
					check_is_leaf_node(node.n, vec![("y", 25), ("z", 26)]);
				}
				_ => panic!("This node should not exist {}", count),
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 10);
		tx.cancel().await.unwrap();
	}

	// This check the possible deletion cases. CRLS, Figure 18.8, pages 500-501
	async fn test_btree_clrs_deletion_test<BK>(mut t: BTree<BK>)
	where
		BK: BKeys + Debug,
	{
		let ds = Datastore::new("memory").await.unwrap();

		{
			let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
			let mut s = s.lock().await;
			let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
			for (key, payload) in CLRS_EXAMPLE {
				t.insert(&mut tx, &mut s, key.into(), payload).await.unwrap();
			}
			s.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			for (key, payload) in [("f", 6), ("m", 13), ("g", 7), ("d", 4), ("b", 2)] {
				let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
				let mut s = s.lock().await;
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				debug!("Delete {}", key);
				assert_eq!(t.delete(&mut tx, &mut s, key.into()).await.unwrap(), Some(payload));
				s.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}
		}

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		let s = t
			.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
			.await
			.unwrap();
		assert_eq!(s.keys_count, 18);
		assert_eq!(s.max_depth, 2);
		assert_eq!(s.nodes_count, 7);
		// There should be one record per node
		assert_eq!(7, tx.scan(vec![]..vec![0xf], 100).await.unwrap().len());

		let nodes_count = t
			.inspect_nodes(&mut tx, |count, depth, node_id, node| {
				debug!("{} -> {}", depth, node_id);
				node.n.debug(|k| Ok(String::from_utf8(k)?)).unwrap();
				match count {
					0 => {
						assert_eq!(depth, 1);
						assert_eq!(node_id, 1);
						check_is_internal_node(
							node.n,
							vec![("e", 5), ("l", 12), ("p", 16), ("t", 20), ("x", 24)],
							vec![0, 9, 3, 4, 6, 5],
						);
					}
					1 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 0);
						check_is_leaf_node(node.n, vec![("a", 1), ("c", 3)]);
					}
					2 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 9);
						check_is_leaf_node(node.n, vec![("j", 10), ("k", 11)]);
					}
					3 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 3);
						check_is_leaf_node(node.n, vec![("n", 14), ("o", 15)]);
					}
					4 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 4);
						check_is_leaf_node(node.n, vec![("q", 17), ("r", 18), ("s", 19)]);
					}
					5 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 6);
						check_is_leaf_node(node.n, vec![("u", 21), ("v", 22)]);
					}
					6 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 5);
						check_is_leaf_node(node.n, vec![("y", 25), ("z", 26)]);
					}
					_ => panic!("This node should not exist {}", count),
				}
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 7);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_clrs_deletion_test() {
		let t = BTree::<TrieKeys>::new(BState::new(3));
		test_btree_clrs_deletion_test(t).await
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_clrs_deletion_test() {
		let t = BTree::<FstKeys>::new(BState::new(3));
		test_btree_clrs_deletion_test(t).await
	}

	// This check the possible deletion cases. CRLS, Figure 18.8, pages 500-501
	async fn test_btree_fill_and_empty<BK>(mut t: BTree<BK>)
	where
		BK: BKeys + Debug,
	{
		let ds = Datastore::new("memory").await.unwrap();

		let mut expected_keys = HashMap::new();

		{
			let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
			let mut s = s.lock().await;
			let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
			for (key, payload) in CLRS_EXAMPLE {
				expected_keys.insert(key.to_string(), payload);
				t.insert(&mut tx, &mut s, key.into(), payload).await.unwrap();
			}
			s.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
			print_tree(&mut tx, &t).await;
			tx.commit().await.unwrap();
		}

		for (key, _) in CLRS_EXAMPLE {
			debug!("------------------------");
			debug!("Delete {}", key);
			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
				let mut s = s.lock().await;
				t.delete(&mut tx, &mut s, key.into()).await.unwrap();
				print_tree::<BK>(&mut tx, &t).await;
				s.finish(&mut tx).await.unwrap();
				tx.commit().await.unwrap();
			}

			// Check that every expected keys are still found in the tree
			expected_keys.remove(key);

			{
				let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
				let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Read, 20);
				let mut s = s.lock().await;
				for (key, payload) in &expected_keys {
					assert_eq!(
						t.search(&mut tx, &mut s, &key.as_str().into()).await.unwrap(),
						Some(*payload)
					)
				}
				tx.commit().await.unwrap();
			}
		}

		let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
		let s = t
			.statistics(&mut tx, &mut TreeNodeStore::Traversal(TreeNodeProvider::Debug))
			.await
			.unwrap();
		assert_eq!(s.keys_count, 0);
		assert_eq!(s.max_depth, 0);
		assert_eq!(s.nodes_count, 0);
		// There should not be any record in the database
		assert_eq!(0, tx.scan(vec![]..vec![0xf], 100).await.unwrap().len());
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_fill_and_empty() {
		let t = BTree::<TrieKeys>::new(BState::new(3));
		test_btree_fill_and_empty(t).await
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_fill_and_empty() {
		let t = BTree::<FstKeys>::new(BState::new(3));
		test_btree_fill_and_empty(t).await
	}

	async fn new_operation<BK>(
		ds: &Datastore,
		tt: TreeStoreType,
		cache_size: usize,
	) -> (Transaction, Arc<Mutex<BTreeNodeStore<BK>>>)
	where
		BK: BKeys + Debug,
	{
		let tx_type = match tt {
			TreeStoreType::Write => Write,
			TreeStoreType::Read => Read,
			TreeStoreType::Traversal => Read,
		};
		let tx = ds.transaction(tx_type, Optimistic).await.unwrap();
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, tt, cache_size);
		(tx, s)
	}

	#[test(tokio::test)]
	async fn test_delete_adjust() -> Result<(), Error> {
		let ds = Datastore::new("memory").await?;
		let mut t = BTree::<FstKeys>::new(BState::new(3));

		let terms = [
			"aliquam",
			"delete",
			"if",
			"from",
			"Docusaurus",
			"amet,",
			"don't",
			"And",
			"interactive",
			"well!",
			"supports",
			"ultricies.",
			"Fusce",
			"consequat.",
			"just",
			"use",
			"elementum",
			"term",
			"blogging",
			"to",
			"want",
			"added",
			"Lorem",
			"ipsum",
			"blog:",
			"MDX.",
			"posts.",
			"features",
			"posts",
			"features,",
			"truncate",
			"images:",
			"Long",
			"Pellentesque",
			"authors.yml.",
			"filenames,",
			"such",
			"co-locate",
			"you",
			"can",
			"the",
			"-->",
			"comment",
			"tags",
			"A",
			"React",
			"The",
			"adipiscing",
			"consectetur",
			"very",
			"this",
			"and",
			"sit",
			"directory,",
			"Regular",
			"Markdown",
			"Simply",
			"blog",
			"MDX",
			"list",
			"extracted",
			"summary",
			"amet",
			"plugin.",
			"your",
			"long",
			"First",
			"power",
			"post,",
			"convenient",
			"folders)",
			"of",
			"date",
			"powered",
			"2019-05-30-welcome.md",
			"view.",
			"are",
			"be",
			"<!--",
			"Welcome",
			"is",
			"2019-05-30-welcome/index.md",
			"by",
			"directory.",
			"folder",
			"Use",
			"search",
			"authors",
			"false",
			"as:",
			"tempor",
			"files",
			"config.",
			"dignissim",
			"as",
			"a",
			"in",
			"This",
			"authors.yml",
			"create",
			"dolor",
			"Enter",
			"support",
			"add",
			"eros",
			"post",
			"Post",
			"size",
			"(or",
			"rhoncus",
			"Blog",
			"limit",
			"elit.",
		];
		let mut keys = BTreeMap::new();
		{
			for term in terms {
				{
					let (mut tx, st) = new_operation(&ds, TreeStoreType::Write, 100).await;
					let mut st = st.lock().await;
					t.insert(&mut tx, &mut st, term.into(), 0).await?;
					st.finish(&mut tx).await?;
					tx.commit().await?;
				}
				keys.insert(term.to_string(), 0);
				{
					let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
					let (_, tree_keys) = check_btree_properties(&t, &mut tx).await?;
					assert_eq!(keys, tree_keys);
					tx.cancel().await?;
				}
			}
		}
		{
			for term in terms {
				debug!("Delete {term}");
				{
					let (mut tx, st) = new_operation(&ds, TreeStoreType::Write, 100).await;
					let mut st = st.lock().await;
					t.delete(&mut tx, &mut st, term.into()).await?;
					st.finish(&mut tx).await?;
					tx.commit().await?;
				}
				keys.remove(term);
				{
					let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
					let (_, tree_keys) = check_btree_properties(&t, &mut tx).await?;
					assert_eq!(keys, tree_keys);
					tx.cancel().await?;
				}
			}
		}
		{
			let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
			assert_eq!(check_btree_properties(&t, &mut tx).await?.0, 0);
			tx.cancel().await?;
		}
		Ok(())
	}

	async fn check_btree_properties<BK>(
		t: &BTree<BK>,
		tx: &mut Transaction,
	) -> Result<(usize, BTreeMap<String, Payload>), Error>
	where
		BK: BKeys + Debug,
	{
		let mut unique_keys = BTreeMap::new();
		let n = t
			.inspect_nodes(tx, |_, _, _, sn| {
				let keys = sn.n.keys();
				for i in 0..keys.len() {
					let key = keys.get_key(i as usize).unwrap_or_else(|| panic!("No key"));
					let payload = keys.get(&key).unwrap_or_else(|| panic!("No payload"));
					if unique_keys.insert(String::from_utf8(key).unwrap(), payload).is_some() {
						panic!("Non unique");
					}
				}
			})
			.await?;
		Ok((n, unique_keys))
	}

	/////////////
	// HELPERS //
	/////////////

	fn check_is_internal_node<BK>(
		node: BTreeNode<BK>,
		expected_keys: Vec<(&str, i32)>,
		expected_children: Vec<NodeId>,
	) where
		BK: BKeys,
	{
		if let BTreeNode::Internal(keys, children) = node {
			check_keys(keys, expected_keys);
			assert_eq!(children, expected_children, "The children are not matching");
		} else {
			panic!("An internal node was expected, we got a leaf node");
		}
	}

	fn check_is_leaf_node<BK>(node: BTreeNode<BK>, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys,
	{
		if let BTreeNode::Leaf(keys) = node {
			check_keys(keys, expected_keys);
		} else {
			panic!("An internal node was expected, we got a leaf node");
		}
	}

	async fn print_tree<BK>(tx: &mut Transaction, t: &BTree<BK>)
	where
		BK: BKeys + Debug,
	{
		debug!("----------------------------------");
		t.inspect_nodes(tx, |_count, depth, node_id, node| {
			debug!("{} -> {}", depth, node_id);
			node.n.debug(|k| Ok(String::from_utf8(k)?)).unwrap();
		})
		.await
		.unwrap();
		debug!("----------------------------------");
	}

	fn check_keys<BK>(keys: BK, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys,
	{
		assert_eq!(keys.len() as usize, expected_keys.len(), "The number of keys does not match");
		for (key, payload) in expected_keys {
			assert_eq!(
				keys.get(&key.into()),
				Some(payload as Payload),
				"The key {} does not match",
				key
			);
		}
	}

	impl<BK> BTree<BK>
	where
		BK: BKeys + Debug,
	{
		/// This is for debugging
		async fn inspect_nodes<F>(
			&self,
			tx: &mut Transaction,
			mut inspect_func: F,
		) -> Result<usize, Error>
		where
			F: FnMut(usize, usize, NodeId, BStoredNode<BK>),
		{
			let mut node_queue = VecDeque::new();
			if let Some(node_id) = self.state.root {
				node_queue.push_front((node_id, 1));
			}
			let mut count = 0;
			let mut s = TreeNodeStore::Traversal(TreeNodeProvider::Debug);
			while let Some((node_id, depth)) = node_queue.pop_front() {
				let stored_node = s.get_node(tx, node_id).await?;
				if let BTreeNode::Internal(_, children) = &stored_node.n {
					let depth = depth + 1;
					for child_id in children {
						node_queue.push_back((*child_id, depth));
					}
				}
				inspect_func(count, depth, node_id, stored_node);
				count += 1;
			}
			Ok(count)
		}
	}

	impl<BK> BTreeNode<BK>
	where
		BK: BKeys,
	{
		fn debug<F>(&self, to_string: F) -> Result<(), Error>
		where
			F: Fn(Key) -> Result<String, Error>,
		{
			match self {
				BTreeNode::Internal(keys, children) => {
					keys.debug(to_string)?;
					debug!("Children{:?}", children);
					Ok(())
				}
				BTreeNode::Leaf(keys) => keys.debug(to_string),
			}
		}
	}
}
