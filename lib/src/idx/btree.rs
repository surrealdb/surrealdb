use crate::err::Error;
use crate::idx::bkeys::BKeys;
use crate::idx::SerdeState;
use crate::kvs::{Key, Transaction};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) type NodeId = u64;
pub(super) type Payload = u64;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub(super) trait KeyProvider {
	fn get_node_key(&self, node_id: NodeId) -> Key;
	fn get_state_key(&self) -> Key {
		panic!("Not supported")
	}
	async fn load_node<BK>(&self, tx: &mut Transaction, id: NodeId) -> Result<StoredNode<BK>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let key = self.get_node_key(id);
		let (node, size) = Node::<BK>::read(tx, key.clone()).await?;
		Ok(StoredNode {
			node,
			id,
			key,
			size,
		})
	}
}

pub(super) struct BTree<K>
where
	K: KeyProvider + Clone,
{
	keys: K,
	state: State,
	full_size: usize,
	updated: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct State {
	minimum_degree: usize,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

impl SerdeState for State {}

impl State {
	pub(super) fn new(minimum_degree: usize) -> Self {
		assert!(minimum_degree >= 2, "Minimum degree should be >= 2");
		Self {
			minimum_degree,
			root: None,
			next_node_id: 0,
		}
	}
}

#[derive(Debug, Default, PartialEq)]
pub(super) struct Statistics {
	pub(super) keys_count: usize,
	pub(super) max_depth: usize,
	pub(super) nodes_count: usize,
	pub(super) total_size: usize,
}

#[derive(Serialize, Deserialize)]
enum Node<BK>
where
	BK: BKeys,
{
	Internal(BK, Vec<NodeId>),
	Leaf(BK),
}

impl<'a, BK> Node<BK>
where
	BK: BKeys + Serialize + DeserializeOwned + 'a,
{
	async fn read(tx: &mut Transaction, key: Key) -> Result<(Self, usize), Error> {
		if let Some(val) = tx.get(key).await? {
			let size = val.len();
			Ok((Node::try_from_val(val)?, size))
		} else {
			Err(Error::CorruptedIndex)
		}
	}

	async fn write(&mut self, tx: &mut Transaction, key: Key) -> Result<usize, Error> {
		self.keys_mut().compile();
		let val = self.try_to_val()?;
		let size = val.len();
		tx.set(key, val).await?;
		Ok(size)
	}

	fn keys(&self) -> &BK {
		match self {
			Node::Internal(keys, _) => keys,
			Node::Leaf(keys) => keys,
		}
	}

	fn keys_mut(&mut self) -> &mut BK {
		match self {
			Node::Internal(keys, _) => keys,
			Node::Leaf(keys) => keys,
		}
	}

	fn append(&mut self, key: Key, payload: Payload, node: Node<BK>) -> Result<(), Error> {
		match self {
			Node::Internal(keys, children) => {
				if let Node::Internal(append_keys, mut append_children) = node {
					keys.insert(key, payload);
					keys.append(append_keys);
					children.append(&mut append_children);
					Ok(())
				} else {
					Err(Error::CorruptedIndex)
				}
			}
			Node::Leaf(keys) => {
				if let Node::Leaf(append_keys) = node {
					keys.insert(key, payload);
					keys.append(append_keys);
					Ok(())
				} else {
					Err(Error::CorruptedIndex)
				}
			}
		}
	}

	fn debug<F>(&self, to_string: F) -> Result<(), Error>
	where
		F: Fn(Key) -> Result<String, Error>,
	{
		match self {
			Node::Internal(keys, children) => {
				keys.debug(to_string)?;
				debug!("Children{:?}", children);
				Ok(())
			}
			Node::Leaf(keys) => keys.debug(to_string),
		}
	}
}

impl<BK> SerdeState for Node<BK> where BK: BKeys + Serialize + DeserializeOwned {}

struct SplitResult<BK>
where
	BK: BKeys,
{
	parent_node: StoredNode<BK>,
	left_node: StoredNode<BK>,
	right_node: StoredNode<BK>,
	median_key: Key,
}

impl<K> BTree<K>
where
	K: KeyProvider + Clone + Sync,
{
	pub(super) fn new(keys: K, state: State) -> Self {
		Self {
			keys,
			full_size: state.minimum_degree * 2 - 1,
			state,
			updated: false,
		}
	}

	pub(super) async fn search<BK>(
		&self,
		tx: &mut Transaction,
		searched_key: &Key,
	) -> Result<Option<Payload>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut next_node = self.state.root;
		while let Some(node_id) = next_node.take() {
			let current = self.keys.load_node::<BK>(tx, node_id).await?;
			if let Some(payload) = current.node.keys().get(searched_key) {
				return Ok(Some(payload));
			}
			if let Node::Internal(keys, children) = current.node {
				let child_idx = keys.get_child_idx(searched_key);
				next_node.replace(children[child_idx]);
			}
		}
		Ok(None)
	}

	pub(super) fn search_by_prefix(&self, prefix_key: Key) -> BTreeIterator<K> {
		BTreeIterator::new(self.keys.clone(), prefix_key, self.state.root)
	}

	pub(super) async fn insert<BK>(
		&mut self,
		tx: &mut Transaction,
		key: Key,
		payload: Payload,
	) -> Result<(), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		if let Some(root_id) = self.state.root {
			let root = self.keys.load_node::<BK>(tx, root_id).await?;
			if root.node.keys().len() == self.full_size {
				let new_root_id = self.new_node_id();
				let new_root =
					self.new_node(new_root_id, Node::Internal(BK::default(), vec![root_id]));
				self.state.root = Some(new_root.id);
				let new_root = self.split_child(tx, new_root, 0, root).await?.parent_node;
				self.insert_non_full(tx, new_root, key, payload).await?;
			} else {
				self.insert_non_full(tx, root, key, payload).await?;
			}
		} else {
			let new_root_id = self.new_node_id();
			let mut new_root_node = Node::Leaf(BK::with_key_val(key, payload)?);
			self.state.root = Some(new_root_id);
			new_root_node.write(tx, self.keys.get_node_key(new_root_id)).await?;
		}
		self.updated = true;
		Ok(())
	}

	async fn insert_non_full<BK>(
		&mut self,
		tx: &mut Transaction,
		node: StoredNode<BK>,
		key: Key,
		payload: Payload,
	) -> Result<(), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut next_node = Some(node);
		while let Some(mut node) = next_node.take() {
			let key: Key = key.clone();
			match &mut node.node {
				Node::Leaf(keys) => {
					keys.insert(key, payload);
					node.write(tx).await?;
				}
				Node::Internal(keys, children) => {
					if keys.get(&key).is_some() {
						keys.insert(key, payload);
						node.write(tx).await?;
						return Ok(());
					}
					let child_idx = keys.get_child_idx(&key);
					let child = self.keys.load_node::<BK>(tx, children[child_idx]).await?;
					let next = if child.node.keys().len() == self.full_size {
						let split_result =
							self.split_child::<BK>(tx, node, child_idx, child).await?;
						if key.gt(&split_result.median_key) {
							split_result.right_node
						} else {
							split_result.left_node
						}
					} else {
						child
					};
					next_node.replace(next);
				}
			}
		}
		Ok(())
	}

	async fn split_child<BK>(
		&mut self,
		tx: &mut Transaction,
		mut parent_node: StoredNode<BK>,
		idx: usize,
		child_node: StoredNode<BK>,
	) -> Result<SplitResult<BK>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let (left_node, right_node, median_key, median_payload) = match child_node.node {
			Node::Internal(keys, children) => self.split_internal_node::<BK>(keys, children),
			Node::Leaf(keys) => self.split_leaf_node(keys),
		};
		let right_node_id = self.new_node_id();
		match &mut parent_node.node {
			Node::Internal(ref mut keys, ref mut children) => {
				keys.insert(median_key.clone(), median_payload);
				children.insert(idx + 1, right_node_id);
			}
			Node::Leaf(ref mut keys) => {
				keys.insert(median_key.clone(), median_payload);
			}
		};
		// Save the mutated split child with half the (lower) keys
		let mut left_node = self.new_node::<BK>(child_node.id, left_node);
		left_node.write(tx).await?;
		// Save the new child with half the (upper) keys
		let mut right_node = self.new_node::<BK>(right_node_id, right_node);
		right_node.write(tx).await?;
		// Save the parent node
		parent_node.write(tx).await?;
		Ok(SplitResult {
			parent_node,
			left_node,
			right_node,
			median_key,
		})
	}

	fn split_internal_node<BK>(
		&mut self,
		keys: BK,
		mut left_children: Vec<NodeId>,
	) -> (Node<BK>, Node<BK>, Key, Payload)
	where
		BK: BKeys,
	{
		let r = keys.split_keys();
		let right_children = left_children.split_off(r.median_idx + 1);
		let left_node = Node::Internal(r.left, left_children);
		let right_node = Node::Internal(r.right, right_children);
		(left_node, right_node, r.median_key, r.median_payload)
	}

	fn split_leaf_node<BK>(&mut self, keys: BK) -> (Node<BK>, Node<BK>, Key, Payload)
	where
		BK: BKeys,
	{
		let r = keys.split_keys();
		let left_node = Node::Leaf(r.left);
		let right_node = Node::Leaf(r.right);
		(left_node, right_node, r.median_key, r.median_payload)
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.state.next_node_id;
		self.state.next_node_id += 1;
		new_node_id
	}

	pub(super) async fn delete<BK>(
		&mut self,
		tx: &mut Transaction,
		key_to_delete: Key,
	) -> Result<Option<Payload>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut deleted_payload = None;

		if let Some(root_id) = self.state.root {
			let node = self.keys.load_node::<BK>(tx, root_id).await?;
			let mut next_node = Some((true, key_to_delete, node));

			while let Some((is_main_key, key_to_delete, mut node)) = next_node.take() {
				match &mut node.node {
					Node::Leaf(keys) => {
						// CLRS: 1
						if let Some(payload) = keys.get(&key_to_delete) {
							if is_main_key {
								deleted_payload = Some(payload);
							}
							keys.remove(&key_to_delete);
							if keys.len() == 0 {
								// The node is empty, we can delete the record
								tx.del(node.key).await?;
								// Check if this was the root node
								if Some(node.id) == self.state.root {
									self.state.root = None;
								}
							} else {
								node.write(tx).await?;
							}
							self.updated = true;
						}
					}
					Node::Internal(keys, children) => {
						// CLRS: 2
						if let Some(payload) = keys.get(&key_to_delete) {
							if is_main_key {
								deleted_payload = Some(payload);
							}
							next_node.replace(
								self.deleted_from_internal(tx, keys, children, key_to_delete)
									.await?,
							);
							node.write(tx).await?;
							self.updated = true;
						} else {
							// CLRS: 3
							let (node_update, is_main_key, key_to_delete, next_stored_node) = self
								.deleted_traversal(tx, keys, children, key_to_delete, is_main_key)
								.await?;
							if keys.len() == 0 {
								if let Some(root_id) = self.state.root {
									// Delete the old root node
									tx.del(self.keys.get_node_key(root_id)).await?;
								}
								self.state.root = Some(next_stored_node.id);
								self.updated = true;
							} else if node_update {
								node.write(tx).await?;
								self.updated = true;
							}
							next_node.replace((is_main_key, key_to_delete, next_stored_node));
						}
					}
				}
			}
		}
		Ok(deleted_payload)
	}

	async fn deleted_from_internal<BK>(
		&mut self,
		tx: &mut Transaction,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		key_to_delete: Key,
	) -> Result<(bool, Key, StoredNode<BK>), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let left_idx = keys.get_child_idx(&key_to_delete);
		let left_id = children[left_idx];
		let mut left_node = self.keys.load_node::<BK>(tx, left_id).await?;
		if left_node.node.keys().len() >= self.state.minimum_degree {
			// CLRS: 2a -> left_node is named `y` in the book
			if let Some((key_prim, payload_prim)) = left_node.node.keys().get_last_key() {
				keys.remove(&key_to_delete);
				keys.insert(key_prim.clone(), payload_prim);
				return Ok((false, key_prim, left_node));
			}
		}

		let right_idx = left_idx + 1;
		let right_node = self.keys.load_node::<BK>(tx, children[right_idx]).await?;
		if right_node.node.keys().len() >= self.state.minimum_degree {
			// CLRS: 2b -> right_node is name `z` in the book
			if let Some((key_prim, payload_prim)) = right_node.node.keys().get_first_key() {
				keys.remove(&key_to_delete);
				keys.insert(key_prim.clone(), payload_prim);
				return Ok((false, key_prim, right_node));
			}
		}

		// CLRS: 2c
		// Merge children
		// The payload is set to 0. The value does not matter, as the key will be deleted after anyway.
		left_node.node.append(key_to_delete.clone(), 0, right_node.node)?;
		left_node.write(tx).await?;
		tx.del(right_node.key).await?;
		keys.remove(&key_to_delete);
		children.remove(right_idx);
		Ok((false, key_to_delete, left_node))
	}

	async fn deleted_traversal<BK>(
		&mut self,
		tx: &mut Transaction,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		key_to_delete: Key,
		is_main_key: bool,
	) -> Result<(bool, bool, Key, StoredNode<BK>), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		// CLRS 3a
		let child_idx = keys.get_child_idx(&key_to_delete);
		let child_stored_node = self.keys.load_node::<BK>(tx, children[child_idx]).await?;
		// TODO: Remove once everything is stable
		// debug!("** delete_traversal");
		// child_stored_node.node.keys().debug(|k| Ok(String::from_utf8(k)?))?;
		if child_stored_node.node.keys().len() < self.state.minimum_degree {
			// right child (successor)
			if child_idx < children.len() - 1 {
				let right_child_id = children[child_idx + 1];
				let right_child_stored_node = self.keys.load_node::<BK>(tx, right_child_id).await?;
				return if right_child_stored_node.node.keys().len() >= self.state.minimum_degree {
					Self::delete_adjust_successor(
						tx,
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
						tx,
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
				let left_child_id = children[child_idx];
				let left_child_stored_node = self.keys.load_node::<BK>(tx, left_child_id).await?;
				return if left_child_stored_node.node.keys().len() >= self.state.minimum_degree {
					Self::delete_adjust_predecessor(
						tx,
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
						tx,
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

		Ok((false, true, key_to_delete, child_stored_node))
	}

	async fn delete_adjust_successor<BK>(
		tx: &mut Transaction,
		keys: &mut BK,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut child_stored_node: StoredNode<BK>,
		mut right_child_stored_node: StoredNode<BK>,
	) -> Result<(bool, bool, Key, StoredNode<BK>), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		debug!("** delete_adjust_successor - child_idx: {}", child_idx);
		if let Some((ascending_key, ascending_payload)) =
			right_child_stored_node.node.keys().get_first_key()
		{
			right_child_stored_node.node.keys_mut().remove(&ascending_key);
			if let Some(descending_key) = keys.get_key(child_idx) {
				if let Some(descending_payload) = keys.remove(&descending_key) {
					child_stored_node.node.keys_mut().insert(descending_key, descending_payload);
					keys.insert(ascending_key, ascending_payload);
					child_stored_node.write(tx).await?;
					right_child_stored_node.write(tx).await?;
					return Ok((true, is_main_key, key_to_delete, child_stored_node));
				}
			}
		}
		// If we reach this point, something was wrong in the BTree
		Err(Error::CorruptedIndex)
	}

	async fn delete_adjust_predecessor<BK>(
		tx: &mut Transaction,
		keys: &mut BK,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut child_stored_node: StoredNode<BK>,
		mut left_child_stored_node: StoredNode<BK>,
	) -> Result<(bool, bool, Key, StoredNode<BK>), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		// TODO: Remove once everything is stable
		// debug!("** delete_adjust_predecessor - child_idx: {}", child_idx);
		// keys.debug(|k| Ok(String::from_utf8(k)?)).unwrap();

		if let Some((ascending_key, ascending_payload)) =
			left_child_stored_node.node.keys().get_last_key()
		{
			debug!("** ascending_key {}", String::from_utf8_lossy(&ascending_key));
			left_child_stored_node.node.keys_mut().remove(&ascending_key);
			if let Some(descending_key) = keys.get_key(child_idx) {
				debug!("** descending_key {}", String::from_utf8_lossy(&descending_key));
				if let Some(descending_payload) = keys.remove(&descending_key) {
					debug!("** descending_payload {}", descending_payload);
					child_stored_node.node.keys_mut().insert(descending_key, descending_payload);
					keys.insert(ascending_key, ascending_payload);
					child_stored_node.write(tx).await?;
					left_child_stored_node.write(tx).await?;
					return Ok((true, is_main_key, key_to_delete, child_stored_node));
				}
			}
		}
		// If we reach this point, something was wrong in the BTree
		Err(Error::CorruptedIndex)
	}

	async fn merge_nodes<BK>(
		tx: &mut Transaction,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut left_child: StoredNode<BK>,
		right_child: StoredNode<BK>,
	) -> Result<(bool, bool, Key, StoredNode<BK>), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		// TODO: Remove once everything is stable
		// debug!("** merge_nodes - child_idx: {}", child_idx);
		// keys.debug(|k| Ok(String::from_utf8(k)?)).unwrap();

		if let Some(descending_key) = keys.get_key(child_idx) {
			if let Some(descending_payload) = keys.remove(&descending_key) {
				children.remove(child_idx + 1);
				left_child.node.append(descending_key, descending_payload, right_child.node)?;
				left_child.write(tx).await?;
				tx.del(right_child.key).await?;
				return Ok((true, is_main_key, key_to_delete, left_child));
			}
		}
		// If we reach this point, something was wrong in the BTree
		Err(Error::CorruptedIndex)
	}

	/// This is for debugging
	async fn inspect_nodes<BK, F>(
		&self,
		tx: &mut Transaction,
		inspect_func: F,
	) -> Result<usize, Error>
	where
		F: Fn(usize, usize, NodeId, StoredNode<BK>),
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, 1));
		}
		let mut count = 0;
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let stored_node = self.keys.load_node::<BK>(tx, node_id).await?;
			if let Node::Internal(_, children) = &stored_node.node {
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

	pub(super) async fn statistics<BK>(&self, tx: &mut Transaction) -> Result<Statistics, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut stats = Statistics::default();
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, 1));
		}
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let stored = self.keys.load_node::<BK>(tx, node_id).await?;
			stats.keys_count += stored.node.keys().len();
			if depth > stats.max_depth {
				stats.max_depth = depth;
			}
			stats.nodes_count += 1;
			stats.total_size += stored.size;
			if let Node::Internal(_, children) = stored.node {
				let depth = depth + 1;
				for child_id in &children {
					node_queue.push_front((*child_id, depth));
				}
			};
		}
		Ok(stats)
	}

	pub(super) fn get_state(&self) -> &State {
		&self.state
	}

	pub(super) fn is_updated(&self) -> bool {
		self.updated
	}

	fn new_node<BK>(&self, id: NodeId, node: Node<BK>) -> StoredNode<BK>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		StoredNode {
			node,
			id,
			key: self.keys.get_node_key(id),
			size: 0,
		}
	}
}

struct CurrentNode {
	keys: VecDeque<(Key, Payload)>,
	matches_found: bool,
	level_matches_found: Arc<AtomicBool>,
}

pub(super) struct BTreeIterator<K>
where
	K: KeyProvider,
{
	key_provider: K,
	prefix_key: Key,
	node_queue: VecDeque<(NodeId, Arc<AtomicBool>)>,
	current_node: Option<CurrentNode>,
}

impl<K> BTreeIterator<K>
where
	K: KeyProvider + Sync,
{
	fn new(key_provider: K, prefix_key: Key, start_node: Option<NodeId>) -> Self {
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = start_node {
			node_queue.push_front((node_id, Arc::new(AtomicBool::new(false))))
		}
		Self {
			key_provider,
			prefix_key,
			node_queue,
			current_node: None,
		}
	}

	fn set_current_node<BK>(&mut self, node: Node<BK>, level_matches_found: Arc<AtomicBool>)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Node::Internal(keys, children) = &node {
			let same_level_matches_found = Arc::new(AtomicBool::new(false));
			let child_idx = keys.get_child_idx(&self.prefix_key);
			for i in child_idx..children.len() {
				self.node_queue.push_front((children[i], same_level_matches_found.clone()));
			}
		}
		let keys = node.keys().collect_with_prefix(&self.prefix_key);
		let matches_found = !keys.is_empty();
		if matches_found {
			level_matches_found.fetch_and(true, Ordering::Relaxed);
		}
		self.current_node = Some(CurrentNode {
			keys,
			matches_found,
			level_matches_found,
		});
	}

	pub(super) async fn next<BK>(
		&mut self,
		tx: &mut Transaction,
	) -> Result<Option<(Key, Payload)>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		loop {
			if let Some(current) = &mut self.current_node {
				if let Some((key, payload)) = current.keys.pop_front() {
					return Ok(Some((key, payload)));
				} else {
					if !current.matches_found && current.level_matches_found.load(Ordering::Relaxed)
					{
						// If we have found matches in previous (lower) nodes,
						// but we don't find matches anymore, there is no chance we can find new matches
						// in upper child nodes, therefore we can stop the traversal.
						break;
					}
					self.current_node = None;
				}
			} else if let Some((node_id, level_matches_found)) = self.node_queue.pop_front() {
				let st = self.key_provider.load_node::<BK>(tx, node_id).await?;
				self.set_current_node(st.node, level_matches_found);
			} else {
				break;
			}
		}
		Ok(None)
	}
}

pub(super) struct StoredNode<BK>
where
	BK: BKeys,
{
	node: Node<BK>,
	id: NodeId,
	key: Key,
	size: usize,
}

impl<BK> StoredNode<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	async fn write(&mut self, tx: &mut Transaction) -> Result<(), Error> {
		self.size = self.node.write(tx, self.key.clone()).await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::bkeys::{BKeys, FstKeys, TrieKeys};
	use crate::idx::btree::{
		BTree, BTreeIterator, KeyProvider, Node, NodeId, Payload, State, Statistics,
	};
	use crate::idx::SerdeState;
	use crate::kvs::{Datastore, Key, Transaction};
	use rand::prelude::{SliceRandom, ThreadRng};
	use rand::thread_rng;
	use serde::de::DeserializeOwned;
	use serde::Serialize;
	use std::collections::HashMap;
	use test_log::test;

	#[derive(Clone)]
	struct TestKeyProvider {}

	impl KeyProvider for TestKeyProvider {
		fn get_node_key(&self, node_id: NodeId) -> Key {
			node_id.to_be_bytes().to_vec()
		}
		fn get_state_key(&self) -> Key {
			"".into()
		}
	}

	#[test]
	fn test_btree_state_serde() {
		let s = State::new(3);
		let val = s.try_to_val().unwrap();
		let s: State = State::try_from_val(val).unwrap();
		assert_eq!(s.minimum_degree, 3);
		assert_eq!(s.root, None);
		assert_eq!(s.next_node_id, 0);
	}

	#[test]
	fn test_node_serde_internal() {
		let node = Node::Internal(FstKeys::default(), vec![]);
		let val = node.try_to_val().unwrap();
		let _: Node<FstKeys> = Node::try_from_val(val).unwrap();
	}

	#[test]
	fn test_node_serde_leaf() {
		let node = Node::Leaf(TrieKeys::default());
		let val = node.try_to_val().unwrap();
		let _: Node<TrieKeys> = Node::try_from_val(val).unwrap();
	}

	async fn insertions_test<F, BK, K>(
		tx: &mut Transaction,
		t: &mut BTree<K>,
		samples_size: usize,
		sample_provider: F,
	) where
		F: Fn(usize) -> (Key, Payload),
		BK: BKeys + Serialize + DeserializeOwned + Default,
		K: KeyProvider + Clone + Sync,
	{
		for i in 0..samples_size {
			let (key, payload) = sample_provider(i);
			// Insert the sample
			t.insert::<BK>(tx, key.clone(), payload).await.unwrap();
			// Check we can find it
			assert_eq!(t.search::<BK>(tx, &key).await.unwrap(), Some(payload));
		}
	}

	fn get_key_value(idx: usize) -> (Key, Payload) {
		(format!("{}", idx).into(), (idx * 10) as Payload)
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_sequential_insertions() {
		let mut t = BTree::new(TestKeyProvider {}, State::new(5));
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		insertions_test::<_, FstKeys, _>(&mut tx, &mut t, 100, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<FstKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 22,
				total_size: 1757,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_sequential_insertions() {
		let mut t = BTree::new(TestKeyProvider {}, State::new(6));
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		insertions_test::<_, TrieKeys, _>(&mut tx, &mut t, 100, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<TrieKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 18,
				total_size: 1710,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_random_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(8));
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, FstKeys, _>(&mut tx, &mut t, 100, |i| get_key_value(samples[i])).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<FstKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 100);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_random_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(75));
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, TrieKeys, _>(&mut tx, &mut t, 100, |i| get_key_value(samples[i]))
			.await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<TrieKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 100);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_large_order_sequential_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(60));
		insertions_test::<_, FstKeys, _>(&mut tx, &mut t, 10000, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<FstKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 158,
				total_size: 57960,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_large_order_sequential_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(60));
		insertions_test::<_, TrieKeys, _>(&mut tx, &mut t, 10000, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<TrieKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 158,
				total_size: 75680,
			}
		);
	}

	const REAL_WORLD_TERMS: [&str; 30] = [
		"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "the", "fast",
		"fox", "jumped", "over", "the", "lazy", "dog", "the", "dog", "sat", "there", "and", "did",
		"nothing", "the", "other", "animals", "sat", "there", "watching",
	];

	async fn test_btree_read_world_insertions<BK>(default_minimum_degree: usize) -> Statistics
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(default_minimum_degree));
		insertions_test::<_, BK, _>(&mut tx, &mut t, REAL_WORLD_TERMS.len(), |i| {
			(REAL_WORLD_TERMS[i].as_bytes().to_vec(), i as Payload)
		})
		.await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		t.statistics::<BK>(&mut tx).await.unwrap()
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(4).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 5,
				total_size: 436,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(100).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 1,
				nodes_count: 1,
				total_size: 192,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(6).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 3,
				total_size: 348,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(100).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 1,
				nodes_count: 1,
				total_size: 232,
			}
		);
	}

	async fn test_btree_search_by_prefix(
		ds: &Datastore,
		minimum_degree: usize,
		shuffle: bool,
		mut samples: Vec<(&str, Payload)>,
	) -> (BTree<TestKeyProvider>, Statistics) {
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(minimum_degree));
		let samples_len = samples.len();
		if shuffle {
			samples.shuffle(&mut ThreadRng::default());
		}
		for (key, payload) in samples {
			t.insert::<TrieKeys>(&mut tx, key.into(), payload).await.unwrap();
		}
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<TrieKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, samples_len);
		(t, s)
	}

	async fn check_results(
		mut i: BTreeIterator<TestKeyProvider>,
		tx: &mut Transaction,
		e: Vec<(Key, Payload)>,
	) {
		let mut map = HashMap::new();
		while let Some((k, p)) = i.next::<TrieKeys>(tx).await.unwrap() {
			map.insert(k, p);
		}
		assert_eq!(map.len(), e.len());
		for (k, p) in e {
			assert_eq!(map.get(&k), Some(&p));
		}
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_search_by_prefix() {
		for _ in 0..50 {
			let samples = vec![
				("aaaa", 0),
				("bb1", 21),
				("bb2", 22),
				("bb3", 23),
				("bb4", 24),
				("dddd", 0),
				("eeee", 0),
				("ffff", 0),
				("gggg", 0),
				("hhhh", 0),
			];
			let ds = Datastore::new("memory").await.unwrap();
			let (t, s) = test_btree_search_by_prefix(&ds, 3, true, samples).await;

			// For this test to be relevant, we expect the BTree to match the following properties:
			assert_eq!(s.max_depth, 2, "Tree depth should be 2");
			assert!(
				s.nodes_count > 2 && s.nodes_count < 5,
				"The number of node should be between 3 and 4 inclusive"
			);

			let mut tx = ds.transaction(false, false).await.unwrap();

			// We should find all the keys prefixed with "bb"
			let i = t.search_by_prefix("bb".into());
			check_results(
				i,
				&mut tx,
				vec![
					("bb1".into(), 21),
					("bb2".into(), 22),
					("bb3".into(), 23),
					("bb4".into(), 24),
				],
			)
			.await;
		}
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_real_world_search_by_prefix() {
		// We do multiples tests to run the test over many different possible forms of Tree.
		// The samples are shuffled, therefore the insertion order is different on each test,
		// ending up in slightly different variants of the BTrees.
		for _ in 0..50 {
			// This samples simulate postings. Pair of terms and document ids.
			let samples = vec![
				("the-1", 0),
				("quick-1", 0),
				("brown-1", 0),
				("fox-1", 0),
				("jumped-1", 0),
				("over-1", 0),
				("lazy-1", 0),
				("dog-1", 0),
				("the-2", 0),
				("fast-2", 0),
				("fox-2", 0),
				("jumped-2", 0),
				("over-2", 0),
				("lazy-2", 0),
				("dog-2", 0),
				("the-3", 0),
				("dog-3", 0),
				("sat-3", 0),
				("there-3", 0),
				("and-3", 0),
				("did-3", 0),
				("nothing-3", 0),
				("the-4", 0),
				("other-4", 0),
				("animals-4", 0),
				("sat-4", 0),
				("there-4", 0),
				("watching-4", 0),
			];

			let ds = Datastore::new("memory").await.unwrap();
			let (t, s) = test_btree_search_by_prefix(&ds, 7, true, samples).await;

			// For this test to be relevant, we expect the BTree to match the following properties:
			assert_eq!(s.max_depth, 2, "Tree depth should be 2");
			assert!(
				s.nodes_count > 2 && s.nodes_count < 5,
				"The number of node should be > 2 and < 5"
			);

			let mut tx = ds.transaction(false, false).await.unwrap();

			for (prefix, count) in vec![
				("the", 6),
				("there", 2),
				("dog", 3),
				("jumped", 2),
				("lazy", 2),
				("fox", 2),
				("over", 2),
				("sat", 2),
				("other", 1),
				("nothing", 1),
				("animals", 1),
				("watching", 1),
			] {
				let mut i = t.search_by_prefix(prefix.into());
				for _ in 0..count {
					assert!(i.next::<TrieKeys>(&mut tx).await.unwrap().is_some());
				}
				assert_eq!(i.next::<TrieKeys>(&mut tx).await.unwrap(), None);
			}
		}
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
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(3));
		let mut tx = ds.transaction(true, false).await.unwrap();
		for (key, payload) in CLRS_EXAMPLE {
			t.insert::<TrieKeys>(&mut tx, key.into(), payload).await.unwrap();
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<TrieKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 23);
		assert_eq!(s.max_depth, 3);
		assert_eq!(s.nodes_count, 10);
		// There should be one record per node
		assert_eq!(10, tx.scan(vec![]..vec![0xf], 100).await.unwrap().len());

		let nodes_count = t
			.inspect_nodes::<TrieKeys, _>(&mut tx, |count, depth, node_id, node| match count {
				0 => {
					assert_eq!(depth, 1);
					assert_eq!(node_id, 7);
					check_is_internal_node(node.node, vec![("p", 16)], vec![1, 8]);
				}
				1 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 1);
					check_is_internal_node(
						node.node,
						vec![("c", 3), ("g", 7), ("m", 13)],
						vec![0, 9, 2, 3],
					);
				}
				2 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 8);
					check_is_internal_node(node.node, vec![("t", 20), ("x", 24)], vec![4, 6, 5]);
				}
				3 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 0);
					check_is_leaf_node(node.node, vec![("a", 1), ("b", 2)]);
				}
				4 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 9);
					check_is_leaf_node(node.node, vec![("d", 4), ("e", 5), ("f", 6)]);
				}
				5 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 2);
					check_is_leaf_node(node.node, vec![("j", 10), ("k", 11), ("l", 12)]);
				}
				6 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 3);
					check_is_leaf_node(node.node, vec![("n", 14), ("o", 15)]);
				}
				7 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 4);
					check_is_leaf_node(node.node, vec![("q", 17), ("r", 18), ("s", 19)]);
				}
				8 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 6);
					check_is_leaf_node(node.node, vec![("u", 21), ("v", 22)]);
				}
				9 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 5);
					check_is_leaf_node(node.node, vec![("y", 25), ("z", 26)]);
				}
				_ => panic!("This node should not exist {}", count),
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 10);
	}

	// This check the possible deletion cases. CRLS, Figure 18.8, pages 500-501
	async fn test_btree_clrs_deletion_test<BK>()
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(3));
		let mut tx = ds.transaction(true, false).await.unwrap();
		for (key, payload) in CLRS_EXAMPLE {
			t.insert::<BK>(&mut tx, key.into(), payload).await.unwrap();
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		for (key, payload) in [("f", 6), ("m", 13), ("g", 7), ("d", 4), ("b", 2)] {
			debug!("Delete {}", key);
			assert_eq!(t.delete::<BK>(&mut tx, key.into()).await.unwrap(), Some(payload));
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<BK>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 18);
		assert_eq!(s.max_depth, 2);
		assert_eq!(s.nodes_count, 7);
		// There should be one record per node
		assert_eq!(7, tx.scan(vec![]..vec![0xf], 100).await.unwrap().len());

		let nodes_count = t
			.inspect_nodes::<BK, _>(&mut tx, |count, depth, node_id, node| {
				debug!("{} -> {}", depth, node_id);
				node.node.debug(|k| Ok(String::from_utf8(k)?)).unwrap();
				match count {
					0 => {
						assert_eq!(depth, 1);
						assert_eq!(node_id, 1);
						check_is_internal_node(
							node.node,
							vec![("e", 5), ("l", 12), ("p", 16), ("t", 20), ("x", 24)],
							vec![0, 9, 3, 4, 6, 5],
						);
					}
					1 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 0);
						check_is_leaf_node(node.node, vec![("a", 1), ("c", 3)]);
					}
					2 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 9);
						check_is_leaf_node(node.node, vec![("j", 10), ("k", 11)]);
					}
					3 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 3);
						check_is_leaf_node(node.node, vec![("n", 14), ("o", 15)]);
					}
					4 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 4);
						check_is_leaf_node(node.node, vec![("q", 17), ("r", 18), ("s", 19)]);
					}
					5 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 6);
						check_is_leaf_node(node.node, vec![("u", 21), ("v", 22)]);
					}
					6 => {
						assert_eq!(depth, 2);
						assert_eq!(node_id, 5);
						check_is_leaf_node(node.node, vec![("y", 25), ("z", 26)]);
					}
					_ => panic!("This node should not exist {}", count),
				}
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 7);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_clrs_deletion_test() {
		test_btree_clrs_deletion_test::<TrieKeys>().await
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_clrs_deletion_test() {
		test_btree_clrs_deletion_test::<FstKeys>().await
	}

	// This check the possible deletion cases. CRLS, Figure 18.8, pages 500-501
	async fn test_btree_fill_and_empty<BK>()
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(3));

		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut expected_keys = HashMap::new();
		for (key, payload) in CLRS_EXAMPLE {
			expected_keys.insert(key.to_string(), payload);
			t.insert::<BK>(&mut tx, key.into(), payload).await.unwrap();
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		print_tree::<BK, _>(&mut tx, &t).await;

		for (key, _) in CLRS_EXAMPLE {
			debug!("------------------------");
			debug!("Delete {}", key);
			t.delete::<BK>(&mut tx, key.into()).await.unwrap();
			print_tree::<BK, _>(&mut tx, &t).await;

			// Check that every expected keys are still found in the tree
			expected_keys.remove(key);
			for (key, payload) in &expected_keys {
				assert_eq!(
					t.search::<BK>(&mut tx, &key.as_str().into()).await.unwrap(),
					Some(*payload)
				)
			}
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<BK>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 0);
		assert_eq!(s.max_depth, 0);
		assert_eq!(s.nodes_count, 0);
		// There should not be any record in the database
		assert_eq!(0, tx.scan(vec![]..vec![0xf], 100).await.unwrap().len());
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_fill_and_empty() {
		test_btree_fill_and_empty::<TrieKeys>().await
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_fill_and_empty() {
		test_btree_fill_and_empty::<FstKeys>().await
	}

	/////////////
	// HELPERS //
	/////////////

	fn check_is_internal_node<BK>(
		node: Node<BK>,
		expected_keys: Vec<(&str, i32)>,
		expected_children: Vec<NodeId>,
	) where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Node::Internal(keys, children) = node {
			check_keys(keys, expected_keys);
			assert_eq!(children, expected_children, "The children are not matching");
		} else {
			panic!("An internal node was expected, we got a leaf node");
		}
	}

	fn check_is_leaf_node<BK>(node: Node<BK>, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Node::Leaf(keys) = node {
			check_keys(keys, expected_keys);
		} else {
			panic!("An internal node was expected, we got a leaf node");
		}
	}

	async fn print_tree<BK, K>(tx: &mut Transaction, t: &BTree<K>)
	where
		K: KeyProvider + Clone + Sync,
		BK: BKeys + Serialize + DeserializeOwned,
	{
		debug!("----------------------------------");
		t.inspect_nodes::<BK, _>(tx, |_count, depth, node_id, node| {
			debug!("{} -> {}", depth, node_id);
			node.node.debug(|k| Ok(String::from_utf8(k)?)).unwrap();
		})
		.await
		.unwrap();
		debug!("----------------------------------");
	}

	fn check_keys<BK>(keys: BK, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		assert_eq!(keys.len(), expected_keys.len(), "The number of keys does not match");
		for (key, payload) in expected_keys {
			assert_eq!(
				keys.get(&key.into()),
				Some(payload as Payload),
				"The key {} does not match",
				key
			);
		}
	}
}
