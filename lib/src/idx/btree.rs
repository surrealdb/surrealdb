use crate::idx::bkeys::BKeys;
use crate::idx::kvsim::KVSimulator;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

pub(super) type NodeId = u64;
pub(super) type Key = String;
pub(super) type Val = u64;

pub(super) struct BTree {
	order: usize,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
enum Node<BK>
where
	BK: BKeys,
{
	// TODO remove NodeID from serialisation
	Internal(NodeId, BK, Vec<NodeId>),
	Leaf(NodeId, BK),
}

impl<BK> Node<BK>
where
	BK: BKeys,
{
	fn id(&self) -> NodeId {
		match &self {
			Node::Internal(id, _, _) => *id,
			Node::Leaf(id, _) => *id,
		}
	}

	fn keys(&self) -> &BK {
		match self {
			Node::Internal(_, keys, _) => keys,
			Node::Leaf(_, keys) => keys,
		}
	}

	fn keys_mut(&mut self) -> &mut BK {
		match self {
			Node::Internal(_, keys, _) => keys,
			Node::Leaf(_, keys) => keys,
		}
	}
}

impl<BK> Debug for Node<BK>
where
	BK: BKeys,
{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Node::Internal(node_id, keys, children) => {
				write!(f, "INTERNAL({}) => KEYS: [{}] - CHILDREN: {:?}", node_id, keys, children)?;
			}
			Node::Leaf(node_id, keys) => {
				write!(f, "LEAF({}) => KEYS: [{}]", node_id, keys)?;
			}
		}
		Ok(())
	}
}

struct SplitResult<BK>
where
	BK: BKeys,
{
	parent_node: Node<BK>,
	left_node: Node<BK>,
	right_node: Node<BK>,
	median_key: Key,
}

impl BTree {
	pub fn new(order: usize) -> Self {
		Self {
			order,
			root: None,
			next_node_id: 0,
		}
	}

	pub(super) fn search<BK>(&self, kv: &mut KVSimulator, searched_key: &Key) -> Option<u64>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Some(root_id) = &self.root {
			self.recursive_search::<BK>(kv, root_id, searched_key)
		} else {
			None
		}
	}

	fn recursive_search<BK>(
		&self,
		kv: &mut KVSimulator,
		node_id: &NodeId,
		searched_key: &Key,
	) -> Option<Val>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let node = StoredNode::<BK>::read(kv, node_id).node;
		if let Some(value) = node.keys().get(searched_key) {
			return Some(value);
		}
		if let Node::Internal(_, keys, children) = node {
			let child_idx = keys.get_child_idx(searched_key);
			self.recursive_search::<BK>(kv, &children[child_idx], searched_key)
		} else {
			None
		}
	}

	pub(super) fn insert<BK>(&mut self, kv: &mut KVSimulator, key: Key, value: Val)
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		if let Some(root_id) = &self.root {
			let root = StoredNode::<BK>::read(kv, root_id);
			if root.is_full(self.order * 2) {
				let new_root = self.new_internal_node(BK::default(), vec![*root_id]);
				self.root = Some(new_root.id());
				let new_root = self.split_child(kv, new_root, 0, root.node).parent_node;
				self.insert_non_full(kv, new_root, key, value);
			} else {
				self.insert_non_full(kv, root.node, key, value);
			}
		} else {
			let new_root = self.new_leaf_node(BK::with_key_val(key, value));
			self.root = Some(new_root.id());
			StoredNode::write(kv, new_root);
		}
	}

	fn insert_non_full<BK>(
		&mut self,
		kv: &mut KVSimulator,
		mut node: Node<BK>,
		key: Key,
		value: Val,
	) where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		match &mut node {
			Node::Leaf(_, keys) => {
				keys.insert(key, value);
				StoredNode::write(kv, node);
			}
			Node::Internal(_, keys, children) => {
				let child_idx = keys.get_child_idx(&key);
				let child_node = StoredNode::read(kv, &children[child_idx]);
				let child_node = if child_node.is_full(self.order * 2) {
					let split_result = self.split_child(kv, node, child_idx, child_node.node);
					if key.gt(&split_result.median_key) {
						split_result.right_node
					} else {
						split_result.left_node
					}
				} else {
					child_node.node
				};
				self.insert_non_full(kv, child_node, key, value);
			}
		}
	}

	fn split_child<BK>(
		&mut self,
		kv: &mut KVSimulator,
		parent_node: Node<BK>,
		idx: usize,
		child_node: Node<BK>,
	) -> SplitResult<BK>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let (left_node, right_node, median_key, median_value) = match child_node {
			Node::Internal(node_id, keys, children) => {
				self.split_internal_node(node_id, keys, children)
			}
			Node::Leaf(node_id, keys) => self.split_leaf_node(node_id, keys),
		};
		let parent_node = match parent_node {
			Node::Internal(node_id, mut keys, mut children) => {
				keys.insert(median_key.clone(), median_value);
				children.insert(idx + 1, right_node.id());
				Self::update_internal_node(node_id, keys, children)
			}
			Node::Leaf(node_id, mut keys) => {
				keys.insert(median_key.clone(), median_value);
				Self::update_leaf_node(node_id, keys)
			}
		};
		// Save the mutated split child with half the (lower) keys
		let left_node = StoredNode::<BK>::write(kv, left_node).node;
		// Save the new child with half the (upper) keys
		let right_node = StoredNode::<BK>::write(kv, right_node).node;
		// Save the parent node
		let parent_node = StoredNode::<BK>::write(kv, parent_node).node;
		SplitResult {
			parent_node,
			left_node,
			right_node,
			median_key,
		}
	}

	fn split_internal_node<M>(
		&mut self,
		node_id: NodeId,
		keys: M,
		mut left_children: Vec<NodeId>,
	) -> (Node<M>, Node<M>, Key, Val)
	where
		M: BKeys,
	{
		let (median_idx, left_keys, median_key, median_value, right_keys) = keys.split_keys();
		debug!(
			"split_internal_node {} - left: {} - right: {} - median_idx: {}",
			node_id, left_keys, right_keys, median_idx
		);
		let median_key = String::from_utf8(median_key).unwrap();
		let right_children = left_children.split_off(median_idx + 1);
		let left_node = Self::update_internal_node(node_id, left_keys, left_children);
		let right_node = self.new_internal_node(right_keys, right_children);
		(left_node, right_node, median_key, median_value)
	}

	fn split_leaf_node<M>(&mut self, node_id: NodeId, keys: M) -> (Node<M>, Node<M>, Key, Val)
	where
		M: BKeys,
	{
		let (_, left_keys, median_key, median_value, right_keys) = keys.split_keys();
		debug!("split_leaf_node {} - left: {} - right: {}", node_id, left_keys, right_keys);
		let median_key = String::from_utf8(median_key).unwrap();
		let left_node = Self::update_leaf_node(node_id, left_keys);
		let right_node = self.new_leaf_node(right_keys);
		(left_node, right_node, median_key, median_value)
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.next_node_id;
		self.next_node_id += 1;
		new_node_id
	}

	fn new_internal_node<BK>(&mut self, keys: BK, children: Vec<NodeId>) -> Node<BK>
	where
		BK: BKeys,
	{
		Node::Internal(self.new_node_id(), keys, children)
	}

	fn update_internal_node<BK>(id: NodeId, keys: BK, children: Vec<NodeId>) -> Node<BK>
	where
		BK: BKeys,
	{
		Node::Internal(id, keys, children)
	}

	fn new_leaf_node<BK>(&mut self, keys: BK) -> Node<BK>
	where
		BK: BKeys,
	{
		Node::Leaf(self.new_node_id(), keys)
	}

	fn update_leaf_node<BK>(id: NodeId, keys: BK) -> Node<BK>
	where
		BK: BKeys,
	{
		Node::Leaf(id, keys)
	}

	pub(super) fn debug<BK>(&self, kv: &mut KVSimulator)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Some(root_id) = &self.root {
			self.recursive_debug::<BK>(kv, 0, root_id);
		}
	}

	fn recursive_debug<BK>(&self, kv: &mut KVSimulator, depth: usize, node_id: &NodeId)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let node = StoredNode::<BK>::read(kv, node_id).node;
		println!("DEPTH({}) -> {:?}", depth, node);
		if let Node::Internal(_, _, children) = node {
			let depth = depth + 1;
			for child_id in &children {
				self.recursive_debug::<BK>(kv, depth, child_id);
			}
		}
	}
}

struct StoredNode<M>
where
	M: BKeys,
{
	node: Node<M>,
	size: usize,
}

impl<BK> StoredNode<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	fn read(kv: &mut KVSimulator, node_id: &NodeId) -> Self {
		let (size, node) = kv.get(&node_id.to_be_bytes().to_vec()).unwrap();
		Self {
			size,
			node,
		}
	}

	fn write(kv: &mut KVSimulator, mut node: Node<BK>) -> Self {
		let node_id = node.id();
		node.keys_mut().compile();
		let size = kv.set(node_id.to_be_bytes().to_vec(), &node);
		debug!("Save {:?} - size: {}", node, size);
		Self {
			size,
			node,
		}
	}

	fn is_full(&self, full_size: usize) -> bool {
		self.size >= full_size
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::bkeys::{BKeys, FstKeys, TrieKeys};
	use crate::idx::btree::{BTree, Key, Val};
	use crate::idx::kvsim::KVSimulator;
	use rand::prelude::SliceRandom;
	use rand::thread_rng;
	use serde::de::DeserializeOwned;
	use serde::Serialize;

	fn insertions_test<F, BK>(
		kv: &mut KVSimulator,
		t: &mut BTree,
		samples_size: usize,
		sample_provider: F,
	) where
		F: Fn(usize) -> (Key, Val),
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		// Insert the samples
		for i in 0..samples_size {
			let (key, val) = sample_provider(i);
			debug!("Insert {}=>{}", key, val);
			t.insert::<BK>(kv, key, val);
		}
		// Lookup and check the samples
		for i in 0..samples_size {
			let (key, val) = sample_provider(i);
			assert_eq!(t.search::<BK>(kv, &key), Some(val));
		}
	}

	fn get_key_value(idx: usize) -> (Key, Val) {
		(format!("{}", idx), (idx * 10) as u64)
	}

	#[test]
	fn test_btree_fst_small_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(75);
		insertions_test::<_, FstKeys>(&mut kv, &mut t, 100, get_key_value);
		t.debug::<FstKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_small_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(75);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 100, get_key_value);
		t.debug::<TrieKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_fst_small_order_random_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(75);
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, FstKeys>(&mut kv, &mut t, 100, |i| get_key_value(samples[i]));
		t.debug::<FstKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_small_order_random_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(75);
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 100, |i| get_key_value(samples[i]));
		t.debug::<TrieKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_fst_keys_large_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(500);
		insertions_test::<_, FstKeys>(&mut kv, &mut t, 10000, get_key_value);
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_keys_large_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(500);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 10000, get_key_value);
		kv.print_stats();
	}
}
