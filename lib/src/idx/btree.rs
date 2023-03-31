use crate::idx::bkeys::BKeys;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{Domain, IndexId};
use crate::kvs::Key;
use derive::Key;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub(super) type NodeId = u64;
pub(super) type Payload = u64;

#[derive(Serialize, Deserialize, Key)]
struct NodeKey {
	domain: Domain,
	index_id: IndexId,
	node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub(super) struct BTree {
	domain: Domain,
	index_id: IndexId,
	order: usize,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
enum Node<BK>
where
	BK: BKeys,
{
	Internal(BK, Vec<NodeId>),
	Leaf(BK),
}

impl<BK> Node<BK>
where
	BK: BKeys,
{
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
}

struct SplitResult<BK>
where
	BK: BKeys,
{
	parent_node: StoredNode<BK>,
	left_node: StoredNode<BK>,
	right_node: StoredNode<BK>,
	median_key: Key,
}

impl BTree {
	pub fn new(domain: Domain, index_id: IndexId, order: usize) -> Self {
		Self {
			domain,
			index_id,
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
			self.recursive_search::<BK>(kv, *root_id, searched_key)
		} else {
			None
		}
	}

	fn recursive_search<BK>(
		&self,
		kv: &mut KVSimulator,
		node_id: NodeId,
		searched_key: &Key,
	) -> Option<Payload>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let node = StoredNode::<BK>::read(kv, self.new_node_key(node_id)).node;
		if let Some(value) = node.keys().get(searched_key) {
			return Some(value);
		}
		if let Node::Internal(keys, children) = node {
			let child_idx = keys.get_child_idx(searched_key);
			self.recursive_search::<BK>(kv, children[child_idx], searched_key)
		} else {
			None
		}
	}

	pub(super) fn search_by_prefix<BK>(
		&self,
		kv: &mut KVSimulator,
		prefix_key: &Key,
	) -> Vec<(Key, Payload)>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Some(root_id) = &self.root {
			let mut res = Vec::new();
			self.recursive_search_by_prefix::<BK>(kv, *root_id, prefix_key, &mut res);
			res
		} else {
			vec![]
		}
	}

	fn recursive_search_by_prefix<BK>(
		&self,
		kv: &mut KVSimulator,
		node_id: NodeId,
		prefix_key: &Key,
		res: &mut Vec<(Key, Payload)>,
	) where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let previous_size = res.len();
		let node = StoredNode::<BK>::read(kv, self.new_node_key(node_id)).node;
		// If we previously found keys, and this node does not add additional keys, we can skip
		if previous_size > 0 && res.len() == previous_size {
			return;
		}
		node.keys().collect_with_prefix(prefix_key, res);
		if let Node::Internal(keys, children) = node {
			let child_idx = keys.get_child_idx(prefix_key);
			self.recursive_search_by_prefix::<BK>(kv, children[child_idx], prefix_key, res);
		}
	}

	pub(super) fn insert<BK>(&mut self, kv: &mut KVSimulator, key: Key, payload: Payload)
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		if let Some(root_id) = self.root {
			let root = StoredNode::read(kv, self.new_node_key(root_id));
			if root.is_full(self.order * 2) {
				let new_root_id = self.new_node_id();
				let new_root_key = self.new_node_key(new_root_id);
				let new_root_node = Node::Internal(BK::default(), vec![root_id]);
				self.root = Some(new_root_id);
				let new_root =
					self.split_child(kv, new_root_key.into(), new_root_node, 0, root).parent_node;
				self.insert_non_full(kv, new_root, key, payload);
			} else {
				self.insert_non_full(kv, root, key, payload);
			}
		} else {
			let new_root_id = self.new_node_id();
			let new_root_node = Node::Leaf(BK::with_key_val(key, payload));
			self.root = Some(new_root_id);
			StoredNode::write(kv, self.new_node_key(new_root_id).into(), new_root_node);
		}
	}

	fn insert_non_full<BK>(
		&mut self,
		kv: &mut KVSimulator,
		mut node: StoredNode<BK>,
		key: Key,
		payload: Payload,
	) where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		match &mut node.node {
			Node::Leaf(keys) => {
				keys.insert(key, payload);
				StoredNode::write(kv, node.key, node.node);
			}
			Node::Internal(keys, children) => {
				let child_idx = keys.get_child_idx(&key);
				let child_key = self.new_node_key(children[child_idx]);
				let child_node = StoredNode::read(kv, child_key);
				let child_node = if child_node.is_full(self.order * 2) {
					let split_result =
						self.split_child::<BK>(kv, node.key, node.node, child_idx, child_node);
					if key.gt(&split_result.median_key) {
						split_result.right_node
					} else {
						split_result.left_node
					}
				} else {
					child_node
				};
				self.insert_non_full(kv, child_node, key, payload);
			}
		}
	}

	fn split_child<BK>(
		&mut self,
		kv: &mut KVSimulator,
		parent_key: Key,
		parent_node: Node<BK>,
		idx: usize,
		child_node: StoredNode<BK>,
	) -> SplitResult<BK>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let (left_node, right_node, median_key, median_value) = match child_node.node {
			Node::Internal(keys, children) => self.split_internal_node::<BK>(keys, children),
			Node::Leaf(keys) => self.split_leaf_node(keys),
		};
		let right_node_id = self.new_node_id();
		let parent_node = match parent_node {
			Node::Internal(mut keys, mut children) => {
				keys.insert(median_key.clone(), median_value);
				children.insert(idx + 1, right_node_id);
				Node::Internal(keys, children)
			}
			Node::Leaf(mut keys) => {
				keys.insert(median_key.clone(), median_value);
				Node::Leaf(keys)
			}
		};
		// Save the mutated split child with half the (lower) keys
		let left_node = StoredNode::<BK>::write(kv, child_node.key, left_node);
		// Save the new child with half the (upper) keys
		let new_node_key = self.new_node_key(right_node_id).into();
		let right_node = StoredNode::<BK>::write(kv, new_node_key, right_node);
		// Save the parent node
		let parent_node = StoredNode::<BK>::write(kv, parent_key, parent_node);
		SplitResult {
			parent_node,
			left_node,
			right_node,
			median_key,
		}
	}

	fn split_internal_node<BK>(
		&mut self,
		keys: BK,
		mut left_children: Vec<NodeId>,
	) -> (Node<BK>, Node<BK>, Key, Payload)
	where
		BK: BKeys,
	{
		let (median_idx, left_keys, median_key, median_value, right_keys) = keys.split_keys();
		let right_children = left_children.split_off(median_idx + 1);
		let left_node = Node::Internal(left_keys, left_children);
		let right_node = Node::Internal(right_keys, right_children);
		(left_node, right_node, median_key, median_value)
	}

	fn split_leaf_node<BK>(&mut self, keys: BK) -> (Node<BK>, Node<BK>, Key, Payload)
	where
		BK: BKeys,
	{
		let (_, left_keys, median_key, median_value, right_keys) = keys.split_keys();
		let left_node = Node::Leaf(left_keys);
		let right_node = Node::Leaf(right_keys);
		(left_node, right_node, median_key, median_value)
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.next_node_id;
		self.next_node_id += 1;
		new_node_id
	}

	fn new_node_key(&self, node_id: NodeId) -> NodeKey {
		NodeKey {
			domain: self.domain,
			index_id: self.index_id,
			node_id,
		}
	}

	pub(super) fn debug<BK>(&self, kv: &mut KVSimulator)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Some(root_id) = &self.root {
			self.recursive_debug::<BK>(kv, 0, *root_id);
		}
	}

	fn recursive_debug<BK>(&self, kv: &mut KVSimulator, depth: usize, node_id: NodeId)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let node = StoredNode::<BK>::read(kv, self.new_node_key(node_id)).node;
		if let Node::Internal(_, children) = node {
			let depth = depth + 1;
			for child_id in &children {
				self.recursive_debug::<BK>(kv, depth, *child_id);
			}
		}
	}

	pub(super) fn count<BK>(&self, kv: &mut KVSimulator) -> usize
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Some(root_id) = &self.root {
			self.recursive_count::<BK>(kv, *root_id)
		} else {
			0
		}
	}

	fn recursive_count<BK>(&self, kv: &mut KVSimulator, node_id: NodeId) -> usize
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let node = StoredNode::<BK>::read(kv, self.new_node_key(node_id)).node;
		let mut size = node.keys().len();
		if let Node::Internal(_, children) = node {
			for child_id in &children {
				size += self.recursive_count::<BK>(kv, *child_id);
			}
		};
		size
	}
}

struct StoredNode<BK>
where
	BK: BKeys,
{
	key: Key,
	node: Node<BK>,
	size: usize,
}

impl<BK> StoredNode<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	fn read(kv: &mut KVSimulator, node_key: NodeKey) -> Self {
		let key = node_key.into();
		let (size, node) = kv.get_with_size(&key).unwrap();
		Self {
			key,
			size,
			node,
		}
	}

	fn write(kv: &mut KVSimulator, key: Key, mut node: Node<BK>) -> Self {
		node.keys_mut().compile();
		let size = kv.set(key.clone(), &node);
		Self {
			key,
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
	use crate::idx::btree::{BTree, Node, Payload};
	use crate::idx::kvsim::KVSimulator;
	use crate::kvs::Key;
	use rand::prelude::SliceRandom;
	use rand::thread_rng;
	use serde::de::DeserializeOwned;
	use serde::Serialize;

	#[test]
	fn test_btree_serde() {
		let tree = BTree::new(1u8, 2u64, 75);
		let buf = bincode::serialize(&tree).unwrap();
		let tree: BTree = bincode::deserialize(&buf).unwrap();
		assert_eq!(tree.order, 75);
		assert_eq!(tree.domain, 1);
		assert_eq!(tree.index_id, 2);
		assert_eq!(tree.next_node_id, 0);
		assert_eq!(tree.root, None);
	}

	#[test]
	fn test_node_serde_internal() {
		let node = Node::Internal(FstKeys::default(), vec![]);
		let buf = bincode::serialize(&node).unwrap();
		let _: Node<FstKeys> = bincode::deserialize(&buf).unwrap();
	}

	#[test]
	fn test_node_serde_leaf() {
		let node = Node::Leaf(TrieKeys::default());
		let buf = bincode::serialize(&node).unwrap();
		let _: Node<TrieKeys> = bincode::deserialize(&buf).unwrap();
	}

	fn insertions_test<F, BK>(
		kv: &mut KVSimulator,
		t: &mut BTree,
		samples_size: usize,
		sample_provider: F,
	) where
		F: Fn(usize) -> (Key, Payload),
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		// Insert the samples
		for i in 0..samples_size {
			let (key, payload) = sample_provider(i);
			debug!("Insert {}=>{}", String::from_utf8_lossy(&key), payload);
			t.insert::<BK>(kv, key, payload);
		}
		// Lookup and check the samples
		for i in 0..samples_size {
			let (key, val) = sample_provider(i);
			assert_eq!(t.search::<BK>(kv, &key), Some(val));
		}
	}

	fn get_key_value(idx: usize) -> (Key, Payload) {
		(format!("{}", idx).into(), (idx * 10) as u64)
	}

	#[test]
	fn test_btree_fst_small_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 75);
		insertions_test::<_, FstKeys>(&mut kv, &mut t, 100, get_key_value);
		assert_eq!(t.count::<FstKeys>(&mut kv), 100);
		t.debug::<FstKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_small_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 75);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 100, get_key_value);
		assert_eq!(t.count::<TrieKeys>(&mut kv), 100);
		t.debug::<TrieKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_fst_small_order_random_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 75);
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, FstKeys>(&mut kv, &mut t, 100, |i| get_key_value(samples[i]));
		assert_eq!(t.count::<FstKeys>(&mut kv), 100);
		t.debug::<FstKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_small_order_random_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 75);
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 100, |i| get_key_value(samples[i]));
		assert_eq!(t.count::<TrieKeys>(&mut kv), 100);
		t.debug::<TrieKeys>(&mut kv);
		kv.print_stats();
	}

	#[test]
	fn test_btree_fst_keys_large_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 500);
		insertions_test::<_, FstKeys>(&mut kv, &mut t, 10000, get_key_value);
		assert_eq!(t.count::<FstKeys>(&mut kv), 10000);
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_keys_large_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 500);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 10000, get_key_value);
		assert_eq!(t.count::<TrieKeys>(&mut kv), 10000);
		kv.print_stats();
	}
}
