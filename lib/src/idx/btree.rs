use crate::idx::bkeys::{BKeys, KeyVisitor};
use crate::idx::kvsim::KVSimulator;
use crate::idx::{Domain, IndexId};
use crate::kvs::Key;
use derive::Key;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;

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

	pub(super) fn search_by_prefix<BK, V>(
		&self,
		kv: &mut KVSimulator,
		prefix_key: &Key,
		visitor: &mut V,
	) where
		BK: BKeys + Serialize + DeserializeOwned,
		V: KeyVisitor,
	{
		if let Some(root_id) = &self.root {
			self.recursive_search_by_prefix::<BK, V>(kv, *root_id, prefix_key, visitor);
		}
	}

	fn recursive_search_by_prefix<BK, V>(
		&self,
		kv: &mut KVSimulator,
		node_id: NodeId,
		prefix_key: &Key,
		visitor: &mut V,
	) -> bool
	where
		BK: BKeys + Serialize + DeserializeOwned,
		V: KeyVisitor,
	{
		let node = StoredNode::<BK>::read(kv, self.new_node_key(node_id)).node;
		// If we previously found keys, and this node does not add additional keys, we can skip
		let mut found = node.keys().collect_with_prefix(prefix_key, visitor);
		if let Node::Internal(keys, children) = node {
			let child_idx = keys.get_child_idx(prefix_key);
			for i in child_idx..children.len() {
				if !self.recursive_search_by_prefix::<BK, V>(kv, children[i], prefix_key, visitor) {
					break;
				} else {
					if !found {
						found = true;
					}
				}
			}
		}
		found
	}

	pub(super) fn insert<BK>(&mut self, kv: &mut KVSimulator, key: Key, payload: Payload)
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		if let Some(root_id) = self.root {
			let root = StoredNode::<BK>::read(kv, self.new_node_key(root_id));
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
			StoredNode::<BK>::write(kv, self.new_node_key(new_root_id).into(), new_root_node);
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
				StoredNode::<BK>::write(kv, node.key, node.node);
			}
			Node::Internal(keys, children) => {
				if keys.get(&key).is_some() {
					keys.insert(key, payload);
					StoredNode::<BK>::write(kv, node.key, node.node);
					return;
				}
				let child_idx = keys.get_child_idx(&key);
				let child_key = self.new_node_key(children[child_idx]);
				let child_node = StoredNode::<BK>::read(kv, child_key);
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
		let (left_node, right_node, median_key, median_payload) = match child_node.node {
			Node::Internal(keys, children) => self.split_internal_node::<BK>(keys, children),
			Node::Leaf(keys) => self.split_leaf_node(keys),
		};
		let right_node_id = self.new_node_id();
		let parent_node = match parent_node {
			Node::Internal(mut keys, mut children) => {
				keys.insert(median_key.clone(), median_payload);
				children.insert(idx + 1, right_node_id);
				Node::Internal(keys, children)
			}
			Node::Leaf(mut keys) => {
				keys.insert(median_key.clone(), median_payload);
				Node::Leaf(keys)
			}
		};
		// Save the mutated split child with half the (lower) keys
		let left_node = StoredNode::<BK>::write(kv, child_node.key, left_node);
		// Save the new child with half the (upper) keys
		let right_node =
			StoredNode::<BK>::write(kv, self.new_node_key(right_node_id).into(), right_node);
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

	pub(super) fn debug<F, BK>(&self, kv: &mut KVSimulator, to_string: F)
	where
		F: Fn(Key) -> String,
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.root {
			node_queue.push_front((node_id, 0));
		}
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let node = StoredNode::<BK>::read(kv, self.new_node_key(node_id)).node;
			debug!("Node: {} - depth: {} -  keys: ", node_id, depth);
			node.keys().debug(|k| to_string(k));
			if let Node::Internal(_, children) = node {
				debug!("children: {:?}", children);
				let depth = depth + 1;
				for child_id in &children {
					node_queue.push_front((*child_id, depth));
				}
			}
		}
	}

	pub(super) fn statistics<BK>(&self, kv: &mut KVSimulator) -> Statistics
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut stats = Statistics::default();
		if let Some(root_id) = &self.root {
			self.recursive_stats::<BK>(kv, 1, *root_id, &mut stats);
		}
		stats
	}

	fn recursive_stats<BK>(
		&self,
		kv: &mut KVSimulator,
		mut depth: usize,
		node_id: NodeId,
		stats: &mut Statistics,
	) where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let stored = StoredNode::<BK>::read(kv, self.new_node_key(node_id));
		stats.keys_count += stored.node.keys().len();
		if depth > stats.max_depth {
			stats.max_depth = depth;
		}
		stats.nodes_count += 1;
		stats.total_size += stored.size;
		if let Node::Internal(_, children) = stored.node {
			depth += 1;
			for child_id in &children {
				self.recursive_stats::<BK>(kv, depth, *child_id, stats);
			}
		};
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
		let (size, node): (_, Node<BK>) = kv.get_with_size(&key).unwrap();
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
	use crate::idx::btree::{BTree, Node, Payload, Statistics};
	use crate::idx::kvsim::KVSimulator;
	use crate::idx::tests::HashVisitor;
	use crate::kvs::Key;
	use rand::prelude::SliceRandom;
	use rand::thread_rng;
	use serde::de::DeserializeOwned;
	use serde::Serialize;
	use test_log::test;

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
		for i in 0..samples_size {
			let (key, payload) = sample_provider(i);
			// Insert the sample
			t.insert::<BK>(kv, key.clone(), payload);
			// Check we can find it
			assert_eq!(t.search::<BK>(kv, &key), Some(payload));
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
		assert_eq!(
			t.statistics::<FstKeys>(&mut kv),
			Statistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 10,
				total_size: 1042,
			}
		);
		t.debug::<_, FstKeys>(&mut kv, |k| String::from_utf8(k).unwrap());
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_small_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 75);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 100, get_key_value);
		assert_eq!(
			t.statistics::<TrieKeys>(&mut kv),
			Statistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 16,
				total_size: 1615,
			}
		);
		t.debug::<_, TrieKeys>(&mut kv, |k| String::from_utf8(k).unwrap());
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
		let s = t.statistics::<FstKeys>(&mut kv);
		assert_eq!(s.keys_count, 100);
		t.debug::<_, FstKeys>(&mut kv, |k| String::from_utf8(k).unwrap());
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
		let s = t.statistics::<TrieKeys>(&mut kv);
		assert_eq!(s.keys_count, 100);
		t.debug::<_, TrieKeys>(&mut kv, |k| String::from_utf8(k).unwrap());
		kv.print_stats();
	}

	#[test]
	fn test_btree_fst_keys_large_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 500);
		insertions_test::<_, FstKeys>(&mut kv, &mut t, 10000, get_key_value);
		assert_eq!(
			t.statistics::<FstKeys>(&mut kv),
			Statistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 100,
				total_size: 54548,
			}
		);
		kv.print_stats();
	}

	#[test]
	fn test_btree_trie_keys_large_order_sequential_insertions() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 500);
		insertions_test::<_, TrieKeys>(&mut kv, &mut t, 10000, get_key_value);
		assert_eq!(
			t.statistics::<TrieKeys>(&mut kv),
			Statistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 135,
				total_size: 74107,
			}
		);
		kv.print_stats();
	}

	const REAL_WORLD_TERMS: [&str; 30] = [
		"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "the", "fast",
		"fox", "jumped", "over", "the", "lazy", "dog", "the", "dog", "sat", "there", "and", "did",
		"nothing", "the", "other", "animals", "sat", "there", "watching",
	];

	fn test_btree_read_world_insertions<BK>(default_btree_order: usize) -> Statistics
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, default_btree_order);
		insertions_test::<_, BK>(&mut kv, &mut t, REAL_WORLD_TERMS.len(), |i| {
			(REAL_WORLD_TERMS[i].as_bytes().to_vec(), i as Payload)
		});
		kv.print_stats();
		t.statistics::<BK>(&mut kv)
	}

	#[test]
	fn test_btree_fst_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(70);
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 3,
				total_size: 317,
			}
		);
	}

	#[test]
	fn test_btree_fst_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(1000);
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

	#[test]
	fn test_btree_trie_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(70);
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 3,
				total_size: 346,
			}
		);
	}

	#[test]
	fn test_btree_trie_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(1000);
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

	#[test]
	fn test_btree_trie_keys_search_by_prefix() {
		let mut kv = KVSimulator::new(None, 0);
		let mut t = BTree::new(1u8, 2u64, 45);
		for (key, payload) in vec![
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
		] {
			t.insert::<TrieKeys>(&mut kv, key.into(), payload);
		}
		// For this test to be relevant, we expect the BTree to match the following statistics:
		let s = t.statistics::<TrieKeys>(&mut kv);
		assert_eq!(s.max_depth, 2);
		assert_eq!(s.nodes_count, 4);
		t.debug::<_, TrieKeys>(&mut kv, |k| String::from_utf8(k).unwrap());

		// We should find all the keys prefixed with "bb"
		let mut visitor = HashVisitor::default();
		t.search_by_prefix::<TrieKeys, _>(&mut kv, &"bb".into(), &mut visitor);
		visitor.check(vec![
			("bb1".into(), 21),
			("bb2".into(), 22),
			("bb3".into(), 23),
			("bb4".into(), 24),
		]);
	}
}
