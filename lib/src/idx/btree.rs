use crate::idx::fstmap::FstMap;
use crate::idx::kvsim::KVSimulator;
use fst::Streamer;
use serde::{Deserialize, Serialize};

pub(super) type NodeId = u64;
pub(super) type Key = Vec<u8>;
pub(super) type Value = u64;

pub(super) struct BTree {
	order: usize,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

#[derive(Debug, Serialize, Deserialize)]
enum Node {
	// TODO remove NodeID from serialisation
	Internal(NodeId, FstMap, Vec<NodeId>),
	Leaf(NodeId, FstMap),
}

impl Node {
	fn id(&self) -> NodeId {
		match &self {
			Node::Internal(id, _, _) => *id,
			Node::Leaf(id, _) => *id,
		}
	}

	fn keys(&self) -> &FstMap {
		match self {
			Node::Internal(_, keys, _) => keys,
			Node::Leaf(_, keys) => keys,
		}
	}

	fn keys_mut(&mut self) -> &mut FstMap {
		match self {
			Node::Internal(_, keys, _) => keys,
			Node::Leaf(_, keys) => keys,
		}
	}
}

struct SplitResult {
	parent_node: Node,
	left_node: Node,
	right_node: Node,
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

	pub(super) fn search(&self, kv: &mut KVSimulator, searched_key: &Key) -> Option<u64> {
		if let Some(root_id) = &self.root {
			self.recursive_search(kv, root_id, searched_key)
		} else {
			None
		}
	}

	fn recursive_search(
		&self,
		kv: &mut KVSimulator,
		node_id: &NodeId,
		searched_key: &Key,
	) -> Option<Value> {
		let node = StoredNode::read(kv, node_id).node;
		if let Some(value) = node.keys().get(searched_key) {
			return Some(value);
		}
		if let Node::Internal(_, keys, children) = node {
			let mut stream = keys.key_stream();
			let mut child_idx = 0;
			while let Some(key) = stream.next() {
				if key.gt(searched_key.as_slice()) {
					break;
				}
				child_idx += 1;
			}
			self.recursive_search(kv, &children[child_idx], searched_key)
		} else {
			None
		}
	}

	pub(super) fn insert(&mut self, kv: &mut KVSimulator, key: Key, value: Value) {
		if let Some(root_id) = &self.root {
			let root = StoredNode::read(kv, root_id);
			if root.is_full(self.order * 2) {
				let new_root = self.new_internal_node(FstMap::new().unwrap(), vec![*root_id]);
				self.root = Some(new_root.id());
				let new_root = self.split_child(kv, new_root, 0, root.node).parent_node;
				self.insert_non_full(kv, new_root, key, value);
			} else {
				self.insert_non_full(kv, root.node, key, value);
			}
		} else {
			let new_root = self.new_leaf_node(FstMap::with_key_value(key, value).unwrap());
			self.root = Some(new_root.id());
			StoredNode::write(kv, new_root);
		}
	}

	fn insert_non_full(&mut self, kv: &mut KVSimulator, mut node: Node, key: Key, value: Value) {
		match &mut node {
			Node::Leaf(_, keys) => {
				keys.insert(key, value);
				StoredNode::write(kv, node);
			}
			Node::Internal(_, keys, children) => {
				let mut stream = keys.key_stream();
				let mut idx = 0;
				while let Some(k) = stream.next() {
					if k.gt(key.as_slice()) {
						break;
					}
					idx += 1;
				}
				if idx >= children.len() {
					self.debug(kv);
					panic!("");
				}
				let child_node = StoredNode::read(kv, &children[idx]);
				let child_node = if child_node.is_full(self.order * 2) {
					let split_result = self.split_child(kv, node, idx, child_node.node);
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

	fn split_child(
		&mut self,
		kv: &mut KVSimulator,
		parent_node: Node,
		idx: usize,
		child_node: Node,
	) -> SplitResult {
		let (left_node, right_node, median_key, median_value) = match child_node {
			Node::Internal(node_id, keys, left_children) => {
				self.split_internal_node(node_id, keys, left_children)
			}
			Node::Leaf(node_id, keys) => self.split_leaf_node(node_id, keys),
		};
		let parent_node = match parent_node {
			Node::Internal(node_id, mut keys, mut children) => {
				keys.insert(median_key.clone(), median_value);
				children.insert(idx, right_node.id());
				Self::update_internal_node(node_id, keys, children)
			}
			Node::Leaf(node_id, mut keys) => {
				keys.insert(median_key.clone(), median_value);
				Self::update_leaf_node(node_id, keys)
			}
		};
		// Save the mutated split child with half the (lower) keys
		let left_node = StoredNode::write(kv, left_node).node;
		// Save the new child with half the (upper) keys
		let right_node = StoredNode::write(kv, right_node).node;
		// Save the parent node
		let parent_node = StoredNode::write(kv, parent_node).node;
		SplitResult {
			parent_node,
			left_node,
			right_node,
			median_key,
		}
	}

	fn split_internal_node(
		&mut self,
		node_id: NodeId,
		keys: FstMap,
		mut left_children: Vec<NodeId>,
	) -> (Node, Node, Vec<u8>, u64) {
		let (n, left_keys, median_key, median_value, right_keys) = keys.split_keys();
		let right_children = left_children.split_off(n);
		let left_node = Self::update_internal_node(node_id, left_keys, left_children);
		let right_node = self.new_internal_node(right_keys, right_children);
		(left_node, right_node, median_key, median_value)
	}

	fn split_leaf_node(&mut self, node_id: NodeId, keys: FstMap) -> (Node, Node, Vec<u8>, u64) {
		let (_, left_keys, median_key, median_value, right_keys) = keys.split_keys();
		let left_node = Self::update_leaf_node(node_id, left_keys);
		let right_node = self.new_leaf_node(right_keys);
		(left_node, right_node, median_key, median_value)
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.next_node_id;
		self.next_node_id += 1;
		new_node_id
	}

	fn new_internal_node(&mut self, keys: FstMap, children: Vec<NodeId>) -> Node {
		Node::Internal(self.new_node_id(), keys, children)
	}

	fn update_internal_node(id: NodeId, keys: FstMap, children: Vec<NodeId>) -> Node {
		Node::Internal(id, keys, children)
	}

	fn new_leaf_node(&mut self, keys: FstMap) -> Node {
		Node::Leaf(self.new_node_id(), keys)
	}

	fn update_leaf_node(id: NodeId, keys: FstMap) -> Node {
		Node::Leaf(id, keys)
	}

	pub(super) fn debug(&self, kv: &mut KVSimulator) {
		if let Some(root_id) = &self.root {
			self.recursive_debug(kv, 0, root_id);
		}
	}

	fn recursive_debug(&self, kv: &mut KVSimulator, depth: usize, node_id: &NodeId) {
		match StoredNode::read(kv, node_id).node {
			Node::Internal(node_id, keys, children) => {
				println!("{} -> INTERNAL({}) => {{{}}}", depth, node_id, keys);
				for children_id in children {
					self.recursive_debug(kv, depth + 1, &children_id);
				}
			}
			Node::Leaf(node_id, keys) => {
				println!("{} -> LEAF({}) => {{{}}}", depth, node_id, keys);
			}
		}
	}
}

struct StoredNode {
	node: Node,
	size: usize,
}

impl StoredNode {
	fn read(kv: &mut KVSimulator, node_id: &NodeId) -> Self {
		let (size, node) = kv.get(&node_id.to_be_bytes().to_vec()).unwrap();
		Self {
			size,
			node,
		}
	}

	fn write(kv: &mut KVSimulator, mut node: Node) -> Self {
		node.keys_mut().rebuild();
		let node_id = node.id();
		let size = kv.set(node_id.to_be_bytes().to_vec(), &node);
		println!("Save {} - size: {}", node_id, size);
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
	use crate::idx::btree::BTree;
	use crate::idx::kvsim::KVSimulator;

	#[test]
	fn test_btree_correctness() {
		let mut kv = KVSimulator::default();
		let mut t = BTree::new(75);

		let samples: Vec<(Vec<u8>, u64)> =
			(0..10i32).map(|k| (k.to_be_bytes().to_vec(), (k * 10) as u64)).collect();

		for (k, v) in &samples {
			t.insert(&mut kv, k.clone(), *v);
		}

		for (k, v) in &samples {
			assert_eq!(t.search(&mut kv, k), Some(*v));
		}
	}
}
