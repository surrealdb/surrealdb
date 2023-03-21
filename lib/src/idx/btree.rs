use crate::idx::fstmap::FstMap;
use crate::idx::kvsim::KVSimulator;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(super) type NodeId = u64;
pub(super) type Key = Vec<u8>;
pub(super) type Value = u64;

pub(super) struct BTree {
	order: usize,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
enum Node {
	Internal(NodeId, BTreeMap<Key, NodeId>),
	Leaf(NodeId, FstMap),
}

impl BTree {
	pub fn new(order: usize) -> Self {
		Self {
			order,
			root: None,
			next_node_id: 0,
		}
	}

	fn search(&self, kv: &mut KVSimulator, key: &Vec<u8>) -> Option<u64> {
		if let Some(root_id) = &self.root {
			self.b_tree_search(kv, root_id, key)
		} else {
			None
		}
	}

	fn b_tree_search(&self, kv: &mut KVSimulator, node_id: &NodeId, key: &Key) -> Option<Value> {
		match StoredNode::load(kv, node_id).node {
			Node::Internal(_, children) => {
				let mut node_id = Self::find_equal_or_less(&children, key);
				if node_id.is_none() {
					node_id = children.values().last().copied();
				}
				if let Some(node_id) = node_id {
					self.b_tree_search(kv, &node_id, key)
				} else {
					None
				}
			}
			Node::Leaf(_, values) => values.get(&key),
		}
	}

	fn find_equal_or_less(children: &BTreeMap<Key, NodeId>, key: &Key) -> Option<NodeId> {
		if let Some(node_id) = children.get(key) {
			Some(*node_id)
		} else {
			children.range(..key.clone()).next_back().map(|(_, value)| *value)
		}
	}

	pub(super) fn insert(&mut self, kv: &mut KVSimulator, key: Key, value: Value) {
		if let Some(root_id) = &self.root {
			let root = StoredNode::load(kv, root_id);
			if root.is_full(self.order * 2) {
				todo!()
			} else {
				self.insert_non_full(kv, root.node, key, value);
			}
		} else {
			let mut new_map = FstMap::default();
			new_map.insert(key, value);
			StoredNode::save(kv, Node::Leaf(self.next_node_id, new_map));
			self.root = Some(self.next_node_id);
			self.next_node_id += 1;
		}
	}

	fn insert_non_full(&mut self, kv: &mut KVSimulator, mut node: Node, key: Key, value: Value) {
		match &mut node {
			Node::Leaf(_, fst_map) => {
				fst_map.insert(key, value);
				StoredNode::save(kv, node);
			}
			Node::Internal(_, _) => {
				todo!()
			}
		}
	}
}

struct StoredNode {
	node: Node,
	size: usize,
}

impl StoredNode {
	fn load(kv: &mut KVSimulator, node_id: &NodeId) -> Self {
		let (size, node) = kv.get(&node_id.to_be_bytes().to_vec()).unwrap();
		Self {
			size,
			node,
		}
	}

	fn save(kv: &mut KVSimulator, mut node: Node) -> Self {
		let node_id = match &mut node {
			Node::Internal(node_id, _) => *node_id,
			Node::Leaf(node_id, fst_map) => {
				fst_map.rebuild();
				*node_id
			}
		};
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
