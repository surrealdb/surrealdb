use crate::idx::fstmap::FstMap;
use crate::idx::kvsim::KVSimulator;
use fst::map::Stream;
use fst::{MapBuilder, Streamer};
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
	Internal(NodeId, Vec<(Key, NodeId)>),
	Leaf(NodeId, FstMap),
}

impl Node {
	fn id(&self) -> NodeId {
		match &self {
			Node::Internal(id, _) => *id,
			Node::Leaf(id, _) => *id,
		}
	}
}

impl BTree {
	pub fn new(order: usize) -> Self {
		Self {
			order,
			root: None,
			next_node_id: 0,
		}
	}

	pub(super) fn search(&self, kv: &mut KVSimulator, key: &Vec<u8>) -> Option<u64> {
		if let Some(root_id) = &self.root {
			self.recursive_search(kv, root_id, key)
		} else {
			None
		}
	}

	fn recursive_search(&self, kv: &mut KVSimulator, node_id: &NodeId, key: &Key) -> Option<Value> {
		match StoredNode::load(kv, node_id).node {
			Node::Internal(_, children) => {
				let mut node_id = Self::find_equal_or_less(&children, key);
				if node_id.is_none() {
					node_id = children.last().map(|(_, id)| *id);
				}
				if let Some(node_id) = node_id {
					self.recursive_search(kv, &node_id, key)
				} else {
					None
				}
			}
			Node::Leaf(_, keys) => keys.get(&key),
		}
	}

	fn find_equal_or_less(children: &Vec<(Key, NodeId)>, search_key: &Key) -> Option<NodeId> {
		let mut last_node_id = None;
		for (key, node_id) in children {
			if key.gt(search_key) {
				break;
			}
			last_node_id = Some(node_id);
		}
		last_node_id.copied()
	}

	pub(super) fn insert(&mut self, kv: &mut KVSimulator, key: Key, value: Value) {
		if let Some(root_id) = &self.root {
			let root = StoredNode::load(kv, root_id);
			if root.is_full(self.order * 2) {
				let new_root = self.split_child(kv, root.node);
				self.insert_non_full(kv, new_root.node, key, value);
			} else {
				self.insert_non_full(kv, root.node, key, value);
			}
		} else {
			let new_root = self.new_leaf_node(FstMap::with_key_value(key, value).unwrap());
			self.root = Some(new_root.id());
			StoredNode::save(kv, new_root);
		}
	}

	fn insert_non_full(&mut self, kv: &mut KVSimulator, mut node: Node, key: Key, value: Value) {
		match &mut node {
			Node::Leaf(_, keys) => {
				keys.insert(key, value);
				StoredNode::save(kv, node);
			}
			Node::Internal(_, _children) => {
				todo!()
			}
		}
	}

	fn split_child(&mut self, kv: &mut KVSimulator, node_to_split: Node) -> StoredNode {
		match node_to_split {
			Node::Internal(id, children) => self.split_internal_node(kv, id, children),
			Node::Leaf(id, keys) => self.split_leaf_node(kv, id, keys),
		}
	}

	fn split_internal_node(
		&mut self,
		kv: &mut KVSimulator,
		node_id: NodeId,
		mut children: Vec<(Key, NodeId)>,
	) -> StoredNode {
		// Do the split by mutating the pre-existing node
		let right_children = children.split_off(children.len() / 2);

		// Extract the higher keys
		let left_key = children.last().map(|(k, _)| k.to_vec()).unwrap();
		let right_key = right_children.last().map(|(k, _)| k.to_vec()).unwrap();

		// Create the new child node containing the extracted half (upper) keys
		let right_node = self.new_internal_node(right_children);

		// Create the children for the new parent node
		let parent_children = vec![(left_key, node_id), (right_key, right_node.id())];

		// Save the mutated split child with half the (lower) keys
		StoredNode::save(kv, Self::update_internal_node(node_id, children));
		// Save the new child with half the (upper) keys
		StoredNode::save(kv, right_node);
		// Save the new parent
		StoredNode::save(kv, self.new_internal_node(parent_children))
	}

	fn split_leaf_node(
		&mut self,
		kv: &mut KVSimulator,
		node_id: NodeId,
		mut keys: FstMap,
	) -> StoredNode {
		// Define the position of the middle key
		let node_len = keys.len();
		let middle_key_nth = node_len / 2;
		let mut stream = keys.stream();

		// Extract the left (lower) keys
		let (left_key, left_map) = Self::map_extraction_loop(&mut stream, 0, middle_key_nth);

		// Extract the right (upper) keys
		let (right_key, right_map) =
			Self::map_extraction_loop(&mut stream, middle_key_nth, node_len);

		let right_node = self.new_leaf_node(right_map);

		// Create the children for the new parent node
		let parent_children = vec![(left_key, node_id), (right_key, right_node.id())];

		// Save the mutated split child with half the (lower) keys
		StoredNode::save(kv, Self::update_leaf_node(node_id, left_map));
		// Save the new child with half the (upper) keys
		StoredNode::save(kv, right_node);
		// Save the new parent
		StoredNode::save(kv, self.new_internal_node(parent_children))
	}

	fn map_extraction_loop(stream: &mut Stream, lower: usize, upper: usize) -> (Key, FstMap) {
		let mut builder = MapBuilder::memory();
		let last = upper - 1;
		for i in lower..upper {
			if let Some((key, value)) = stream.next() {
				builder.insert(key, value).unwrap();
				if i == last {
					// This is the last key
					return (key.to_vec(), FstMap::try_from(builder).unwrap());
				}
			} else {
				panic!("");
			}
		}
		panic!("")
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.next_node_id;
		self.next_node_id += 1;
		new_node_id
	}

	fn new_internal_node(&mut self, children: Vec<(Key, NodeId)>) -> Node {
		Node::Internal(self.new_node_id(), children)
	}

	fn update_internal_node(id: NodeId, children: Vec<(Key, NodeId)>) -> Node {
		Node::Internal(id, children)
	}

	fn new_leaf_node(&mut self, keys: FstMap) -> Node {
		Node::Leaf(self.new_node_id(), keys)
	}

	fn update_leaf_node(id: NodeId, keys: FstMap) -> Node {
		Node::Leaf(id, keys)
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
			Node::Internal(id, _) => *id,
			Node::Leaf(id, keys) => {
				keys.rebuild();
				*id
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
