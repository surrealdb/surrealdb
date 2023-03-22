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
				let new_root = self.split_child(kv, new_root, 0, root.node).node;
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
				let child_node = StoredNode::read(kv, &children[idx]);
				let child_node = if child_node.is_full(self.order * 2) {
					let child_node = self.split_child(kv, node, idx, child_node.node).node;
					// TODO get_nth could be calculated by split_child to avoid another iteration
					if key.gt(&node.keys().get_nth(idx).unwrap()) {
						idx += 1;
					};
					child_node
				} else {
					node
				};
				self.insert_non_full(kv, child_node, key, value);
			}
		}
	}

	fn split_child(
		&mut self,
		_kv: &mut KVSimulator,
		_node: Node,
		_idx: usize,
		_child_node: Node,
	) -> StoredNode {
		todo!()
	}

	// fn split_internal_node(
	// 	&mut self,
	// 	kv: &mut KVSimulator,
	// 	node_id: NodeId,
	// 	mut keys: FstMap,
	// 	mut children: Vec<NodeId>,
	// ) -> StoredNode {
	// 	let key_idx = keys.len() / 2;
	//
	// 	// Do the split by mutating the pre-existing node
	// 	let right_children = children.split_off(children.len() / 2);
	//
	// 	// Extract the higher keys
	// 	let left_key = children.last().map(|(k, _)| k.to_vec()).unwrap();
	// 	let right_key = right_children.last().map(|(k, _)| k.to_vec()).unwrap();
	//
	// 	// Create the new child node containing the extracted half (upper) keys
	// 	let right_node = self.new_internal_node(right_children);
	//
	// 	// Create the children for the new parent node
	// 	let parent_children = vec![(left_key, node_id), (right_key, right_node.id())];
	//
	// 	// Save the mutated split child with half the (lower) keys
	// 	StoredNode::write(kv, Self::update_internal_node(node_id, children));
	// 	// Save the new child with half the (upper) keys
	// 	StoredNode::write(kv, right_node);
	// 	// Save the new parent
	// 	StoredNode::write(kv, self.new_internal_node(parent_children))
	// }
	//
	// fn split_leaf_node(
	// 	&mut self,
	// 	kv: &mut KVSimulator,
	// 	node_id: NodeId,
	// 	mut keys: FstMap,
	// ) -> StoredNode {
	// 	// Define the position of the middle key
	// 	let node_len = keys.len();
	// 	let middle_key_nth = node_len / 2;
	// 	let mut stream = keys.stream();
	//
	// 	// Extract the left (lower) keys
	// 	let (left_key, left_map) = Self::map_extraction_loop(&mut stream, 0, middle_key_nth);
	//
	// 	// Extract the right (upper) keys
	// 	let (right_key, right_map) =
	// 		Self::map_extraction_loop(&mut stream, middle_key_nth, node_len);
	//
	// 	let right_node = self.new_leaf_node(right_map);
	//
	// 	// Create the children for the new parent node
	// 	let parent_children = vec![node_id, right_key];
	//
	// 	// Save the mutated split child with half the (lower) keys
	// 	StoredNode::write(kv, Self::update_leaf_node(node_id, left_map));
	// 	// Save the new child with half the (upper) keys
	// 	StoredNode::write(kv, right_node);
	// 	// Save the new parent
	// 	StoredNode::write(kv, self.new_internal_node(parent_children))
	// }

	// fn map_extraction_loop(stream: &mut Stream, lower: usize, upper: usize) -> (Key, FstMap) {
	// 	let mut builder = MapBuilder::memory();
	// 	let last = upper - 1;
	// 	for i in lower..upper {
	// 		if let Some((key, value)) = stream.next() {
	// 			builder.insert(key, value).unwrap();
	// 			if i == last {
	// 				// This is the last key
	// 				return (key.to_vec(), FstMap::try_from(builder).unwrap());
	// 			}
	// 		} else {
	// 			panic!("");
	// 		}
	// 	}
	// 	panic!("")
	// }

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
		let node_id = match &mut node {
			Node::Internal(id, _, _) => *id,
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
