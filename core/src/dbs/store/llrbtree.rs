use std::cmp::Ordering;

/// This is an implementation of a Left-leaning red–black tree.
/// It is a red–black tree, a self-balancing binary search tree data structure
/// noted for fast insertion and retrieval of ordered information.
/// https://en.wikipedia.org/wiki/Left-leaning_red%E2%80%93black_tree
#[derive(Default)]
pub(super) struct LLRBTree {
	root: Option<usize>,
	nodes: Nodes,
}

impl LLRBTree {
	pub(super) fn insert<C>(&mut self, val: usize, cmp: C)
	where
		C: Fn(usize, usize) -> Ordering,
	{
		self.root = self.nodes.insert(self.root.take(), val, cmp);
		if let Some(root) = self.root {
			self.nodes.node_mut(root).is_red = false;
		}
	}
}

#[derive(Default)]
struct Nodes(Vec<Node>);

struct Node {
	val: usize,
	left: Option<usize>,
	right: Option<usize>,
	is_red: bool,
}
impl Nodes {
	fn new_red_node(&mut self, val: usize) -> usize {
		let idx = self.0.len();
		let new_node = Node {
			val,
			left: None,
			right: None,
			is_red: true,
		};
		self.0.push(new_node);
		idx
	}
	fn node(&self, idx: usize) -> &Node {
		&self.0[idx]
	}

	fn node_mut(&mut self, idx: usize) -> &mut Node {
		&mut self.0[idx]
	}

	fn insert<C>(&mut self, h_idx: Option<usize>, val: usize, cmp: C) -> Option<usize>
	where
		C: Fn(usize, usize) -> Ordering,
	{
		let mut h_idx = match h_idx {
			Some(h_idx) => h_idx,
			None => return Some(self.new_red_node(val)),
		};

		match cmp(val, self.node(h_idx).val) {
			Ordering::Less => {
				self.node_mut(h_idx).left = self.insert(self.node(h_idx).left, val, cmp)
			}
			Ordering::Equal => self.node_mut(h_idx).val = val,
			Ordering::Greater => {
				self.node_mut(h_idx).right = self.insert(self.node(h_idx).right, val, cmp)
			}
		};

		// Fix right-leaning reds on the way up
		if self.is_red(self.node(h_idx).right) && !self.is_red(self.node(h_idx).left) {
			h_idx = self.rotate_left(h_idx);
		}

		if self.is_red(self.node(h_idx).left) {
			let h_left_left = self.node(self.node(h_idx).left.unwrap()).left;
			if self.is_red(h_left_left) {
				h_idx = self.rotate_right(h_idx);
			}
		}
		let (l, r) = {
			let h = &self.node(h_idx);
			(h.left, h.right)
		};
		if self.is_red(l) && self.is_red(r) {
			self.flip_colors(h_idx);
		}

		Some(h_idx)
	}
	fn is_red(&self, node_idx: Option<usize>) -> bool {
		node_idx.map_or(false, |idx| self.node(idx).is_red)
	}

	fn rotate_left(&mut self, h_idx: usize) -> usize {
		let x_idx = self.node(h_idx).right.unwrap();

		// Perform rotation
		self.node_mut(h_idx).right = self.node(x_idx).left;
		self.node_mut(x_idx).left = Some(h_idx);

		// Adjust colors
		self.node_mut(x_idx).is_red = self.node(h_idx).is_red;
		self.node_mut(h_idx).is_red = true;

		x_idx
	}

	fn rotate_right(&mut self, h_idx: usize) -> usize {
		let x_idx = self.node(h_idx).left.unwrap();

		// Perform rotation
		self.node_mut(h_idx).left = self.node(x_idx).right;
		self.node_mut(x_idx).right = Some(h_idx);

		// Adjust colors
		self.node_mut(x_idx).is_red = self.node(h_idx).is_red;
		self.node_mut(h_idx).is_red = true;

		x_idx
	}

	fn flip_colors(&mut self, h_idx: usize) {
		self.node_mut(h_idx).is_red = !self.node(h_idx).is_red;
		if let Some(left_idx) = self.node(h_idx).left {
			self.node_mut(left_idx).is_red = !self.node(left_idx).is_red;
		}
		if let Some(right_idx) = self.node(h_idx).right {
			self.node_mut(right_idx).is_red = !self.node(right_idx).is_red;
		}
	}
}

// Consuming iterator implementation
pub struct LLRBTreeIterator {
	nodes: Nodes,
	stack: Vec<usize>,
}

impl LLRBTreeIterator {
	fn new(tree: LLRBTree) -> Self {
		let mut stack = Vec::with_capacity(16);
		let mut current = tree.root;
		let mut nodes = tree.nodes;

		// Push all the left children onto the stack
		while let Some(node_idx) = current {
			let node = nodes.node_mut(node_idx);
			current = node.left.take();
			stack.push(node_idx);
		}

		Self {
			nodes,
			stack,
		}
	}
}

impl Iterator for LLRBTreeIterator {
	type Item = usize;

	fn next(&mut self) -> Option<Self::Item> {
		// Pop the top node from the stack
		let node_idx = self.stack.pop()?;
		let node = self.nodes.node_mut(node_idx);

		// Save value to return
		let val = node.val;

		// If the node has a right child, push all its left children onto the stack
		let mut current = node.right.take();
		while let Some(n_idx) = current {
			let n = self.nodes.node_mut(n_idx);
			current = n.left.take();
			self.stack.push(n_idx);
		}

		Some(val)
	}
}

impl IntoIterator for LLRBTree {
	type Item = usize;
	type IntoIter = LLRBTreeIterator;

	fn into_iter(self) -> Self::IntoIter {
		LLRBTreeIterator::new(self)
	}
}

#[cfg(test)]
mod test {
	use crate::dbs::store::llrbtree::LLRBTree;

	#[test]
	fn insertion() {
		let mut tree = LLRBTree::default();
		let cmp = |a: usize, b: usize| a.cmp(&b);
		tree.insert(5, cmp);
		tree.insert(2, cmp);
		tree.insert(4, cmp);
		tree.insert(3, cmp);
		tree.insert(1, cmp);

		let result = tree.into_iter().collect::<Vec<_>>();
		assert_eq!(result, vec![1, 2, 3, 4, 5]);
	}
}
