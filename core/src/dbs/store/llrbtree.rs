struct Node {
	key: usize,
	value: usize,
	left: Option<Box<Node>>,
	right: Option<Box<Node>>,
	is_red: bool,
}

/// This is an implementation of a Left-leaning red–black tree.
/// It is a red–black tree, a self-balancing binary search tree data structure
/// noted for fast insertion and retrieval of ordered information.
/// https://en.wikipedia.org/wiki/Left-leaning_red%E2%80%93black_tree
pub(super) struct LLRBTree {
	root: Option<Box<Node>>,
}

impl LLRBTree {
	pub(super) fn new() -> Self {
		LLRBTree {
			root: None,
		}
	}

	pub(super) fn insert<C>(&mut self, key: usize, value: usize, cmp: C)
	where
		C: Fn(usize, usize) -> Ordering,
	{
		self.root = insert(self.root.take(), key, value, cmp);
		if let Some(ref mut root) = self.root {
			root.is_red = false;
		}
	}
}

use std::cmp::Ordering;

fn is_red(node: &Option<Box<Node>>) -> bool {
	node.as_ref().map_or(false, |n| n.is_red)
}

fn rotate_left(mut h: Box<Node>) -> Box<Node> {
	let mut x = h.right.take().unwrap();
	h.right = x.left.take();
	x.left = Some(h);
	x.is_red = x.left.as_ref().unwrap().is_red;
	x.left.as_mut().unwrap().is_red = true;
	x
}

fn rotate_right(mut h: Box<Node>) -> Box<Node> {
	let mut x = h.left.take().unwrap();
	h.left = x.right.take();
	x.right = Some(h);
	x.is_red = x.right.as_ref().unwrap().is_red;
	x.right.as_mut().unwrap().is_red = true;
	x
}

fn flip_colors(h: &mut Node) {
	h.is_red = !h.is_red;
	if let Some(ref mut left) = h.left {
		left.is_red = !left.is_red;
	}
	if let Some(ref mut right) = h.right {
		right.is_red = !right.is_red;
	}
}

fn insert<C>(h: Option<Box<Node>>, key: usize, value: usize, cmp: C) -> Option<Box<Node>>
where
	C: Fn(usize, usize) -> Ordering,
{
	let mut h = match h {
		Some(node) => node,
		None => {
			return Some(Box::new(Node {
				key,
				value,
				left: None,
				right: None,
				is_red: true,
			}));
		}
	};

	match cmp(key, h.key) {
		Ordering::Less => h.left = insert(h.left, key, value, cmp),
		Ordering::Equal => h.value = value,
		Ordering::Greater => h.right = insert(h.right, key, value, cmp),
	};

	// Fix right-leaning reds on the way up
	if is_red(&h.right) && !is_red(&h.left) {
		h = rotate_left(h);
	}
	if is_red(&h.left) && is_red(&h.left.as_ref().unwrap().left) {
		h = rotate_right(h);
	}
	if is_red(&h.left) && is_red(&h.right) {
		flip_colors(&mut h);
	}

	Some(h)
}
// Consuming iterator implementation
pub struct IntoIter {
	stack: Vec<Node>,
}

impl IntoIter {
	fn new(root: Option<Box<Node>>) -> Self {
		let mut stack = Vec::with_capacity(16);
		let mut current = root;

		// Push all the left children onto the stack
		while let Some(mut node) = current {
			current = node.left.take();
			stack.push(*node);
		}

		Self {
			stack,
		}
	}
}

impl Iterator for IntoIter {
	type Item = (usize, usize);

	fn next(&mut self) -> Option<Self::Item> {
		// Pop the top node from the stack
		let mut node = self.stack.pop()?;

		// Save the key and value to return
		let key = node.key;
		let value = node.value;

		// If the node has a right child, push all its left children onto the stack
		let mut current = node.right.take();
		while let Some(mut n) = current {
			current = n.left.take();
			self.stack.push(*n);
		}

		Some((key, value))
	}
}

impl IntoIterator for LLRBTree {
	type Item = (usize, usize);
	type IntoIter = IntoIter;

	fn into_iter(self) -> Self::IntoIter {
		IntoIter::new(self.root)
	}
}

#[cfg(test)]
mod test {
	use crate::dbs::store::llrbtree::LLRBTree;

	#[test]
	fn llrbtree() {
		let mut tree = LLRBTree::new();
		let cmp = |a: usize, b: usize| a.cmp(&b);
		tree.insert(5, 50, cmp);
		tree.insert(2, 20, cmp);
		tree.insert(4, 40, cmp);
		tree.insert(3, 30, cmp);
		tree.insert(1, 10, cmp);

		let result = tree.into_iter().collect::<Vec<_>>();
		assert_eq!(result, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);
	}
}
