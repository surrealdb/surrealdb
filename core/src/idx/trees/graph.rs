use crate::idx::trees::dynamicset::DynamicSet;
use ahash::HashMap;
#[cfg(test)]
use ahash::HashSet;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug)]
pub(super) struct UndirectedGraph<T, S>
where
	T: Eq + Hash + Clone + Copy + Default + 'static + Send + Sync,
	S: DynamicSet<T>,
{
	capacity: usize,
	nodes: HashMap<T, S>,
}

impl<T, S> UndirectedGraph<T, S>
where
	T: Eq + Hash + Clone + Copy + Default + 'static + Send + Sync,
	S: DynamicSet<T>,
{
	pub(super) fn new(capacity: usize) -> Self {
		Self {
			capacity,
			nodes: HashMap::default(),
		}
	}

	#[inline]
	pub(super) fn new_edges(&self) -> S {
		S::with_capacity(self.capacity)
	}

	#[inline]
	pub(super) fn get_edges(&self, node: &T) -> Option<&S> {
		self.nodes.get(node)
	}

	pub(super) fn add_empty_node(&mut self, node: T) -> bool {
		if let Entry::Vacant(e) = self.nodes.entry(node) {
			e.insert(S::with_capacity(self.capacity));
			true
		} else {
			false
		}
	}

	pub(super) fn add_node_and_bidirectional_edges(&mut self, node: T, edges: S) -> Vec<T> {
		let mut r = Vec::with_capacity(edges.len());
		for &e in edges.iter() {
			self.nodes.entry(e).or_insert_with(|| S::with_capacity(self.capacity)).insert(node);
			r.push(e);
		}
		self.nodes.insert(node, edges);
		r
	}
	#[inline]
	pub(super) fn set_node(&mut self, node: T, new_edges: S) {
		self.nodes.insert(node, new_edges);
	}

	pub(super) fn remove_node_and_bidirectional_edges(&mut self, node: &T) -> Option<S> {
		if let Some(edges) = self.nodes.remove(node) {
			for edge in edges.iter() {
				if let Some(edges_to_node) = self.nodes.get_mut(edge) {
					edges_to_node.remove(node);
				}
			}
			Some(edges)
		} else {
			None
		}
	}
}

#[cfg(test)]
impl<T, S> UndirectedGraph<T, S>
where
	T: Eq + Hash + Clone + Copy + Default + 'static + Debug + Send + Sync,
	S: DynamicSet<T>,
{
	pub(in crate::idx::trees) fn len(&self) -> usize {
		self.nodes.len()
	}

	pub(in crate::idx::trees) fn nodes(&self) -> &HashMap<T, S> {
		&self.nodes
	}
	pub(in crate::idx::trees) fn check(&self, g: Vec<(T, Vec<T>)>) {
		for (n, e) in g {
			let edges: HashSet<T> = e.into_iter().collect();
			let n_edges: Option<HashSet<T>> =
				self.get_edges(&n).map(|e| e.iter().cloned().collect());
			assert_eq!(n_edges, Some(edges), "{n:?}");
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::trees::dynamicset::{AHashSet, ArraySet, DynamicSet};
	use crate::idx::trees::graph::UndirectedGraph;

	fn test_undirected_graph<S: DynamicSet<i32>>(m_max: usize) {
		// Graph creation
		let mut g = UndirectedGraph::<i32, S>::new(m_max);
		assert_eq!(g.capacity, 10);

		// Adding an empty node
		let res = g.add_empty_node(0);
		assert!(res);
		g.check(vec![(0, vec![])]);

		// Adding the same node
		let res = g.add_empty_node(0);
		assert!(!res);
		g.check(vec![(0, vec![])]);

		// Adding a node with one edge
		let mut e = g.new_edges();
		e.insert(0);
		let res = g.add_node_and_bidirectional_edges(1, e);
		assert_eq!(res, vec![0]);
		g.check(vec![(0, vec![1]), (1, vec![0])]);

		// Adding a node with two edges
		let mut e = g.new_edges();
		e.insert(0);
		e.insert(1);
		let mut res = g.add_node_and_bidirectional_edges(2, e);
		res.sort();
		assert_eq!(res, vec![0, 1]);
		g.check(vec![(0, vec![1, 2]), (1, vec![0, 2]), (2, vec![0, 1])]);

		// Adding a node with two edges
		let mut e = g.new_edges();
		e.insert(1);
		e.insert(2);
		let mut res = g.add_node_and_bidirectional_edges(3, e);
		res.sort();
		assert_eq!(res, vec![1, 2]);
		g.check(vec![(0, vec![1, 2]), (1, vec![0, 2, 3]), (2, vec![0, 1, 3]), (3, vec![1, 2])]);

		// Change the edges of a node
		let mut e = g.new_edges();
		e.insert(0);
		g.set_node(3, e);
		g.check(vec![(0, vec![1, 2]), (1, vec![0, 2, 3]), (2, vec![0, 1, 3]), (3, vec![0])]);

		// Remove a node
		let res = g.remove_node_and_bidirectional_edges(&2);
		assert_eq!(
			res.map(|v| {
				let mut v: Vec<i32> = v.iter().cloned().collect();
				v.sort();
				v
			}),
			Some(vec![0, 1, 3])
		);
		g.check(vec![(0, vec![1]), (1, vec![0, 3]), (3, vec![0])]);

		// Remove again
		let res = g.remove_node_and_bidirectional_edges(&2);
		assert!(res.is_none());

		// Set a non existing node
		let mut e = g.new_edges();
		e.insert(1);
		g.set_node(2, e);
		g.check(vec![(0, vec![1]), (1, vec![0, 3]), (2, vec![1]), (3, vec![0])]);
	}

	#[test]
	fn test_undirected_graph_array() {
		test_undirected_graph::<ArraySet<i32, 10>>(10);
	}

	#[test]
	fn test_undirected_graph_hash() {
		test_undirected_graph::<AHashSet<i32>>(10);
	}
}
