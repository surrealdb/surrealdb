use std::collections::hash_map::Entry;
use std::fmt::Debug;

use ahash::HashMap;
#[cfg(test)]
use ahash::HashSet;
use anyhow::Result;
use bytes::{Buf, BytesMut};

use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::ElementId;

#[derive(Debug)]
pub(super) struct UndirectedGraph<S>
where
	S: DynamicSet,
{
	capacity: usize,
	nodes: HashMap<ElementId, S>,
}

impl<S> UndirectedGraph<S>
where
	S: DynamicSet,
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
	pub(super) fn get_edges(&self, node: &ElementId) -> Option<&S> {
		self.nodes.get(node)
	}

	pub(super) fn add_empty_node(&mut self, node: ElementId) -> bool {
		if let Entry::Vacant(e) = self.nodes.entry(node) {
			e.insert(S::with_capacity(self.capacity));
			true
		} else {
			false
		}
	}

	pub(super) fn add_node_and_bidirectional_edges(
		&mut self,
		node: ElementId,
		edges: S,
	) -> Vec<ElementId> {
		let mut r = Vec::with_capacity(edges.len());
		for &e in edges.iter() {
			self.nodes.entry(e).or_insert_with(|| S::with_capacity(self.capacity)).insert(node);
			r.push(e);
		}
		self.nodes.insert(node, edges);
		r
	}
	#[inline]
	pub(super) fn set_node(&mut self, node: ElementId, new_edges: S) {
		self.nodes.insert(node, new_edges);
	}

	pub(super) fn remove_node_and_bidirectional_edges(&mut self, node: &ElementId) -> Option<S> {
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

	pub(super) fn lecacy_reload(&mut self, val: &[u8]) -> Result<()> {
		self.nodes.clear();
		if val.is_empty() {
			return Ok(());
		}
		let mut buf = BytesMut::from(val);
		let len = buf.get_u32() as usize;
		for _ in 0..len {
			let e = buf.get_u64();
			let s_len = buf.get_u16() as usize;
			let mut s = S::with_capacity(s_len);
			for _ in 0..s_len {
				s.insert(buf.get_u64() as ElementId);
			}
			self.nodes.insert(e, s);
		}
		Ok(())
	}

	/// Serializes a single node's edge list into a byte buffer.
	/// Returns `None` if the node does not exist in the graph.
	pub(super) fn node_to_val(&self, node: &ElementId) -> Option<Vec<u8>> {
		self.nodes.get(node).map(|edges| {
			let mut buf = Vec::with_capacity(2 + edges.len() * 8);
			buf.extend_from_slice(&(edges.len() as u16).to_be_bytes());
			for &e in edges.iter() {
				buf.extend_from_slice(&e.to_be_bytes());
			}
			buf
		})
	}

	/// Deserializes a single node's edge list from a byte buffer
	/// and inserts it into the graph, as produced by [`Self::node_to_val`].
	pub(super) fn load_node(&mut self, node: ElementId, val: &[u8]) {
		let mut buf = val;
		let s_len = (&mut buf).get_u16() as usize;
		let mut s = S::with_capacity(s_len);
		for _ in 0..s_len {
			s.insert((&mut buf).get_u64() as ElementId);
		}
		self.nodes.insert(node, s);
	}

	/// Returns all node IDs currently present in the graph.
	pub(super) fn node_ids(&self) -> Vec<ElementId> {
		self.nodes.keys().copied().collect()
	}

	/// Removes all nodes and edges from the graph.
	pub(super) fn clear(&mut self) {
		self.nodes.clear();
	}
}

#[cfg(test)]
impl<S> UndirectedGraph<S>
where
	S: DynamicSet,
{
	pub(in crate::idx::trees) fn len(&self) -> usize {
		self.nodes.len()
	}

	pub(in crate::idx::trees) fn nodes(&self) -> &HashMap<ElementId, S> {
		&self.nodes
	}
	pub(in crate::idx::trees) fn check(&self, g: Vec<(ElementId, Vec<ElementId>)>) {
		for (n, e) in g {
			let edges: HashSet<ElementId> = e.into_iter().collect();
			let n_edges: Option<HashSet<ElementId>> =
				self.get_edges(&n).map(|e| e.iter().copied().collect());
			assert_eq!(n_edges, Some(edges), "{n:?}");
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::trees::dynamicset::{AHashSet, ArraySet, DynamicSet};
	use crate::idx::trees::graph::UndirectedGraph;
	use crate::idx::trees::hnsw::ElementId;

	fn test_undirected_graph<S: DynamicSet>(m_max: usize) {
		// Graph creation
		let mut g = UndirectedGraph::<S>::new(m_max);
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
				let mut v: Vec<ElementId> = v.iter().copied().collect();
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
		test_undirected_graph::<ArraySet<10>>(10);
	}

	#[test]
	fn test_undirected_graph_hash() {
		test_undirected_graph::<AHashSet>(10);
	}

	/// Test that reload handles empty buffer gracefully.
	/// This can happen when a layer has no stored chunks yet (chunks == 0).
	#[test]
	fn test_reload_empty_buffer() {
		let mut g = UndirectedGraph::<ArraySet<10>>::new(10);

		// Add some data first
		g.add_empty_node(1);
		g.add_empty_node(2);
		assert_eq!(g.len(), 2);

		// Reload with empty buffer should clear the graph, not panic
		g.lecacy_reload(&[]).unwrap();
		assert_eq!(g.len(), 0);
	}

	/// Test that clear handles graph reset gracefully.
	#[test]
	fn test_clear() {
		let mut g = UndirectedGraph::<ArraySet<10>>::new(10);

		// Add some data first
		g.add_empty_node(1);
		g.add_empty_node(2);
		assert_eq!(g.len(), 2);

		// Clear should reset the graph
		g.clear();
		assert_eq!(g.len(), 0);
	}

	/// Test per-node serialization round-trip.
	#[test]
	fn test_node_to_val_and_load_node() {
		let mut g = UndirectedGraph::<ArraySet<10>>::new(10);

		let mut e = ArraySet::<10>::with_capacity(10);
		e.insert(2);
		e.insert(3);
		g.add_node_and_bidirectional_edges(1, e);

		// Serialize node 1
		let val = g.node_to_val(&1).unwrap();

		// Load into a new graph
		let mut g2 = UndirectedGraph::<ArraySet<10>>::new(10);
		g2.load_node(1, &val);

		// Check the loaded node has the same edges
		let edges: Vec<ElementId> = {
			let mut v: Vec<ElementId> = g2.get_edges(&1).unwrap().iter().copied().collect();
			v.sort();
			v
		};
		assert_eq!(edges, vec![2, 3]);
	}
}
