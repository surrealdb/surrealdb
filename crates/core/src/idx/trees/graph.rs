use std::collections::hash_map::Entry;
use std::fmt::Debug;

use ahash::HashMap;
#[cfg(test)]
use ahash::HashSet;
use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};

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

	pub(super) fn to_val(&self) -> Result<BytesMut> {
		let mut buf = BytesMut::new();
		buf.put_u32(self.nodes.len() as u32);
		for (&e, s) in &self.nodes {
			buf.put_u64(e);
			buf.put_u16(s.len() as u16);
			for &i in s.iter() {
				buf.put_u64(i);
			}
		}
		Ok(buf)
	}

	pub(super) fn reload(&mut self, val: &[u8]) -> Result<()> {
		let mut buf = BytesMut::from(val);
		self.nodes.clear();
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
}
