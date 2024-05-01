use crate::idx::trees::dynamicset::{DynamicSet, DynamicSetImpl};
use crate::idx::trees::hnsw::ElementId;
use std::collections::hash_map::Entry as HEntry;
use std::collections::HashMap;

pub(super) struct UndirectedGraph {
	m_max: usize,
	nodes: HashMap<ElementId, DynamicSet<ElementId>>,
}

impl UndirectedGraph {
	pub(super) fn new(m_max: usize) -> Self {
		Self {
			m_max,
			nodes: HashMap::new(),
		}
	}

	#[inline]
	pub(super) fn get_edges(&self, node: &ElementId) -> Option<&DynamicSet<ElementId>> {
		self.nodes.get(node)
	}

	pub(super) fn add_empty_node(&mut self, node: ElementId) -> bool {
		if let HEntry::Vacant(e) = self.nodes.entry(node) {
			e.insert(DynamicSet::with_capacity(self.m_max));
			true
		} else {
			false
		}
	}

	pub(super) fn add_node_and_bidirectional_edges(
		&mut self,
		node: ElementId,
		edges: DynamicSet<ElementId>,
	) -> Vec<ElementId> {
		let mut r = Vec::with_capacity(edges.len());
		for &e in edges.iter() {
			self.nodes
				.entry(e)
				.or_insert_with(|| DynamicSet::with_capacity(self.m_max))
				.insert(node);
			r.push(e);
		}
		self.nodes.insert(node, edges);
		r
	}
	#[inline]
	pub(super) fn set_node(&mut self, node: ElementId, new_edges: DynamicSet<ElementId>) {
		self.nodes.insert(node, new_edges);
	}

	pub(super) fn remove_node_and_bidirectional_edges(
		&mut self,
		node: &ElementId,
	) -> Option<DynamicSet<ElementId>> {
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
mod tests {
	use crate::idx::trees::dynamicset::{DynamicSet, DynamicSetImpl};
	use crate::idx::trees::graph::UndirectedGraph;
	use crate::idx::trees::hnsw::ElementId;
	use std::collections::{HashMap, HashSet};

	impl UndirectedGraph {
		pub(in crate::idx::trees) fn len(&self) -> usize {
			self.nodes.len()
		}

		pub(in crate::idx::trees) fn nodes(&self) -> &HashMap<ElementId, DynamicSet<ElementId>> {
			&self.nodes
		}
		pub(in crate::idx::trees) fn check(&self, g: Vec<(ElementId, Vec<ElementId>)>) {
			for (n, e) in g {
				let edges: HashSet<ElementId> = e.into_iter().collect();
				let n_edges: Option<HashSet<ElementId>> =
					self.get_edges(&n).map(|e| e.iter().cloned().collect());
				assert_eq!(n_edges, Some(edges), "{n}");
			}
		}
	}

	#[test]
	fn test_undirected_graph() {
		// Graph creation
		let mut g = UndirectedGraph::new(10);
		assert_eq!(g.m_max, 10);

		// Adding an empty node
		let res = g.add_empty_node(0);
		assert!(res);
		g.check(vec![(0, vec![])]);

		// Adding the same node
		let res = g.add_empty_node(0);
		assert!(!res);
		g.check(vec![(0, vec![])]);

		// Adding a node with one edge
		let mut e = DynamicSet::with_capacity(g.m_max);
		e.insert(0);
		let res = g.add_node_and_bidirectional_edges(1, e);
		assert_eq!(res, vec![0]);
		g.check(vec![(0, vec![1]), (1, vec![0])]);

		// Adding a node with two edges
		let mut e = DynamicSet::with_capacity(g.m_max);
		e.insert(0);
		e.insert(1);
		let mut res = g.add_node_and_bidirectional_edges(2, e);
		res.sort();
		assert_eq!(res, vec![0, 1]);
		g.check(vec![(0, vec![1, 2]), (1, vec![0, 2]), (2, vec![0, 1])]);

		// Adding a node with two edges
		let mut e = DynamicSet::with_capacity(g.m_max);
		e.insert(1);
		e.insert(2);
		let mut res = g.add_node_and_bidirectional_edges(3, e);
		res.sort();
		assert_eq!(res, vec![1, 2]);
		g.check(vec![(0, vec![1, 2]), (1, vec![0, 2, 3]), (2, vec![0, 1, 3]), (3, vec![1, 2])]);

		// Change the edges of a node
		let mut e = DynamicSet::with_capacity(g.m_max);
		e.insert(0);
		g.set_node(3, e);
		g.check(vec![(0, vec![1, 2]), (1, vec![0, 2, 3]), (2, vec![0, 1, 3]), (3, vec![0])]);

		// Remove a node
		let res = g.remove_node_and_bidirectional_edges(&2);
		assert_eq!(
			res.map(|v| {
				let mut v: Vec<ElementId> = v.iter().cloned().collect();
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
		let mut e = DynamicSet::with_capacity(g.m_max);
		e.insert(1);
		g.set_node(2, e);
		g.check(vec![(0, vec![1]), (1, vec![0, 3]), (2, vec![1]), (3, vec![0])]);
	}
}
