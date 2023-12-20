use crate::idx::trees::store::NodeId;
use crate::idx::trees::vector::SharedVector;
use fst::raw::Node;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct HnswGraph {
	nodes: Vec<HnswNode>, // All nodes in the graph.
	max_layer: usize,     // Maximum layer in the graph.
}

struct HnswNode {
	point: SharedVector, // The data point. T could be a type that represents your data.
	neighbors: Vec<Vec<NodeId>>, // Nested Vec, each sub-Vec represents a layer, containing indices of neighbor nodes.
}

impl HnswGraph {
	fn new(max_layer: usize) -> Self {
		HnswGraph {
			nodes: Vec::new(),
			max_layer,
		}
	}

	fn add_node(&mut self, point: SharedVector) {
		let node_layer = self.get_random_layer(self.max_layer);
		let new_node_index = self.nodes.len() as NodeId;

		let node = HnswNode {
			point,
			neighbors: vec![Vec::new(); self.max_layer + 1], // Initialize neighbors for each layer.
		};
		self.nodes.push(node);
		// Further logic to connect the node in the graph goes here.

		// Connect the node to the graph
		for layer in (0..=node_layer).rev() {
			self.connect_node(new_node_index, layer);
		}
	}

	fn connect_node(&mut self, node_index: NodeId, layer: usize) {
		// Implement the logic to find the nearest neighbors at the given layer.
		// This can be a complex part, as it involves searching the graph.
		// Update the neighbors of the node and its neighbors as well.
	}

	pub fn search(&self, query_point: &SharedVector, k: usize) -> Vec<usize> {
		let mut current_layer = self.max_layer;
		let mut entry_point_index = 0; // Assuming you start with an arbitrary node or a predefined entry point

		while current_layer > 0 {
			entry_point_index = self.search_layer(query_point, entry_point_index, current_layer);
			current_layer -= 1;
		}

		self.find_nearest_neighbors(query_point, entry_point_index, k)
	}

	fn search_layer(
		&self,
		query_point: &SharedVector,
		entry_point_index: usize,
		layer: usize,
	) -> usize {
		let mut closest_node = entry_point_index;
		let mut closest_distance = euclidean_distance(query_point, &self.nodes[closest_node].point);

		loop {
			let mut changed = false;
			for &neighbor_index in &self.nodes[closest_node].neighbors[layer] {
				let distance = euclidean_distance(query_point, &self.nodes[neighbor_index].point);
				if distance < closest_distance {
					closest_node = neighbor_index;
					closest_distance = distance;
					changed = true;
				}
			}

			if !changed {
				break;
			}
		}

		closest_node
	}

	fn find_nearest_neighbors(
		&self,
		query_point: &SharedVector,
		entry_point_index: usize,
		k: usize,
	) -> Vec<usize> {
		let mut visited = vec![false; self.nodes.len()];
		let mut heap = BinaryHeap::new();

		heap.push(ReverseOrder((0.0, entry_point_index)));

		while let Some(ReverseOrder((distance, index))) = heap.pop() {
			if visited[index] {
				continue;
			}

			visited[index] = true;
			if heap.len() > k {
				break;
			}

			for &neighbor_index in &self.nodes[index].neighbors[0] {
				if !visited[neighbor_index] {
					let neighbor_distance =
						euclidean_distance(query_point, &self.nodes[neighbor_index].point);
					heap.push(ReverseOrder((neighbor_distance, neighbor_index)));
				}
			}
		}

		heap.into_iter().map(|ReverseOrder((_, index))| index).collect()
	}
}

#[derive(Eq)]
struct ReverseOrder<T>(T);

impl<T: PartialEq> PartialEq for ReverseOrder<T> {
	fn eq(&self, other: &Self) -> bool {
		self.0.eq(&other.0)
	}
}
impl<T: Ord> PartialOrd for ReverseOrder<T> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		other.0.partial_cmp(&self.0) // Reverse the order
	}
}

impl<T: Ord> Ord for ReverseOrder<T> {
	fn cmp(&self, other: &Self) -> Ordering {
		other.0.cmp(&self.0) // Reverse the order
	}
}

impl<T: Eq> Eq for ReverseOrder<T> {}
