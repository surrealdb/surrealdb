use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::knn::PriorityNode;
use crate::idx::trees::store::NodeId;
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::Distance;
use roaring::RoaringTreemap;
use std::collections::BinaryHeap;

struct HnswGraph {
	nodes: Vec<HnswNode>, // All nodes in the graph.
	max_layer: usize,     // Maximum layer in the graph.
	distance: Distance,
}

struct HnswNode {
	point: SharedVector, // The data point. T could be a type that represents your data.
	docs: RoaringTreemap,
	neighbors: Vec<Vec<NodeId>>, // Nested Vec, each sub-Vec represents a layer, containing indices of neighbor nodes.
}

impl HnswGraph {
	fn new(max_layer: usize, distance: Distance) -> Self {
		HnswGraph {
			nodes: Vec::new(),
			max_layer,
			distance,
		}
	}

	fn get_random_layer(&self, _max_layer: usize) -> Result<usize, Error> {
		todo!()
	}

	fn add_node(&mut self, point: SharedVector, doc_id: DocId) -> Result<(), Error> {
		let node_layer = self.get_random_layer(self.max_layer)?;
		let new_node_index = self.nodes.len() as NodeId;

		let node = HnswNode {
			point,
			docs: RoaringTreemap::from([doc_id]),
			neighbors: vec![Vec::new(); self.max_layer + 1], // Initialize neighbors for each layer.
		};
		self.nodes.push(node);
		// Further logic to connect the node in the graph goes here.

		// Connect the node to the graph
		for layer in (0..=node_layer).rev() {
			self.connect_node(new_node_index, layer);
		}
		Ok(())
	}

	fn connect_node(&mut self, _node_index: NodeId, _layer: usize) {
		// Implement the logic to find the nearest neighbors at the given layer.
		// This can be a complex part, as it involves searching the graph.
		// Update the neighbors of the node and its neighbors as well.
	}

	pub fn search(&self, query_point: &SharedVector, k: usize) -> Result<Vec<NodeId>, Error> {
		let mut current_layer = self.max_layer;
		let mut entry_point_index = 0; // Assuming you start with an arbitrary node or a predefined entry point

		while current_layer > 0 {
			entry_point_index = self.search_layer(query_point, entry_point_index, current_layer)?;
			current_layer -= 1;
		}

		self.find_nearest_neighbors(query_point, entry_point_index, k)
	}

	fn search_layer(
		&self,
		query_point: &SharedVector,
		entry_point_index: NodeId,
		layer: usize,
	) -> Result<NodeId, Error> {
		let mut closest_node = entry_point_index as usize;
		let mut closest_distance =
			self.distance.compute(query_point, &self.nodes[closest_node].point)?;

		loop {
			let mut changed = false;
			for &neighbor_index in &self.nodes[closest_node].neighbors[layer] {
				let neighbor_index = neighbor_index as usize;
				let distance =
					self.distance.compute(query_point, &self.nodes[neighbor_index].point)?;
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

		Ok(closest_node as NodeId)
	}

	fn find_nearest_neighbors(
		&self,
		query_point: &SharedVector,
		entry_point_index: NodeId,
		k: usize,
	) -> Result<Vec<NodeId>, Error> {
		let mut visited = RoaringTreemap::new();
		let mut heap = BinaryHeap::new();

		heap.push(PriorityNode::new(0.0, entry_point_index));

		while let Some(pn) = heap.pop() {
			let node_id = pn.node_id();
			if visited.contains(node_id) {
				continue;
			}

			visited.insert(node_id);
			if heap.len() > k {
				break;
			}

			for &neighbor_index in &self.nodes[node_id as usize].neighbors[0] {
				if !visited.contains(neighbor_index) {
					let neighbor_distance = self
						.distance
						.compute(query_point, &self.nodes[neighbor_index as usize].point)?;
					heap.push(PriorityNode::new(neighbor_distance, neighbor_index));
				}
			}
		}

		Ok(heap.into_iter().map(|pn| pn.node_id()).collect())
	}
}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::docids::DocId;
	use crate::idx::trees::hnsw::HnswGraph;
	use crate::idx::trees::knn::tests::TestCollection;
	use crate::idx::trees::vector::SharedVector;
	use crate::sql::index::{Distance, VectorType};
	use std::collections::HashMap;

	fn insert_collection_one_by_one(
		h: &mut HnswGraph,
		collection: &TestCollection,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			h.add_node(obj.clone(), *doc_id)?;
			map.insert(*doc_id, obj.clone());
		}
		Ok(map)
	}

	fn find_collection(h: &mut HnswGraph, collection: &TestCollection) -> Result<(), Error> {
		let max_knn = 20.max(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.search(obj, knn)?;
				if collection.is_unique() {
					assert!(
						res.contains(doc_id),
						"Search: {:?} - Knn: {} - Wrong Doc - Expected: {} - Got: {:?}",
						obj,
						knn,
						doc_id,
						res
					);
				}
				let expected_len = collection.as_ref().len().min(knn);
				assert_eq!(
					expected_len,
					res.len(),
					"Wrong knn count - Expected: {} - Got: {} - Collection: {}",
					expected_len,
					res.len(),
					collection.as_ref().len(),
				)
			}
		}
		Ok(())
	}

	fn test_hnsw_collection(vt: VectorType, collection: TestCollection) -> Result<(), Error> {
		for distance in [Distance::Euclidean, Distance::Manhattan] {
			debug!(
				"Distance: {:?} - Collection: {} - Vector type: {}",
				distance,
				collection.as_ref().len(),
				vt,
			);
			let mut h = HnswGraph::new(100, distance.clone());
			insert_collection_one_by_one(&mut h, &collection)?;
		}
		Ok(())
	}

	#[test]
	fn test_hnsw_unique_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for i in 0..30 {
				test_hnsw_collection(vt, TestCollection::new_unique(i, vt, 2))?;
			}
		}
		Ok(())
	}
}
