use crate::idx::docids::DocId;
use crate::idx::trees::knn::{Docs, KnnResult, KnnResultBuilder};
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::Distance;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

struct HnswIndex<const M: usize, const M0: usize, const EFC: usize> {
	h: Hnsw<M, M0, EFC>,
	d: HashMap<SharedVector, Docs>,
}

impl<const M: usize, const M0: usize, const EFC: usize> HnswIndex<M, M0, EFC> {
	fn new(_distance: Distance) -> Self {
		let h = Hnsw::default();
		let d = HashMap::new();
		HnswIndex {
			h,
			d,
		}
	}

	fn insert(&mut self, o: SharedVector, d: DocId) {
		self.h.insert(o.clone());
		match self.d.entry(o) {
			Entry::Occupied(mut e) => {
				let docs = e.get_mut();
				if let Some(new_docs) = docs.insert(d) {
					e.insert(new_docs);
				}
			}
			Entry::Vacant(e) => {
				e.insert(Docs::One(d));
			}
		}
	}

	fn search(&mut self, o: &SharedVector, n: usize, ef: usize) -> KnnResult {
		let neighbors = self.h.knn_search(o, n, ef);
		let mut builder = KnnResultBuilder::new(n);
		for (e, d) in neighbors {
			if builder.check_add(d) {
				if let Some(docs) = self.d.get(&e) {
					builder.add(d, docs);
				}
			}
		}

		builder.build(
			#[cfg(debug_assertions)]
			HashMap::new(),
		)
	}
}

struct Hnsw<const M: usize, const M0: usize, const EFC: usize> {
	ml: f64,
	zero: Vec<[usize; M0]>,
	layers: Vec<Vec<Node<M>>>,
	elements: Vec<SharedVector>,
	rng: SmallRng,
}

impl<const M: usize, const M0: usize, const EFC: usize> Default for Hnsw<M, M0, EFC> {
	fn default() -> Self {
		Self::new(1.0 / (M as f64).ln())
	}
}
impl<const M: usize, const M0: usize, const EFC: usize> Hnsw<M, M0, EFC> {
	fn new(ml: f64) -> Self {
		Self {
			ml,
			zero: Vec::default(),
			layers: Vec::default(),
			elements: Vec::default(),
			rng: SmallRng::from_entropy(),
		}
	}

	fn insert(&mut self, q: SharedVector) -> usize {
		if self.zero.is_empty() {
			return self.insert_first_element(q);
		}

		todo!()
	}

	fn insert_first_element(&mut self, q: SharedVector) -> usize {
		self.zero.push([!0; M0]);
		self.elements.push(q);
		let level = self.new_element_level();
		while self.layers.len() < level {
			// It's always index 0 with no neighbors since its the first feature.
			let node = Node {
				zero_node: 0,
				next_node: 0,
				neighbors: [!0; M],
			};
			self.layers.push(vec![node]);
		}
		0
	}

	fn new_element_level(&mut self) -> usize {
		let unif: f64 = self.rng.gen(); // generate a uniform random number between 0 and 1
		let layer = (-unif.ln() * self.ml).floor() as usize; // calculate the layer
		layer
	}

	fn knn_search(&self, _q: &SharedVector, _k: usize, _ef: usize) -> Vec<(SharedVector, f64)> {
		todo!()
	}
}

pub struct Node<const N: usize> {
	/// The node in the zero layer this refers to.
	pub zero_node: usize,
	/// The node in the layer below this one that this node corresponds to.
	pub next_node: usize,
	/// The neighbors in the graph of this node.
	pub neighbors: [usize; N],
}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::docids::DocId;
	use crate::idx::trees::hnsw::HnswIndex;
	use crate::idx::trees::knn::tests::TestCollection;
	use crate::idx::trees::vector::SharedVector;
	use crate::sql::index::{Distance, VectorType};
	use std::collections::HashMap;
	use test_log::test;

	fn insert_collection_one_by_one<const M: usize, const M0: usize, const EFC: usize>(
		h: &mut HnswIndex<M, M0, EFC>,
		collection: &TestCollection,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			h.insert(obj.clone(), *doc_id);
			map.insert(*doc_id, obj.clone());
		}
		Ok(map)
	}

	fn find_collection<const M: usize, const M0: usize, const EFC: usize>(
		h: &mut HnswIndex<M, M0, EFC>,
		collection: &TestCollection,
	) -> Result<(), Error> {
		let max_knn = 20.max(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.search(obj, knn, 500);
				if collection.is_unique() {
					assert!(
						res.docs.contains(doc_id),
						"Search: {:?} - Knn: {} - Wrong Doc - Expected: {} - Got: {:?}",
						obj,
						knn,
						doc_id,
						res.docs
					);
				}
				let expected_len = collection.as_ref().len().min(knn);
				assert_eq!(
					expected_len,
					res.docs.len(),
					"Wrong knn count - Expected: {} - Got: {} - Collection: {}",
					expected_len,
					res.docs.len(),
					collection.as_ref().len(),
				)
			}
		}
		Ok(())
	}

	fn test_hnsw_collection<const M: usize, const M0: usize, const EFC: usize>(
		distance: Distance,
		collection: &TestCollection,
	) -> Result<(), Error> {
		let mut h: HnswIndex<M, M0, EFC> = HnswIndex::new(distance);
		insert_collection_one_by_one::<M, M0, EFC>(&mut h, collection)?;
		find_collection::<M, M0, EFC>(&mut h, &collection)?;
		Ok(())
	}

	fn test_hnsw_collection_distances<
		const D: usize,
		const M: usize,
		const M0: usize,
		const EFC: usize,
	>(
		vt: VectorType,
		collection: TestCollection,
	) -> Result<(), Error> {
		for distance in [
			Distance::Euclidean,
			Distance::Manhattan,
			Distance::Hamming,
			Distance::Minkowski(2.into()),
		] {
			debug!(
				"Distance: {:?} - Collection: {} - Vector type: {}",
				distance,
				collection.as_ref().len(),
				vt,
			);
			test_hnsw_collection::<M, M0, EFC>(distance, &collection)?;
		}
		Ok(())
	}

	#[test]
	fn test_hnsw_unique_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			const DIM: usize = 10;
			test_hnsw_collection_distances::<DIM, 12, 24, 500>(
				vt,
				TestCollection::new_unique(DIM, vt, 2),
			)?;
		}
		Ok(())
	}
}
