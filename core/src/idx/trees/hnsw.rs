use crate::idx::docids::DocId;
use crate::idx::trees::knn::{Docs, KnnResult, KnnResultBuilder};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::sql::index::Distance;
use hnsw::{Hnsw, Params, Searcher};
use rand_pcg::Pcg64;
use space::{Metric, Neighbor};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

struct HnswIndex<const M: usize, const M0: usize> {
	s: Searcher<u64>,
	h: Hnsw<Distance, SharedVector, Pcg64, M, M0>,
	d: HashMap<SharedVector, Docs>,
}
impl Metric<SharedVector> for Distance {
	type Unit = u64;

	//TODO! Remove unwrap
	fn distance(&self, a: &Arc<Vector>, b: &Arc<Vector>) -> Self::Unit {
		match &self {
			Distance::Euclidean => a.euclidean_distance(b).unwrap().to_bits(),
			Distance::Manhattan => a.manhattan_distance(b).unwrap().to_bits(),
			Distance::Hamming => todo!(),
			Distance::Minkowski(order) => a.minkowski_distance(b, order).unwrap().to_bits(),
		}
	}
}

impl<const M: usize, const M0: usize> HnswIndex<M, M0> {
	fn new(distance: Distance, efc: usize) -> Self {
		let s = Searcher::default();
		let h = Hnsw::new_params(distance, Params::new().ef_construction(efc));
		let d = HashMap::new();
		HnswIndex {
			s,
			h,
			d,
		}
	}

	fn insert(&mut self, o: SharedVector, d: DocId) {
		self.h.insert(o.clone(), &mut self.s);
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
		let mut prepare = vec![
			Neighbor {
				index: !0,
				distance: !0,
			};
			n
		];
		let neighbors = self.h.nearest(o, ef, &mut self.s, &mut prepare);
		let mut builder = KnnResultBuilder::new(n);
		for n in neighbors {
			let d = n.distance as f64;
			if builder.check_add(d) {
				let o = self.h.feature(n.index);
				if let Some(docs) = self.d.get(o) {
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

	fn insert_collection_one_by_one<const M: usize, const M0: usize>(
		h: &mut HnswIndex<M, M0>,
		collection: &TestCollection,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			h.insert(obj.clone(), *doc_id);
			map.insert(*doc_id, obj.clone());
		}
		Ok(map)
	}

	fn find_collection<const M: usize, const M0: usize>(
		h: &mut HnswIndex<M, M0>,
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

	fn test_hnsw_collection<const M: usize, const M0: usize>(
		distance: Distance,
		collection: &TestCollection,
	) -> Result<(), Error> {
		let mut h: HnswIndex<M, M0> = HnswIndex::new(distance, 24);
		insert_collection_one_by_one::<M, M0>(&mut h, collection)?;
		find_collection::<M, M0>(&mut h, &collection)?;
		Ok(())
	}

	fn test_hnsw_collection_distances<const D: usize, const M: usize, const M0: usize>(
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
			test_hnsw_collection::<M, M0>(distance, &collection)?;
		}
		Ok(())
	}

	#[test]
	fn test_hnsw_unique_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			const DIM: usize = 10;
			test_hnsw_collection_distances::<DIM, 12, 24>(
				vt,
				TestCollection::new_unique(DIM, vt, 2),
			)?;
		}
		Ok(())
	}
}
