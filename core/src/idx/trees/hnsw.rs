use crate::idx::docids::DocId;
use crate::idx::trees::knn::KnnResult;
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::Distance;
use hnsw::{Hnsw, Params, Searcher};
use rand_pcg::Pcg64;
use roaring::RoaringTreemap;
use space::Metric;
use std::collections::HashMap;

enum Docs {
	One(DocId),
	Vec2([DocId; 2]),
	Vec3([DocId; 3]),
	Vec4([DocId; 4]),
	Bits(RoaringTreemap),
}
struct HnswIndex<T, const M: usize, const M0: usize> {
	h: Hnsw<Distance, T, Pcg64, M, M0>,
	d: HashMap<SharedVector, Docs>,
}

impl<T, const M: usize, const M0: usize> HnswIndex<T, M, M0> {
	fn new(distance: Distance, efc: usize) -> Self {
		let h: Hnsw<_, _, Pcg64, M, M0> =
			Hnsw::new_params(distance, Params::new().ef_construction(efc));
		let d = HashMap::new();
		HnswIndex {
			h,
			d,
		}
	}

	fn insert(&mut self, o: T, _d: DocId) {
		let mut searcher: Searcher<Distance> = Searcher::default();
		self.h.insert(o, &mut searcher);
	}

	fn search(&self, _o: &SharedVector, _n: usize) -> KnnResult {
		todo!()
	}
}

impl Metric<&'static [f32]> for Distance {
	type Unit = u32;
	fn distance(&self, a: &&[f32], b: &&[f32]) -> Self::Unit {
		match self {
			Distance::Euclidean => {
				a.iter().zip(b.iter()).map(|(&a, &b)| (a - b).powi(2)).sum::<f32>().sqrt().to_bits()
			}
			Distance::Manhattan => {
				a.iter().zip(b.iter()).map(|(&a, &b)| (a - b).abs()).sum::<f32>().to_bits()
			}
			Distance::Hamming => a.iter().zip(b.iter()).filter(|(&a, &b)| a != b).count() as u32,
			Distance::Minkowski(order) => a
				.iter()
				.zip(b.iter())
				.map(|(&a, &b)| (a - b).abs().powf(order.to_float() as f32))
				.sum::<f32>()
				.to_bits(),
		}
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

	fn insert_collection_one_by_one<T, const M: usize, const M0: usize>(
		h: &mut HnswIndex<T, M, M0>,
		collection: &TestCollection,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			h.insert(obj.clone(), *doc_id);
			map.insert(*doc_id, obj.clone());
		}
		Ok(map)
	}

	fn find_collection<T, const M: usize, const M0: usize>(
		h: &HnswIndex<T, M, M0>,
		collection: &TestCollection,
	) -> Result<(), Error> {
		let max_knn = 20.max(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.search(obj, knn);
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

	fn test_hnsw_collection<T, const M: usize, const M0: usize>(
		vt: VectorType,
		collection: TestCollection,
	) -> Result<(), Error> {
		for distance in [Distance::Euclidean, Distance::Manhattan] {
			debug!(
				"Distance: {:?} - Collection: {} - Vector type: {}",
				distance,
				collection.as_ref().len(),
				vt,
			);
			let mut h = HnswIndex::new(distance, 500);
			insert_collection_one_by_one::<T, M, M0>(&mut h, &collection)?;
			find_collection::<T, M, M0>(&h, &collection)?;
		}
		Ok(())
	}

	#[test]
	fn test_hnsw_unique_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for i in 0..30 {
				test_hnsw_collection::<&'static [f32], 12, 24>(
					vt,
					TestCollection::new_unique(i, vt, 2),
				)?;
			}
		}
		Ok(())
	}
}
