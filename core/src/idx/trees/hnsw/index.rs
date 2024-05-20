use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::planner::checker::ConditionChecker;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::hnsw::elements::HnswElements;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::sql::index::{HnswParams, VectorType};
use crate::sql::{Number, Thing, Value};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use reblessive::tree::Stk;
use std::collections::VecDeque;

pub struct HnswIndex {
	dim: usize,
	vector_type: VectorType,
	hnsw: HnswFlavor,
	docs: HnswDocs,
	vec_docs: VecDocs,
}

pub(super) type VecDocs = HashMap<SharedVector, (Ids64, ElementId)>;

pub(super) struct HnswSearchContext<'a> {
	elements: &'a HnswElements,
	vec_docs: Option<&'a VecDocs>,
	pt: &'a SharedVector,
	ef: usize,
}

impl<'a> HnswSearchContext<'a> {
	pub(super) fn new(
		elements: &'a HnswElements,
		vec_docs: Option<&'a VecDocs>,
		pt: &'a SharedVector,
		ef: usize,
	) -> Self {
		Self {
			elements,
			vec_docs,
			pt,
			ef,
		}
	}

	pub(super) fn pt(&self) -> &SharedVector {
		self.pt
	}

	pub(super) fn ef(&self) -> usize {
		self.ef
	}

	pub(super) fn get_docs(&self, pt: &SharedVector) -> Option<&Ids64> {
		if let Some(vec_docs) = self.vec_docs {
			vec_docs.get(pt).map(|(doc_ids, _)| doc_ids)
		} else {
			None
		}
	}

	pub(super) fn elements(&self) -> &HnswElements {
		self.elements
	}
}

impl HnswIndex {
	pub fn new(p: &HnswParams) -> Self {
		Self {
			dim: p.dimension as usize,
			vector_type: p.vector_type,
			hnsw: HnswFlavor::new(p),
			docs: HnswDocs::default(),
			vec_docs: HashMap::default(),
		}
	}

	pub fn index_document(&mut self, rid: &Thing, content: &Vec<Value>) -> Result<(), Error> {
		// Resolve the doc_id
		let doc_id = self.docs.resolve(rid);
		// Index the values
		for value in content {
			// Extract the vector
			let vector = Vector::try_from_value(self.vector_type, self.dim, value)?;
			vector.check_dimension(self.dim)?;
			self.insert(vector.into(), doc_id);
		}
		Ok(())
	}

	pub(super) fn insert(&mut self, o: SharedVector, d: DocId) {
		match self.vec_docs.entry(o) {
			Entry::Occupied(mut e) => {
				let (docs, element_id) = e.get_mut();
				if let Some(new_docs) = docs.insert(d) {
					let element_id = *element_id;
					e.insert((new_docs, element_id));
				}
			}
			Entry::Vacant(e) => {
				let o = e.key().clone();
				let element_id = self.hnsw.insert(o);
				e.insert((Ids64::One(d), element_id));
			}
		}
	}

	pub(super) fn remove(&mut self, o: SharedVector, d: DocId) {
		if let Entry::Occupied(mut e) = self.vec_docs.entry(o) {
			let (docs, e_id) = e.get_mut();
			if let Some(new_docs) = docs.remove(d) {
				let e_id = *e_id;
				if new_docs.is_empty() {
					e.remove();
					self.hnsw.remove(e_id);
				} else {
					e.insert((new_docs, e_id));
				}
			}
		}
	}

	pub(crate) fn remove_document(
		&mut self,
		rid: &Thing,
		content: &Vec<Value>,
	) -> Result<(), Error> {
		if let Some(doc_id) = self.docs.remove(rid) {
			for v in content {
				// Extract the vector
				let vector = Vector::try_from_value(self.vector_type, self.dim, v)?;
				vector.check_dimension(self.dim)?;
				// Remove the vector
				self.remove(vector.into(), doc_id);
			}
		}
		Ok(())
	}

	pub async fn knn_search(
		&self,
		o: &Vec<Number>,
		n: usize,
		ef: usize,
		stk: &mut Stk,
		mut chk: ConditionChecker<'_>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		// Extract the vector
		let vector: SharedVector = Vector::try_from_vector(self.vector_type, o)?.into();
		vector.check_dimension(self.dim)?;
		// Do the search
		let result = self.search(&vector, n, ef, stk, &mut chk).await?;
		let res = chk.convert_result(result.docs).await?;
		Ok(res)
	}

	pub(super) async fn search(
		&self,
		pt: &SharedVector,
		k: usize,
		ef: usize,
		stk: &mut Stk,
		chk: &mut ConditionChecker<'_>,
	) -> Result<KnnResult, Error> {
		let neighbors = match chk {
			ConditionChecker::Hnsw(_) => self.hnsw.knn_search(pt, k, ef),
			ConditionChecker::HnswCondition(_) => {
				self.hnsw.knn_search_checked(pt, k, ef, &self.vec_docs, stk, chk).await?
			}
			#[cfg(test)]
			ConditionChecker::None => self.hnsw.knn_search(pt, k, ef),
			_ => unreachable!(),
		};
		let result = self.build_result(neighbors, k, chk);
		Ok(result)
	}

	fn build_result(
		&self,
		neighbors: Vec<(f64, ElementId)>,
		n: usize,
		chk: &mut ConditionChecker<'_>,
	) -> KnnResult {
		let mut builder = KnnResultBuilder::new(n);
		for (e_dist, e_id) in neighbors {
			if builder.check_add(e_dist) {
				if let Some(v) = self.hnsw.get_vector(&e_id) {
					if let Some((docs, _)) = self.vec_docs.get(v) {
						builder.add(e_dist, docs, chk);
					}
				}
			}
		}
		builder.build(
			#[cfg(debug_assertions)]
			HashMap::new(),
		)
	}

	pub(in crate::idx) fn get_thing(&self, doc_id: DocId) -> Option<&Thing> {
		self.docs.get_thing(doc_id)
	}

	#[cfg(test)]
	pub(super) fn check_hnsw_properties(&self, expected_count: usize) {
		self.hnsw.check_hnsw_properties(expected_count)
	}
}
