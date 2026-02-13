use std::collections::VecDeque;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::{DatabaseDefinition, HnswParams, TableId, VectorType};
use crate::ctx::Context;
use crate::idx::IndexKeyBase;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::docs::{HnswDocs, VecDocs};
use crate::idx::trees::hnsw::elements::HnswElements;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::{ElementId, HnswSearch, VectorPendingUpdate};
use crate::idx::trees::knn::{KnnResult, KnnResultBuilder};
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::kvs::Transaction;
use crate::val::{Number, RecordIdKey, Value};

pub(crate) struct HnswIndex {
	dim: usize,
	ikb: IndexKeyBase,
	vector_type: VectorType,
	hnsw: HnswFlavor,
	docs: HnswDocs,
	vec_docs: VecDocs,
}

pub(super) struct HnswCheckedSearchContext<'a> {
	elements: &'a HnswElements,
	docs: &'a HnswDocs,
	vec_docs: &'a VecDocs,
	pt: &'a SharedVector,
	ef: usize,
}

impl<'a> HnswCheckedSearchContext<'a> {
	pub(super) fn new(
		elements: &'a HnswElements,
		docs: &'a HnswDocs,
		vec_docs: &'a VecDocs,
		pt: &'a SharedVector,
		ef: usize,
	) -> Self {
		Self {
			elements,
			docs,
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

	pub(super) fn docs(&self) -> &HnswDocs {
		self.docs
	}

	pub(super) fn vec_docs(&self) -> &VecDocs {
		self.vec_docs
	}

	pub(super) fn elements(&self) -> &HnswElements {
		self.elements
	}
}

impl HnswIndex {
	pub(crate) async fn new(
		vector_cache: VectorCache,
		tx: &Transaction,
		ikb: IndexKeyBase,
		tb: TableId,
		p: &HnswParams,
	) -> Result<Self> {
		Ok(Self {
			dim: p.dimension as usize,
			vector_type: p.vector_type,
			hnsw: HnswFlavor::new(tb, ikb.clone(), p, vector_cache)?,
			docs: HnswDocs::new(tx, ikb.table().to_string().into(), ikb.clone()).await?,
			vec_docs: VecDocs::new(ikb.clone(), p.use_hashed_vector),
			ikb,
		})
	}

	fn content_to_vectors(&self, content: &[Value]) -> Result<Vec<SerializedVector>> {
		let mut vectors = Vec::with_capacity(content.len());
		// Index the values
		for value in content.iter().filter(|v| !v.is_nullish()) {
			// Extract the vector
			let vector = SerializedVector::try_from_value(self.vector_type, self.dim, value)?;
			Vector::check_expected_dimension(vector.dimension(), self.dim)?;
			// Insert the vector
			vectors.push(vector);
		}
		Ok(vectors)
	}

	async fn vector_pending_update<F>(
		&self,
		ctx: &Context,
		id: &RecordIdKey,
		content: &[Value],
		func: F,
	) -> Result<()>
	where
		F: FnOnce(Vec<SerializedVector>) -> VectorPendingUpdate,
	{
		let vectors = self.content_to_vectors(content)?;
		let key = self.ikb.new_hp_key(id);
		let batch = func(vectors);
		ctx.tx().put(&key, &batch, None).await?;
		Ok(())
	}

	pub(crate) async fn index_document(
		&mut self,
		ctx: &Context,
		id: &RecordIdKey,
		content: &[Value],
	) -> Result<()> {
		self.vector_pending_update(ctx, id, content, VectorPendingUpdate::Add).await
	}

	pub(crate) async fn remove_document(
		&mut self,
		ctx: &Context,
		id: &RecordIdKey,
		content: &[Value],
	) -> Result<()> {
		self.vector_pending_update(ctx, id, content, VectorPendingUpdate::Remove).await
	}

	async fn index_document_vectors(
		&mut self,
		ctx: &Context,
		id: &RecordIdKey,
		vectors: Vec<SerializedVector>,
	) -> Result<()> {
		let tx = ctx.tx();
		// Ensure the layers are up-to-date
		self.hnsw.check_state(ctx).await?;
		// Resolve the doc_id
		let doc_id = self.docs.resolve(&tx, id).await?;
		// Index the values
		for vector in vectors {
			// Extract the vector
			let vector: Vector = vector.into();
			// Insert the vector
			self.vec_docs.insert(&tx, vector, doc_id, &mut self.hnsw).await?;
		}
		self.docs.finish(&tx).await?;
		Ok(())
	}

	async fn remove_document_vectors(
		&mut self,
		ctx: &Context,
		id: &RecordIdKey,
		vectors: Vec<SerializedVector>,
	) -> Result<()> {
		let tx = ctx.tx();
		if let Some(doc_id) = self.docs.remove(&tx, id).await? {
			// Ensure the layers are up-to-date
			self.hnsw.check_state(ctx).await?;
			for vector in vectors {
				// Extract the vector
				let vector: Vector = vector.into();
				// Remove the vector
				self.vec_docs.remove(&tx, &vector, doc_id, &mut self.hnsw).await?;
			}
			self.docs.finish(&tx).await?;
		}
		Ok(())
	}

	// Ensure the layers are up-to-date
	pub(crate) async fn check_state(&mut self, ctx: &Context) -> Result<()> {
		self.hnsw.check_state(ctx).await
	}

	#[expect(clippy::too_many_arguments)]
	pub(crate) async fn knn_search(
		&self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		pt: &[Number],
		k: usize,
		ef: usize,
		mut chk: HnswConditionChecker<'_>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		// Extract the vector
		let vector: SharedVector = Vector::try_from_vector(self.vector_type, pt)?.into();
		vector.check_dimension(self.dim)?;
		let search = HnswSearch::new(vector, k, ef);
		// Do the search
		let result = self.search(db, tx, stk, &search, &mut chk).await?;
		let res = chk.convert_result(tx, &self.docs, result).await?;
		Ok(res)
	}

	pub(super) async fn search(
		&self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswSearch,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<KnnResult> {
		// Do the HNSW search
		let neighbors = match chk {
			HnswConditionChecker::Hnsw(_) => self.hnsw.knn_search(tx, search).await?,
			HnswConditionChecker::HnswCondition(_) => {
				self.hnsw
					.knn_search_checked(db, tx, stk, search, &self.docs, &self.vec_docs, chk)
					.await?
			}
		};
		// Collect the pending vectors
		self.build_result(tx, neighbors, search.k, chk).await
	}

	async fn build_result(
		&self,
		tx: &Transaction,
		neighbors: Vec<(f64, ElementId)>,
		n: usize,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<KnnResult> {
		let mut builder = KnnResultBuilder::new(n);
		for (e_dist, e_id) in neighbors {
			if builder.check_add(e_dist)
				&& let Some(v) = self.hnsw.get_vector(tx, &e_id).await?
				&& let Some(docs) = self.vec_docs.get_docs(tx, &v).await?
			{
				let evicted_docs = builder.add(e_dist, docs);
				chk.expires(evicted_docs);
			}
		}
		Ok(builder.build())
	}

	#[cfg(test)]
	pub(super) async fn check_hnsw_properties(&self, expected_count: usize) {
		self.hnsw.check_hnsw_properties(expected_count).await
	}
}
