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
use anyhow::Result;
use reblessive::tree::Stk;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::RwLock;

pub(crate) struct HnswIndex {
	dim: usize,
	ikb: IndexKeyBase,
	vector_type: VectorType,
	hnsw: RwLock<HnswFlavor>,
	docs: RwLock<HnswDocs>,
	vec_docs: VecDocs,
	next_appending_id: AtomicU32,
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
			hnsw: RwLock::new(HnswFlavor::new(tb, ikb.clone(), p, vector_cache)?),
			docs: RwLock::new(
				HnswDocs::new(tx, ikb.table().to_string().into(), ikb.clone()).await?,
			),
			vec_docs: VecDocs::new(ikb.clone(), p.use_hashed_vector),
			ikb,
			// TODO should be calculated based on the existing pendings
			next_appending_id: AtomicU32::new(0),
		})
	}

	fn content_to_vectors(&self, content: Vec<Value>) -> Result<Vec<SerializedVector>> {
		let mut vectors = Vec::with_capacity(content.len());
		// Index the values
		for value in content.into_iter().filter(|v| !v.is_nullish()) {
			// Extract the vector
			let vector = SerializedVector::try_from_value(self.vector_type, self.dim, value)?;
			Vector::check_expected_dimension(vector.dimension(), self.dim)?;
			// Insert the vector
			vectors.push(vector);
		}
		Ok(vectors)
	}

	pub(crate) async fn index(
		&self,
		ctx: &Context,
		id: &RecordIdKey,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
	) -> Result<()> {
		if old_values.is_none() && new_values.is_none() {
			return Ok(());
		}
		let old_vectors = if let Some(v) = old_values {
			self.content_to_vectors(v)?
		} else {
			vec![]
		};
		let new_vectors = if let Some(v) = new_values {
			self.content_to_vectors(v)?
		} else {
			vec![]
		};
		let tx = ctx.tx();
		let doc_id = self.docs.write().await.resolve(&tx, id).await?;
		let appending_id = self.next_appending_id.fetch_add(1, Ordering::Relaxed);
		let key = self.ikb.new_hp_key(appending_id);
		let pending = VectorPendingUpdate {
			doc_id,
			old_vectors,
			new_vectors,
		};
		tx.put(&key, &pending, None).await?;
		Ok(())
	}

	pub(crate) async fn index_pending(
		&self,
		ctx: &Context,
		pending: VectorPendingUpdate,
	) -> Result<()> {
		let tx = ctx.tx();
		let mut hnsw = self.hnsw.write().await;
		// Ensure the layers are up-to-date
		hnsw.check_state(ctx).await?;

		// Remove old values if any
		for vector in pending.old_vectors {
			// Extract the vector
			let vector = Vector::from(vector);
			// Remove the vector
			self.vec_docs.remove(&tx, &vector, pending.doc_id, &mut hnsw).await?;
		}

		// Index the new values if any
		for vector in pending.new_vectors {
			// Extract the vector
			let vector = Vector::from(vector);
			// Insert the vector
			self.vec_docs.insert(&tx, vector, pending.doc_id, &mut hnsw).await?;
		}
		Ok(())
	}

	// Ensure the layers are up-to-date
	pub(crate) async fn check_state(&self, ctx: &Context) -> Result<()> {
		self.hnsw.write().await.check_state(ctx).await
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
		let docs = self.docs.read().await;
		let res = chk.convert_result(tx, &docs, result).await?;
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
		let hnsw = self.hnsw.read().await;
		let docs = self.docs.read().await;
		// Do the search
		let neighbors = match chk {
			HnswConditionChecker::Hnsw(_) => hnsw.knn_search(tx, search).await?,
			HnswConditionChecker::HnswCondition(_) => {
				hnsw.knn_search_checked(db, tx, stk, search, &docs, &self.vec_docs, chk).await?
			}
		};
		self.build_result(tx, &hnsw, neighbors, search.k, chk).await
	}

	async fn collect_pending(&self, tx: &Transaction) -> Result<()> {
		// let rng = self.ikb.new_hp_range()?;
		// tx.stream_keys_vals(rng, None, None).await?;
		todo!()
	}

	async fn collect_pending_checked(
		&self,
		tx: &Transaction,
		chk: &HnswConditionChecker<'_>,
	) -> Result<()> {
		todo!()
	}

	async fn build_result(
		&self,
		tx: &Transaction,
		hnsw: &HnswFlavor,
		neighbors: Vec<(f64, ElementId)>,
		n: usize,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<KnnResult> {
		let mut builder = KnnResultBuilder::new(n);
		for (e_dist, e_id) in neighbors {
			if builder.check_add(e_dist)
				&& let Some(v) = hnsw.get_vector(tx, &e_id).await?
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
