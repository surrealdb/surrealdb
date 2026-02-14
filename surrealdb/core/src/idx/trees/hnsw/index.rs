use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};

use ahash::HashMap;
use anyhow::{Result, bail};
use futures::StreamExt;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::catalog::{DatabaseDefinition, Distance, HnswParams, TableId, VectorType};
use crate::ctx::Context;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::planner::ScanDirection;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::docs::{HnswDocs, VecDocs};
use crate::idx::trees::hnsw::elements::HnswElements;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::{ElementId, HnswSearch, VectorPendingId, VectorPendingUpdate};
use crate::idx::trees::knn::{FloatKey, KnnResult, KnnResultBuilder};
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::kvs::{KVValue, Key, Transaction};
use crate::val::{Number, RecordIdKey, Value};

pub(crate) struct HnswIndex {
	dim: usize,
	distance: Distance,
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
			distance: p.distance.clone(),
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
		let id = if let Some(doc_id) = self.docs.read().await.get(&tx, &id).await? {
			VectorPendingId::DocId(doc_id)
		} else {
			VectorPendingId::Id(id.clone())
		};
		let appending_id = self.next_appending_id.fetch_add(1, Ordering::Relaxed);
		let key = self.ikb.new_hp_key(appending_id);
		let pending = VectorPendingUpdate {
			id,
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
		let mut docs = self.docs.write().await;

		// Ensure the layers are up-to-date
		hnsw.check_state(ctx).await?;

		// Remove old values if any
		if let VectorPendingId::DocId(doc_id) = pending.id {
			for vector in pending.old_vectors {
				// Extract the vector
				let vector = Vector::from(vector);
				// Remove the vector
				self.vec_docs.remove(&tx, &vector, doc_id, &mut hnsw).await?;
			}
		}

		// Index the new values if any
		if pending.new_vectors.is_empty() {
			let doc_id = match pending.id {
				VectorPendingId::DocId(doc_id) => doc_id,
				VectorPendingId::Id(id) => docs.resolve(&tx, &id).await?,
			};
			for vector in pending.new_vectors {
				// Extract the vector
				let vector = Vector::from(vector);
				// Insert the vector
				self.vec_docs.insert(&tx, vector, doc_id, &mut hnsw).await?;
			}
		}
		// update the state
		docs.finish(&tx).await?;
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
		ctx: &Context,
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
		let tx = ctx.tx();
		// Do the search
		let result = self.search(db, ctx, &tx, stk, &search, &mut chk).await?;
		let docs = self.docs.read().await;
		let res = chk.convert_result(&tx, &docs, result).await?;
		Ok(res)
	}

	pub(super) async fn search(
		&self,
		db: &DatabaseDefinition,
		ctx: &Context,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswSearch,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<KnnResult> {
		let (pending_result, pending_delete) = self.search_pendings(ctx, tx, search).await?;
		println!("pending_result: {:?}", pending_result);
		println!("pending_delete: {:?}", pending_delete);
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

	async fn search_pendings(
		&self,
		ctx: &Context,
		tx: &Transaction,
		search: &HnswSearch,
	) -> Result<(BTreeMap<FloatKey, VectorPendingId>, RoaringTreemap)> {
		let mut all_existing_docs = RoaringTreemap::new();
		let mut non_deleted_docs = HashMap::default();
		// First pass, identify deleted doc
		self.collect_pending(ctx, tx, |_, pending| {
			if let VectorPendingId::DocId(doc_id) = pending.id {
				all_existing_docs.insert(doc_id);
			};
			if pending.new_vectors.is_empty() {
				non_deleted_docs.remove(&pending.id);
			} else {
				non_deleted_docs.insert(pending.id, pending.new_vectors);
			}
		})
		.await?;
		// Second pass, we build the KNN result for non-deleted documents
		let mut result = BTreeMap::default();
		for (id, vectors) in non_deleted_docs {
			for vector in vectors {
				let vector = Vector::from(vector);
				let d = self.distance.calculate(&search.pt, &vector);
				result.insert(FloatKey::from(d), id.clone());
				if result.len() > search.k {
					result.pop_last();
				}
			}
		}
		Ok((result, all_existing_docs))
	}
	async fn collect_pending<F>(
		&self,
		ctx: &Context,
		tx: &Transaction,
		mut collector: F,
	) -> Result<()>
	where
		F: FnMut(Key, VectorPendingUpdate),
	{
		let rng = self.ikb.new_hp_range()?;
		let mut stream = tx.stream_keys_vals(rng, None, None, 0, ScanDirection::Forward);
		// Loop until no more entries
		let mut count = 0;
		while let Some(res) = stream.next().await {
			let batch = res?;
			for (k, v) in batch {
				// Check if the context is finished
				if ctx.is_done(Some(count)).await? {
					bail!(Error::QueryCancelled)
				}
				let pending = VectorPendingUpdate::kv_decode_value(v)?;
				collector(k, pending);
				// Parse the data from the store
				count += 1;
			}
		}
		Ok(())
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
		self.hnsw.read().await.check_hnsw_properties(expected_count).await
	}
}
