use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use ahash::HashMap;
use anyhow::{Result, bail};
use futures::StreamExt;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::catalog::{DatabaseDefinition, Distance, HnswParams, TableId, VectorType};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Cond;
use crate::idx::IndexKeyBase;
use crate::idx::planner::ScanDirection;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::docs::{HnswDocs, VecDocs};
use crate::idx::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::{ElementId, HnswSearch, VectorId, VectorPendingUpdate};
use crate::idx::trees::knn::KnnResultBuilder;
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::kvs::{KVValue, Key, Transaction};
use crate::val::{Number, RecordId, RecordIdKey, Value};

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

pub(super) struct HnswContext<'a> {
	pub(super) ctx: &'a FrozenContext,
	pub(super) tx: Arc<Transaction>,
	pub(super) db: &'a DatabaseDefinition,
	pub(super) vec_docs: &'a VecDocs,
}

impl<'a> HnswContext<'a> {
	pub(super) fn new(
		ctx: &'a FrozenContext,
		db: &'a DatabaseDefinition,
		vec_docs: &'a VecDocs,
	) -> Self {
		Self {
			ctx,
			tx: ctx.tx(),
			db,
			vec_docs,
		}
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
			VectorId::DocId(doc_id)
		} else {
			VectorId::RecordKey(Arc::new(id.clone()))
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

	pub(super) fn new_hnsw_context<'a>(
		&'a self,
		ctx: &'a FrozenContext,
		db: &'a DatabaseDefinition,
	) -> HnswContext<'a> {
		HnswContext::new(ctx, db, &self.vec_docs)
	}

	pub(crate) async fn index_pending(
		&self,
		ctx: &FrozenContext,
		db: &DatabaseDefinition,
		pending: VectorPendingUpdate,
	) -> Result<()> {
		let mut hnsw = self.hnsw.write().await;
		// Ensure the layers are up-to-date
		hnsw.check_state(ctx).await?;
		// Create a new context
		let mut ctx = self.new_hnsw_context(ctx, db);
		// Remove old values if any
		if let VectorId::DocId(doc_id) = pending.id {
			for vector in pending.old_vectors {
				// Extract the vector
				let vector = Vector::from(vector);
				// Remove the vector
				self.vec_docs.remove(&ctx, &vector, doc_id, &mut hnsw).await?;
			}
		}

		// Index the new values if any
		let mut docs = self.docs.write().await;
		if pending.new_vectors.is_empty() {
			let doc_id = match pending.id {
				VectorId::DocId(doc_id) => doc_id,
				VectorId::RecordKey(id) => docs.resolve(&ctx.tx, &id).await?,
			};
			for vector in pending.new_vectors {
				// Extract the vector
				let vector = Vector::from(vector);
				// Insert the vector
				self.vec_docs.insert(&mut ctx, vector, doc_id, &mut hnsw).await?;
			}
		}
		// update the state
		docs.finish(&ctx.tx).await?;
		Ok(())
	}

	// Ensure the layers are up-to-date
	pub(crate) async fn check_state(&self, ctx: &FrozenContext) -> Result<()> {
		self.hnsw.write().await.check_state(ctx).await
	}

	#[expect(clippy::too_many_arguments)]
	pub(crate) async fn knn_search(
		&self,
		db: &DatabaseDefinition,
		ctx: &FrozenContext,
		stk: &mut Stk,
		pt: &[Number],
		k: usize,
		ef: usize,
		cond_filter: Option<(&Options, Arc<Cond>)>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		// Build a filter if required
		let mut filter = if let Some((opt, cond)) = cond_filter {
			Some(HnswTruthyDocumentFilter::new(opt, self.ikb.clone(), self.docs.read().await, cond))
		} else {
			None
		};
		// Extract the vector
		let vector: SharedVector = Vector::try_from_vector(self.vector_type, pt)?.into();
		vector.check_dimension(self.dim)?;
		let search = HnswSearch::new(vector, k, ef);
		// Get a new HNSW context
		let ctx = self.new_hnsw_context(ctx, db);
		// Collect the result
		let mut builder = KnnResultBuilder::new(k);

		// Search in the pendings if any
		let pending_docs =
			self.search_pendings(&ctx, stk, &search, &mut filter, &mut builder).await?;
		// Search in the graph
		self.search_graph(&ctx, stk, &search, pending_docs, &mut filter, &mut builder).await?;

		// We build the final result: replacing DocId with RecordIds
		let result = builder.collect();

		let (docs, cache) = if let Some(filter) = filter {
			// If there is a filter, we returns the read-locked HnswDoc
			// and the record cache
			let (docs, cache) = filter.release();
			(docs, Some(cache))
		} else {
			(self.docs.read().await, None)
		};
		// We can now build the final result
		let mut res = VecDeque::with_capacity(result.len());
		for (dist, id) in result {
			let dist: f64 = dist.into();
			// Do we have it from the cache?
			if let Some(cache) = &cache {
				if let Some(Some((rid, record))) = cache.get(&id) {
					res.push_back((rid.clone(), dist, Some(record.clone())));
					continue;
				}
			}
			// Otherwise we get it from the state
			match id {
				VectorId::DocId(doc_id) => {
					if let Some(rid) = docs.get_thing(&ctx.tx, doc_id).await? {
						res.push_back((Arc::new(rid), dist, None));
					}
				}
				VectorId::RecordKey(key) => {
					let rid = RecordId::new(self.ikb.table().clone(), key.as_ref().clone());
					res.push_back((Arc::new(rid), dist, None));
				}
			}
		}
		Ok(res)
	}

	/// Search for results in the HNSW graph
	pub(super) async fn search_graph(
		&self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		search: &HnswSearch,
		pending_docs: Option<RoaringTreemap>,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
	) -> Result<()> {
		let hnsw = self.hnsw.read().await;
		// Do the search
		if let Some(filter) = filter {
			let neighbours = hnsw
				.knn_search_with_filter(&ctx, search, stk, filter, pending_docs.as_ref())
				.await?;
			self.add_graph_results(&ctx.tx, &hnsw, neighbours, builder, |evicted_docs| {
				filter.expires(&evicted_docs)
			})
			.await
		} else {
			let neighbours = hnsw.knn_search(ctx, search, pending_docs.as_ref()).await?;
			self.add_graph_results(&ctx.tx, &hnsw, neighbours, builder, |_| {}).await
		}
	}

	async fn search_pendings(
		&self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		search: &HnswSearch,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
	) -> Result<Option<RoaringTreemap>> {
		let mut all_existing_docs = RoaringTreemap::new();
		let mut non_deleted_docs = HashMap::default();
		// First pass, identify deleted doc
		self.collect_pending(ctx.ctx, &ctx.tx, |_, pending| {
			if let VectorId::DocId(doc_id) = pending.id {
				all_existing_docs.insert(doc_id);
			};
			if pending.new_vectors.is_empty() {
				non_deleted_docs.remove(&pending.id);
			} else {
				non_deleted_docs.insert(pending.id, pending.new_vectors);
			}
		})
		.await?;
		if all_existing_docs.is_empty() && non_deleted_docs.is_empty() {
			return Ok(None);
		}
		// Second pass, we build the KNN result for non-deleted documents
		for (id, vectors) in non_deleted_docs {
			// If there is a filter, we need to check if the record is truthy
			if let Some(filter) = filter {
				if !filter.check_vector_id_truthy(ctx, stk, id.clone()).await? {
					continue;
				}
			}
			for vector in vectors {
				let vector = Vector::from(vector);
				let d = self.distance.calculate(&search.pt, &vector);
				if builder.check_add(d) {
					if let Some(evicted_id) = builder.add_vector_id_result(d, id.clone())
						&& let Some(filter) = filter
					{
						filter.expire(&evicted_id);
					}
				}
			}
		}
		if all_existing_docs.is_empty() {
			return Ok(None);
		}
		Ok(Some(all_existing_docs))
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
		let mut stream = tx.stream_keys_vals(rng, None, None, 0, ScanDirection::Forward, false);
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

	async fn add_graph_results<F>(
		&self,
		tx: &Transaction,
		hnsw: &HnswFlavor,
		neighbors: Vec<(f64, ElementId)>,
		builder: &mut KnnResultBuilder,
		mut evited_docs_func: F,
	) -> Result<()>
	where
		F: FnMut(Vec<VectorId>),
	{
		for (e_dist, e_id) in neighbors {
			if builder.check_add(e_dist)
				&& let Some(v) = hnsw.get_vector(tx, &e_id).await?
				&& let Some(docs) = self.vec_docs.get_docs(tx, &v).await?
			{
				let evicted_docs = builder.add_graph_result(e_dist, docs);
				evited_docs_func(evicted_docs);
			}
		}
		Ok(())
	}

	#[cfg(test)]
	pub(super) async fn check_hnsw_properties(&self, expected_count: usize) {
		self.hnsw.read().await.check_hnsw_properties(expected_count).await
	}
}
