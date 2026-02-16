use std::collections::hash_map::Entry;
use std::sync::Arc;

use ahash::HashMap;
use anyhow::Result;
use reblessive::tree::Stk;
use tokio::sync::RwLockReadGuard;

use crate::catalog::Record;
use crate::catalog::providers::TableProvider;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Cond, FlowResultExt as _};
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::VectorId;
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::hnsw::index::HnswContext;
use crate::idx::trees::knn::Ids64;
use crate::val::RecordId;

pub(super) type FilterCache = HashMap<VectorId, Option<(Arc<RecordId>, Arc<Record>)>>;

pub(super) struct HnswTruthyDocumentFilter<'a> {
	opt: &'a Options,
	ikb: IndexKeyBase,
	docs: RwLockReadGuard<'a, HnswDocs>,
	cond: Arc<Cond>,
	cache: FilterCache,
}

impl<'a> HnswTruthyDocumentFilter<'a> {
	pub(super) fn new(
		opt: &'a Options,
		ikb: IndexKeyBase,
		docs: RwLockReadGuard<'a, HnswDocs>,
		cond: Arc<Cond>,
	) -> Self {
		Self {
			opt,
			ikb,
			docs,
			cond,
			cache: Default::default(),
		}
	}

	pub(super) async fn check_any_doc_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		doc_ids: Ids64,
	) -> Result<bool> {
		for doc_id in doc_ids.iter() {
			if self.check_vector_id_truthy(ctx, stk, VectorId::DocId(doc_id)).await? {
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub(super) async fn check_vector_id_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		id: VectorId,
	) -> Result<bool> {
		match self.cache.entry(id) {
			Entry::Occupied(e) => Ok(e.get().is_some()),
			Entry::Vacant(e) => {
				// Collect the RecordId
				let rid = match e.key() {
					VectorId::DocId(doc_id) => {
						let Some(rid) = self.docs.get_thing(&ctx.tx, *doc_id).await? else {
							// No record ID ? It is not truthy
							return Ok(false);
						};
						rid
					}
					VectorId::RecordKey(key) => {
						RecordId::new(self.ikb.table().clone(), key.as_ref().clone())
					}
				};
				let rid = Arc::new(rid);
				// Is the record truthy?
				let record =
					Self::is_record_truthy(ctx, self.opt, stk, self.cond.clone(), rid.clone())
						.await?;
				let truthy = record.is_some();
				// Store the result in the cache
				let entry = record.map(|r| (rid, r));
				e.insert(entry);
				// Return the result
				Ok(truthy)
			}
		}
	}

	async fn is_record_truthy(
		ctx: &HnswContext<'_>,
		opt: &Options,
		stk: &mut Stk,
		cond: Arc<Cond>,
		rid: Arc<RecordId>,
	) -> Result<Option<Arc<Record>>> {
		let val = ctx.tx.get_record(ctx.ikb.0.ns, ctx.ikb.0.db, &rid.table, &rid.key, None).await?;
		if val.data.as_ref().is_nullish() {
			return Ok(None);
		}
		let cursor_doc = CursorDoc {
			rid: Some(rid.clone()),
			ir: None,
			doc: val.into(),
			fields_computed: false,
		};
		let truthy = stk
			.run(|stk| cond.0.compute(stk, ctx.ctx, opt, Some(&cursor_doc)))
			.await
			.catch_return()?
			.is_truthy();
		if truthy {
			return Ok(Some(cursor_doc.doc.into_read_only()));
		}
		Ok(None)
	}

	/// Remove a vector id that has been evicted from the knn result
	pub(super) fn expire(&mut self, id: &VectorId) {
		self.cache.remove(id);
	}

	/// Remove a list of vector ids that have been evicted from the knn result
	pub(super) fn expires(&mut self, ids: &[VectorId]) {
		for id in ids {
			self.cache.remove(id);
		}
	}

	/// Returns the locked HnswDocs and the cache
	pub(super) fn release(self) -> (RwLockReadGuard<'a, HnswDocs>, FilterCache) {
		(self.docs, self.cache)
	}
}
