use std::collections::HashSet;
use std::ops::BitAnd;
use std::sync::Arc;

use reblessive::tree::Stk;
use revision::{Revisioned, revisioned};
use roaring::RoaringTreemap;
use roaring::treemap::IntoIter;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::catalog::{self, DatabaseId, NamespaceId, Scoring, SearchParams};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::Idiom;
use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::docids::btdocids::BTreeDocIds;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::highlighter::{HighlightParams, Highlighter, Offseter};
use crate::idx::ft::offset::OffsetRecords;
use crate::idx::ft::{DocLength, TermFrequency};
use crate::idx::planner::iterators::MatchesHitsIterator;
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::IndexStores;
use crate::kvs::{KVValue, Key, Transaction, TransactionType};
use crate::val::{Object, RecordId, Value};

mod doclength;
mod offsets;
mod postings;
pub(crate) mod scorer;
pub(in crate::idx) mod termdocs;
pub(crate) mod terms;

use doclength::DocLengths;
use offsets::Offsets;
use postings::Postings;
use scorer::BM25Scorer;
use termdocs::{SearchTermDocs, SearchTermsDocs};
use terms::{SearchTerms, TermId, TermLen};

pub(in crate::idx) type TermIdList = Vec<Option<(TermId, TermLen)>>;

pub(in crate::idx) struct TermIdSet {
	set: HashSet<TermId>,
	has_unknown_terms: bool,
}

impl TermIdSet {
	/// If the query TermsSet contains terms that are unknown in the index
	/// of if there is no terms in the set then
	/// we are sure that it does not match any document
	pub(in crate::idx) fn is_matchable(&self) -> bool {
		!(self.has_unknown_terms || self.set.is_empty())
	}

	pub(in crate::idx) fn is_subset(&self, other: &TermIdSet) -> bool {
		if self.has_unknown_terms {
			return false;
		}
		self.set.is_subset(&other.set)
	}
}

pub(crate) struct SearchIndex {
	analyzer: Analyzer,
	index_key_base: IndexKeyBase,
	state: SearchIndexState,
	bm25: Option<Bm25Params>,
	highlighting: bool,
	doc_ids: Arc<RwLock<BTreeDocIds>>,
	doc_lengths: Arc<RwLock<DocLengths>>,
	postings: Arc<RwLock<Postings>>,
	terms: Arc<RwLock<SearchTerms>>,
	offsets: Offsets,
	term_docs: SearchTermDocs,
}

#[derive(Clone)]
pub(crate) struct Bm25Params {
	pub(in crate::idx) k1: f32,
	pub(in crate::idx) b: f32,
}

impl Default for Bm25Params {
	fn default() -> Self {
		Self {
			k1: 1.2,
			b: 0.75,
		}
	}
}

pub(crate) struct FtStatistics {
	doc_ids: BStatistics,
	terms: BStatistics,
	doc_lengths: BStatistics,
	postings: BStatistics,
}

impl From<FtStatistics> for Value {
	fn from(stats: FtStatistics) -> Self {
		let mut res = Object::default();
		res.insert("doc_ids".to_owned(), Value::from(stats.doc_ids));
		res.insert("terms".to_owned(), Value::from(stats.terms));
		res.insert("doc_lengths".to_owned(), Value::from(stats.doc_lengths));
		res.insert("postings".to_owned(), Value::from(stats.postings));
		Value::from(res)
	}
}

#[revisioned(revision = 1)]
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct SearchIndexState {
	total_docs_lengths: u128,
	doc_count: u64,
}

impl KVValue for SearchIndexState {
	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> anyhow::Result<Self> {
		Ok(Self::deserialize_revisioned(&mut val.as_slice())?)
	}
}

impl SearchIndex {
	pub(crate) async fn new(
		ctx: &Context,
		ns: NamespaceId,
		db: DatabaseId,
		az: &str,
		ikb: IndexKeyBase,
		p: &SearchParams,
		tt: TransactionType,
	) -> anyhow::Result<Self> {
		let tx = ctx.tx();
		let ixs = ctx.get_index_stores();
		let az = tx.get_db_analyzer(ns, db, az).await?;
		ixs.mappers().check(&az).await?;
		Self::with_analyzer(ixs, &tx, az, ikb, p, tt).await
	}

	async fn with_analyzer(
		ixs: &IndexStores,
		txn: &Transaction,
		az: Arc<catalog::AnalyzerDefinition>,
		index_key_base: IndexKeyBase,
		p: &SearchParams,
		tt: TransactionType,
	) -> anyhow::Result<Self> {
		let state_key = index_key_base.new_bs_key();
		let state: SearchIndexState = txn.get(&state_key, None).await?.unwrap_or_default();
		let doc_ids = Arc::new(RwLock::new(
			BTreeDocIds::new(txn, tt, index_key_base.clone(), p.doc_ids_order, p.doc_ids_cache)
				.await?,
		));
		let doc_lengths = Arc::new(RwLock::new(
			DocLengths::new(
				txn,
				index_key_base.clone(),
				p.doc_lengths_order,
				tt,
				p.doc_lengths_cache,
			)
			.await?,
		));
		let postings = Arc::new(RwLock::new(
			Postings::new(txn, index_key_base.clone(), p.postings_order, tt, p.postings_cache)
				.await?,
		));
		let terms = Arc::new(RwLock::new(
			SearchTerms::new(txn, index_key_base.clone(), p.terms_order, tt, p.terms_cache).await?,
		));
		let term_docs = SearchTermDocs::new(index_key_base.clone());
		let offsets = Offsets::new(index_key_base.clone());
		let mut bm25 = None;
		if let Scoring::Bm {
			k1,
			b,
		} = p.sc
		{
			bm25 = Some(Bm25Params {
				k1,
				b,
			});
		}
		let analyzer = Analyzer::new(ixs, az)?;
		Ok(Self {
			state,
			index_key_base,
			bm25,
			highlighting: p.hl,
			analyzer,
			doc_ids,
			doc_lengths,
			postings,
			terms,
			term_docs,
			offsets,
		})
	}

	pub(in crate::idx) fn doc_ids(&self) -> Arc<RwLock<BTreeDocIds>> {
		self.doc_ids.clone()
	}

	pub(in crate::idx) fn terms(&self) -> Arc<RwLock<SearchTerms>> {
		self.terms.clone()
	}

	pub(crate) async fn remove_document(
		&mut self,
		ctx: &Context,
		rid: &RecordId,
	) -> anyhow::Result<()> {
		let tx = ctx.tx();
		// Extract and remove the doc_id (if any)
		let mut doc_ids = self.doc_ids.write().await;
		let doc_id = doc_ids.remove_doc(&tx, rid).await?;
		drop(doc_ids);
		if let Some(doc_id) = doc_id {
			self.state.doc_count -= 1;

			// Remove the doc length
			let mut doc_lengths = self.doc_lengths.write().await;
			let dl = doc_lengths.remove_doc_length(&tx, doc_id).await?;
			drop(doc_lengths);
			if let Some(doc_lengths) = dl {
				self.state.total_docs_lengths -= doc_lengths as u128;
			}

			// Get the term list
			if let Some(term_list) = tx.get(&self.index_key_base.new_bk_key(doc_id), None).await? {
				// Remove the postings
				let mut p = self.postings.write().await;
				let mut t = self.terms.write().await;
				for term_id in &term_list {
					p.remove_posting(&tx, term_id, doc_id).await?;
					// if the term is not present in any document in the index, we can remove it
					let doc_count = self.term_docs.remove_doc(&tx, term_id, doc_id).await?;
					if doc_count == 0 {
						t.remove_term_id(&tx, term_id).await?;
					}
				}
				drop(p);
				drop(t);
				// Remove the offsets if any
				if self.highlighting {
					for term_id in term_list {
						// TODO?: Removal can be done with a prefix on doc_id
						self.offsets.remove_offsets(&tx, doc_id, term_id).await?;
					}
				}
			}
		}
		drop(tx);
		Ok(())
	}

	pub(crate) async fn index_document(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rid: &RecordId,
		content: Vec<Value>,
	) -> anyhow::Result<()> {
		let tx = ctx.tx();
		// Resolve the doc_id
		let resolved = {
			let mut doc_ids = self.doc_ids.write().await;
			doc_ids.resolve_doc_id(&tx, rid).await?
		};
		let doc_id = resolved.doc_id();

		// Extract the doc_lengths, terms en frequencies (and offset)
		let (doc_length, terms_and_frequencies, offsets) = if self.highlighting {
			let (dl, tf, ofs) =
				self.extract_terms_with_frequencies_with_offsets(stk, ctx, opt, content).await?;
			(dl, tf, Some(ofs))
		} else {
			let (dl, tf) = self.extract_terms_with_frequencies(stk, ctx, opt, content).await?;
			(dl, tf, None)
		};

		// Set the doc length
		{
			let mut dl = self.doc_lengths.write().await;
			if resolved.was_existing() {
				if let Some(old_doc_length) = dl.get_doc_length_mut(&tx, doc_id).await? {
					self.state.total_docs_lengths -= old_doc_length as u128;
				}
			}
			dl.set_doc_length(&tx, doc_id, doc_length).await?;
		}

		// Retrieve the existing terms for this document (if any)
		let term_ids_key = self.index_key_base.new_bk_key(doc_id);
		let mut old_term_ids = tx.get(&term_ids_key, None).await?;

		// Set the terms postings and term docs
		let mut terms_ids = RoaringTreemap::default();
		let mut p = self.postings.write().await;
		for (term_id, term_freq) in terms_and_frequencies {
			p.update_posting(&tx, term_id, doc_id, term_freq).await?;
			if let Some(old_term_ids) = &mut old_term_ids {
				old_term_ids.remove(term_id);
			}
			self.term_docs.set_doc(&tx, term_id, doc_id).await?;
			terms_ids.insert(term_id);
		}

		// Remove any remaining postings
		if let Some(old_term_ids) = &old_term_ids {
			let mut t = self.terms.write().await;
			for old_term_id in old_term_ids {
				p.remove_posting(&tx, old_term_id, doc_id).await?;
				let doc_count = self.term_docs.remove_doc(&tx, old_term_id, doc_id).await?;
				// if the term does not have anymore postings, we can remove the term
				if doc_count == 0 {
					t.remove_term_id(&tx, old_term_id).await?;
				}
			}
		}
		drop(p);

		if self.highlighting {
			// Set the offset if any
			if let Some(ofs) = offsets {
				if !ofs.is_empty() {
					for (tid, or) in ofs {
						self.offsets.set_offsets(&tx, doc_id, tid, or).await?;
					}
				}
			}
			// In case of an update, w remove the offset for the terms that does not exist
			// anymore
			if let Some(old_term_ids) = old_term_ids {
				for old_term_id in old_term_ids {
					self.offsets.remove_offsets(&tx, doc_id, old_term_id).await?;
				}
			}
		}

		// Stores the term list for this doc_id
		tx.set(&term_ids_key, &terms_ids, None).await?;

		// Update the index state
		self.state.total_docs_lengths += doc_length as u128;
		if !resolved.was_existing() {
			self.state.doc_count += 1;
		}

		// Update the states
		tx.set(&self.index_key_base.new_bs_key(), &self.state, None).await?;
		Ok(())
	}

	pub(in crate::idx) async fn extract_indexing_terms(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		content: Value,
	) -> anyhow::Result<TermIdSet> {
		let mut tv = Vec::new();
		self.analyzer
			.analyze_value(stk, ctx, opt, content, FilteringStage::Indexing, &mut tv)
			.await?;
		let mut set = HashSet::new();
		let mut has_unknown_terms = false;
		let tx = ctx.tx();
		let t = self.terms.read().await;
		for tokens in tv {
			for token in tokens.list() {
				if let Some(term_id) = t.get_term_id(&tx, tokens.get_token_string(token)?).await? {
					set.insert(term_id);
				} else {
					has_unknown_terms = true;
				}
			}
		}
		Ok(TermIdSet {
			set,
			has_unknown_terms,
		})
	}

	/// This method is used for indexing.
	/// It will create new term ids for non-already existing terms.
	async fn extract_terms_with_frequencies(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		field_content: Vec<Value>,
	) -> anyhow::Result<(DocLength, Vec<(TermId, TermFrequency)>)> {
		// Let's first collect all the inputs and collect the tokens.
		// We need to store them because everything after is zero-copy
		let inputs = self
			.analyzer
			.analyze_content(stk, ctx, opt, field_content, FilteringStage::Indexing)
			.await?;
		// We then collect every unique term and count the frequency
		let (dl, tf) = Analyzer::extract_frequencies(&inputs)?;
		// Now we can resolve the term ids
		let mut tfid = Vec::with_capacity(tf.len());
		let tx = ctx.tx();
		let mut terms = self.terms.write().await;
		for (t, f) in tf {
			tfid.push((terms.resolve_term_id(&tx, t).await?, f));
		}
		Ok((dl, tfid))
	}

	pub(in crate::idx) async fn extract_querying_terms(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		query_string: String,
	) -> anyhow::Result<(TermIdList, TermIdSet, SearchTermsDocs)> {
		let tokens = self
			.analyzer
			.generate_tokens(stk, ctx, opt, FilteringStage::Querying, query_string)
			.await?;
		// We extract the term ids
		let mut list = Vec::with_capacity(tokens.list().len());
		let mut unique_tokens = HashSet::new();
		let mut set = HashSet::new();
		let tx = ctx.tx();
		let mut has_unknown_terms = false;
		let t = self.terms.read().await;
		for token in tokens.list() {
			// Tokens can contain duplicates, not need to evaluate them again
			if unique_tokens.insert(token) {
				// Is the term known in the index?
				let opt_term_id = t.get_term_id(&tx, tokens.get_token_string(token)?).await?;
				list.push(opt_term_id.map(|tid| (tid, token.get_char_len())));
				if let Some(term_id) = opt_term_id {
					set.insert(term_id);
				} else {
					has_unknown_terms = true;
				}
			}
		}
		// We collect the term docs
		let mut terms_docs = Vec::with_capacity(list.len());
		for opt_term in &list {
			if let Some((term_id, _)) = opt_term {
				let docs = self.term_docs.get_docs(&tx, *term_id).await?;
				if let Some(docs) = docs {
					terms_docs.push(Some((*term_id, docs)));
				} else {
					terms_docs.push(Some((*term_id, RoaringTreemap::new())));
				}
			} else {
				terms_docs.push(None);
			}
		}
		Ok((
			list,
			TermIdSet {
				set,
				has_unknown_terms,
			},
			terms_docs,
		))
	}

	/// This method is used for indexing.
	/// It will create new term ids for non-already existing terms.
	async fn extract_terms_with_frequencies_with_offsets(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		content: Vec<Value>,
	) -> anyhow::Result<(DocLength, Vec<(TermId, TermFrequency)>, Vec<(TermId, OffsetRecords)>)> {
		// Let's first collect all the inputs, and collect the tokens.
		// We need to store them because everything after is zero-copy
		let inputs =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		// We then collect every unique term and count the frequency and extract the
		// offsets
		let (dl, tfos) = Analyzer::extract_offsets(&inputs)?;
		// Now we can resolve the term ids
		let mut tfid = Vec::with_capacity(tfos.len());
		let mut osid = Vec::with_capacity(tfos.len());
		let tx = ctx.tx();
		let mut terms = self.terms.write().await;
		for (t, o) in tfos {
			let id = terms.resolve_term_id(&tx, t).await?;
			tfid.push((id, o.len() as TermFrequency));
			osid.push((id, OffsetRecords(o)));
		}
		Ok((dl, tfid, osid))
	}

	pub(in crate::idx) fn new_hits_iterator(
		&self,
		terms_docs: &SearchTermsDocs,
	) -> anyhow::Result<Option<SearchHitsIterator>> {
		let mut hits: Option<RoaringTreemap> = None;
		for opt_term_docs in terms_docs {
			if let Some((_, term_docs)) = opt_term_docs {
				if let Some(h) = hits {
					hits = Some(h.bitand(term_docs));
				} else {
					hits = Some(term_docs.clone());
				}
			} else {
				return Ok(None);
			}
		}
		if let Some(hits) = hits {
			if !hits.is_empty() {
				return Ok(Some(SearchHitsIterator::new(self.index_key_base.clone(), hits)));
			}
		}
		Ok(None)
	}

	pub(in crate::idx) fn new_scorer(
		&self,
		terms_docs: Arc<SearchTermsDocs>,
	) -> anyhow::Result<Option<BM25Scorer>> {
		if let Some(bm25) = &self.bm25 {
			return Ok(Some(BM25Scorer::new(
				self.postings.clone(),
				terms_docs,
				self.doc_lengths.clone(),
				self.state.total_docs_lengths,
				self.state.doc_count,
				bm25.k1,
				bm25.b,
			)));
		}
		Ok(None)
	}

	pub(in crate::idx) async fn highlight(
		&self,
		tx: &Transaction,
		thg: &RecordId,
		terms: &[Option<(TermId, TermLen)>],
		hlp: HighlightParams,
		idiom: &Idiom,
		doc: &Value,
	) -> anyhow::Result<Value> {
		let doc_key: Key = revision::to_vec(thg)?;
		let di = self.doc_ids.read().await;
		let doc_id = di.get_doc_id(tx, doc_key).await?;
		drop(di);
		if let Some(doc_id) = doc_id {
			let mut hl = Highlighter::new(hlp, idiom, doc);
			for (term_id, term_len) in terms.iter().flatten() {
				let o = self.offsets.get_offsets(tx, doc_id, *term_id).await?;
				if let Some(o) = o {
					hl.highlight(*term_len, o.0);
				}
			}
			return hl.try_into();
		}
		Ok(Value::None)
	}

	pub(in crate::idx) async fn read_offsets(
		&self,
		tx: &Transaction,
		thg: &RecordId,
		terms: &[Option<(TermId, u32)>],
		partial: bool,
	) -> anyhow::Result<Value> {
		let doc_key: Key = revision::to_vec(thg)?;
		let doc_id = {
			let di = self.doc_ids.read().await;
			di.get_doc_id(tx, doc_key).await?
		};
		if let Some(doc_id) = doc_id {
			let mut or = Offseter::new(partial);
			for (term_id, term_len) in terms.iter().flatten() {
				let o = self.offsets.get_offsets(tx, doc_id, *term_id).await?;
				if let Some(o) = o {
					or.highlight(*term_len, o.0);
				}
			}
			return or.try_into().map_err(anyhow::Error::new);
		}
		Ok(Value::None)
	}

	pub(crate) async fn statistics(&self, ctx: &Context) -> anyhow::Result<FtStatistics> {
		let txn = ctx.tx();
		let res = FtStatistics {
			doc_ids: self.doc_ids.read().await.statistics(&txn).await?,
			terms: self.terms.read().await.statistics(&txn).await?,
			doc_lengths: self.doc_lengths.read().await.statistics(&txn).await?,
			postings: self.postings.read().await.statistics(&txn).await?,
		};
		Ok(res)
	}

	pub(crate) async fn finish(&self, ctx: &Context) -> anyhow::Result<()> {
		let txn = ctx.tx();
		self.doc_ids.write().await.finish(&txn).await?;
		self.doc_lengths.write().await.finish(&txn).await?;
		self.postings.write().await.finish(&txn).await?;
		self.terms.write().await.finish(&txn).await?;
		Ok(())
	}
}

pub(crate) struct SearchHitsIterator {
	ikb: IndexKeyBase,
	iter: IntoIter,
}

impl SearchHitsIterator {
	fn new(ikb: IndexKeyBase, hits: RoaringTreemap) -> Self {
		Self {
			ikb,
			iter: hits.into_iter(),
		}
	}
}

impl MatchesHitsIterator for SearchHitsIterator {
	#[cfg(target_pointer_width = "64")]
	fn len(&self) -> usize {
		self.iter.len()
	}
	#[cfg(not(target_pointer_width = "64"))]
	fn len(&self) -> usize {
		self.iter.size_hint().0
	}

	async fn next(&mut self, tx: &Transaction) -> anyhow::Result<Option<(RecordId, DocId)>> {
		for doc_id in self.iter.by_ref() {
			let doc_id_key = self.ikb.new_bi_key(doc_id);
			if let Some(v) = tx.get(&doc_id_key, None).await? {
				return Ok(Some((v, doc_id)));
			}
		}
		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;
	use std::sync::Arc;

	use reblessive::tree::Stk;
	use test_log::test;

	use crate::catalog::{self, DatabaseId, NamespaceId, SearchParams};
	use crate::ctx::{Context, MutableContext};
	use crate::dbs::Options;
	use crate::expr::statements::DefineAnalyzerStatement;
	use crate::idx::IndexKeyBase;
	use crate::idx::ft::Score;
	use crate::idx::ft::search::scorer::BM25Scorer;
	use crate::idx::ft::search::{SearchHitsIterator, SearchIndex};
	use crate::idx::planner::iterators::MatchesHitsIterator;
	use crate::kvs::LockType::*;
	use crate::kvs::{Datastore, TransactionType};
	use crate::sql::Expr;
	use crate::sql::statements::DefineStatement;
	use crate::syn;
	use crate::val::{Array, RecordId, Value};

	async fn check_hits(
		ctx: &Context,
		hits: Option<SearchHitsIterator>,
		scr: BM25Scorer,
		e: Vec<(&RecordId, Option<Score>)>,
	) {
		let tx = ctx.tx();
		if let Some(mut hits) = hits {
			let mut map = HashMap::new();
			while let Some((k, d)) = hits.next(&tx).await.unwrap() {
				yield_now!();
				let s = scr.score(&tx, d).await.unwrap();
				map.insert(k, s);
			}
			assert_eq!(map.len(), e.len());
			for (k, p) in e {
				assert_eq!(map.get(k), p.as_ref(), "{}", k);
			}
		} else {
			panic!("hits is none");
		}
	}

	async fn search(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		si: &SearchIndex,
		qs: &str,
	) -> (Option<SearchHitsIterator>, BM25Scorer) {
		let (_, _, terms_docs) =
			si.extract_querying_terms(stk, ctx, opt, qs.to_string()).await.unwrap();
		let terms_docs = Arc::new(terms_docs);
		let scr = si.new_scorer(terms_docs.clone()).unwrap().unwrap();
		let hits = si.new_hits_iterator(&terms_docs).unwrap();
		(hits, scr)
	}

	pub(super) async fn tx_fti(
		ctx: &Context,
		ds: &Datastore,
		tt: TransactionType,
		az: Arc<catalog::AnalyzerDefinition>,
		order: u32,
		hl: bool,
	) -> (Context, Options, SearchIndex) {
		let mut ctx = MutableContext::new(ctx);
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let p = SearchParams {
			az: az.name.clone(),
			doc_ids_order: order,
			doc_lengths_order: order,
			postings_order: order,
			terms_order: order,
			sc: Default::default(),
			hl,
			doc_ids_cache: 100,
			doc_lengths_cache: 100,
			postings_cache: 100,
			terms_cache: 100,
		};
		let fti = SearchIndex::with_analyzer(
			ctx.get_index_stores(),
			&tx,
			az,
			IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"),
			&p,
			tt,
		)
		.await
		.unwrap();
		let txn = Arc::new(tx);
		ctx.set_transaction(txn);
		(ctx.freeze(), Options::default(), fti)
	}

	pub(super) async fn finish(ctx: &Context, fti: SearchIndex) {
		fti.finish(ctx).await.unwrap();
		let tx = ctx.tx();
		tx.commit().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_ft_index() {
		let ds = Datastore::new("memory").await.unwrap();
		let ctx = ds.setup_ctx().unwrap().freeze();
		let q = syn::expr("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
		let Expr::Define(q) = q else {
			panic!()
		};
		let DefineStatement::Analyzer(az) = *q else {
			panic!()
		};
		let az = Arc::new(DefineAnalyzerStatement::from(az).to_definition());
		let mut stack = reblessive::TreeStack::new();

		let btree_order = 5;

		let doc1 = RecordId::new("t".to_string(), strand!("doc1").to_owned());
		let doc2 = RecordId::new("t".to_string(), strand!("doc2").to_owned());
		let doc3 = RecordId::new("t".to_string(), strand!("doc3").to_owned());

		stack
			.enter(|stk| async {
				// Add one document
				let (ctx, opt, mut fti) =
					tx_fti(&ctx, &ds, TransactionType::Write, az.clone(), btree_order, false).await;
				fti.index_document(stk, &ctx, &opt, &doc1, vec![Value::from("hello the world")])
					.await
					.unwrap();
				finish(&ctx, fti).await;
			})
			.finish()
			.await;

		stack
			.enter(|stk| async {
				// Add two documents
				let (ctx, opt, mut fti) =
					tx_fti(&ctx, &ds, TransactionType::Write, az.clone(), btree_order, false).await;
				fti.index_document(stk, &ctx, &opt, &doc2, vec![Value::from("a yellow hello")])
					.await
					.unwrap();
				fti.index_document(stk, &ctx, &opt, &doc3, vec![Value::from("foo bar")])
					.await
					.unwrap();
				finish(&ctx, fti).await;
			})
			.finish()
			.await;

		stack
			.enter(|stk| async {
				let (ctx, opt, fti) =
					tx_fti(&ctx, &ds, TransactionType::Read, az.clone(), btree_order, false).await;
				// Check the statistics
				let statistics = fti.statistics(&ctx).await.unwrap();
				assert_eq!(statistics.terms.keys_count, 7);
				assert_eq!(statistics.postings.keys_count, 8);
				assert_eq!(statistics.doc_ids.keys_count, 3);
				assert_eq!(statistics.doc_lengths.keys_count, 3);

				// Search & score
				let (hits, scr) = search(stk, &ctx, &opt, &fti, "hello").await;
				check_hits(
					&ctx,
					hits,
					scr,
					vec![(&doc1, Some(-0.4859746)), (&doc2, Some(-0.4859746))],
				)
				.await;

				let (hits, scr) = search(stk, &ctx, &opt, &fti, "world").await;
				check_hits(&ctx, hits, scr, vec![(&doc1, Some(0.4859746))]).await;

				let (hits, scr) = search(stk, &ctx, &opt, &fti, "yellow").await;
				check_hits(&ctx, hits, scr, vec![(&doc2, Some(0.4859746))]).await;

				let (hits, scr) = search(stk, &ctx, &opt, &fti, "foo").await;
				check_hits(&ctx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

				let (hits, scr) = search(stk, &ctx, &opt, &fti, "bar").await;
				check_hits(&ctx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

				let (hits, _) = search(stk, &ctx, &opt, &fti, "dummy").await;
				assert!(hits.is_none());
			})
			.finish()
			.await;

		stack
			.enter(|stk| async {
				// Reindex one document
				let (ctx, opt, mut fti) =
					tx_fti(&ctx, &ds, TransactionType::Write, az.clone(), btree_order, false).await;
				fti.index_document(stk, &ctx, &opt, &doc3, vec![Value::from("nobar foo")])
					.await
					.unwrap();
				finish(&ctx, fti).await;

				let (ctx, opt, fti) =
					tx_fti(&ctx, &ds, TransactionType::Read, az.clone(), btree_order, false).await;

				// We can still find 'foo'
				let (hits, scr) = search(stk, &ctx, &opt, &fti, "foo").await;
				check_hits(&ctx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

				// We can't anymore find 'bar'
				let (hits, _) = search(stk, &ctx, &opt, &fti, "bar").await;
				assert!(hits.is_none());

				// We can now find 'nobar'
				let (hits, scr) = search(stk, &ctx, &opt, &fti, "nobar").await;
				check_hits(&ctx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;
			})
			.finish()
			.await;

		{
			// Remove documents
			let (ctx, _, mut fti) =
				tx_fti(&ctx, &ds, TransactionType::Write, az.clone(), btree_order, false).await;
			fti.remove_document(&ctx, &doc1).await.unwrap();
			fti.remove_document(&ctx, &doc2).await.unwrap();
			fti.remove_document(&ctx, &doc3).await.unwrap();
			finish(&ctx, fti).await;
		}

		stack
			.enter(|stk| async {
				let (ctx, opt, fti) =
					tx_fti(&ctx, &ds, TransactionType::Read, az.clone(), btree_order, false).await;
				let (hits, _) = search(stk, &ctx, &opt, &fti, "hello").await;
				assert!(hits.is_none());
				let (hits, _) = search(stk, &ctx, &opt, &fti, "foo").await;
				assert!(hits.is_none());
			})
			.finish()
			.await;
	}

	async fn test_ft_index_bm_25(hl: bool) {
		// The function `extract_sorted_terms_with_frequencies` is non-deterministic.
		// the inner structures (BTrees) are built with the same terms and frequencies,
		// but the insertion order is different, ending up in different BTree
		// structures. Therefore it makes sense to do multiple runs.
		for _ in 0..10 {
			let ds = Datastore::new("memory").await.unwrap();
			let ctx = ds.setup_ctx().unwrap().freeze();
			let q = syn::expr("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
			let Expr::Define(q) = q else {
				panic!()
			};
			let DefineStatement::Analyzer(az) = *q else {
				panic!()
			};
			let az = Arc::new(DefineAnalyzerStatement::from(az).to_definition());
			let mut stack = reblessive::TreeStack::new();

			let doc1 = RecordId::new("t".to_string(), strand!("doc1").to_owned());
			let doc2 = RecordId::new("t".to_string(), strand!("doc2").to_owned());
			let doc3 = RecordId::new("t".to_string(), strand!("doc3").to_owned());
			let doc4 = RecordId::new("t".to_string(), strand!("doc4").to_owned());

			let btree_order = 5;
			stack
				.enter(|stk| async {
					let (ctx, opt, mut fti) =
						tx_fti(&ctx, &ds, TransactionType::Write, az.clone(), btree_order, hl)
							.await;
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&doc1,
						vec![Value::from("the quick brown fox jumped over the lazy dog")],
					)
					.await
					.unwrap();
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&doc2,
						vec![Value::from("the fast fox jumped over the lazy dog")],
					)
					.await
					.unwrap();
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&doc3,
						vec![Value::from("the dog sat there and did nothing")],
					)
					.await
					.unwrap();
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&doc4,
						vec![Value::from("the other animals sat there watching")],
					)
					.await
					.unwrap();
					finish(&ctx, fti).await;
				})
				.finish()
				.await;

			stack
				.enter(|stk| async {
					let (ctx, opt, fti) =
						tx_fti(&ctx, &ds, TransactionType::Read, az.clone(), btree_order, hl).await;

					let statistics = fti.statistics(&ctx).await.unwrap();
					assert_eq!(statistics.terms.keys_count, 17);
					assert_eq!(statistics.postings.keys_count, 28);
					assert_eq!(statistics.doc_ids.keys_count, 4);
					assert_eq!(statistics.doc_lengths.keys_count, 4);

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "the").await;
					check_hits(
						&ctx,
						hits,
						scr,
						vec![
							(&doc1, Some(-3.4388628)),
							(&doc2, Some(-3.621457)),
							(&doc3, Some(-2.258829)),
							(&doc4, Some(-2.393017)),
						],
					)
					.await;

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "dog").await;
					check_hits(
						&ctx,
						hits,
						scr,
						vec![
							(&doc1, Some(-0.7832165)),
							(&doc2, Some(-0.8248031)),
							(&doc3, Some(-0.87105393)),
						],
					)
					.await;

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "fox").await;
					check_hits(&ctx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "over").await;
					check_hits(&ctx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "lazy").await;
					check_hits(&ctx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "jumped").await;
					check_hits(&ctx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "nothing").await;
					check_hits(&ctx, hits, scr, vec![(&doc3, Some(0.87105393))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &fti, "animals").await;
					check_hits(&ctx, hits, scr, vec![(&doc4, Some(0.92279965))]).await;

					let (hits, _) = search(stk, &ctx, &opt, &fti, "dummy").await;
					assert!(hits.is_none());
				})
				.finish()
				.await;
		}
	}

	#[test(tokio::test)]
	async fn test_ft_index_bm_25_without_highlighting() {
		test_ft_index_bm_25(false).await;
	}

	#[test(tokio::test)]
	async fn test_ft_index_bm_25_with_highlighting() {
		test_ft_index_bm_25(true).await;
	}

	#[test(tokio::test)]
	async fn remove_insert_sequence() {
		let ds = Datastore::new("memory").await.unwrap();
		let ctx = ds.setup_ctx().unwrap().freeze();
		let mut stack = reblessive::TreeStack::new();
		let q = syn::expr("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
		let Expr::Define(q) = q else {
			panic!()
		};
		let DefineStatement::Analyzer(az) = *q else {
			panic!()
		};
		let az = Arc::new(DefineAnalyzerStatement::from(az).to_definition());
		let doc = RecordId::new("t".to_string(), strand!("doc1").to_owned());
		let content = Value::from(Array::from(vec![
			"Enter a search term",
			"Welcome",
			"Docusaurus blogging features are powered by the blog plugin.",
			"Simply add Markdown files (or folders) to the blog directory.",
			"blog",
			"Regular blog authors can be added to authors.yml.",
			"authors.yml",
			"The blog post date can be extracted from filenames, such as:",
			"2019-05-30-welcome.md",
			"2019-05-30-welcome/index.md",
			"A blog post folder can be convenient to co-locate blog post images:",
			"The blog supports tags as well!",
			"And if you don't want a blog: just delete this directory, and use blog: false in your Docusaurus config.",
			"blog: false",
			"MDX Blog Post",
			"Blog posts support Docusaurus Markdown features, such as MDX.",
			"Use the power of React to create interactive blog posts.",
			"Long Blog Post",
			"This is the summary of a very long blog post,",
			"Use a <!-- truncate --> comment to limit blog post size in the list view.",
			"<!--",
			"truncate",
			"-->",
			"First Blog Post",
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque elementum dignissim ultricies. Fusce rhoncus ipsum tempor eros aliquam consequat. Lorem ipsum dolor sit amet",
		]));

		for i in 0..5 {
			debug!("Attempt {i}");
			{
				let (ctx, opt, mut fti) =
					tx_fti(&ctx, &ds, TransactionType::Write, az.clone(), 5, false).await;
				stack
					.enter(|stk| fti.index_document(stk, &ctx, &opt, &doc, vec![content.clone()]))
					.finish()
					.await
					.unwrap();
				finish(&ctx, fti).await;
			}

			{
				let (ctx, _, fti) =
					tx_fti(&ctx, &ds, TransactionType::Read, az.clone(), 5, false).await;
				let s = fti.statistics(&ctx).await.unwrap();
				assert_eq!(s.terms.keys_count, 113);
			}

			{
				let (ctx, _, mut fti) =
					tx_fti(&ctx, &ds, TransactionType::Write, az.clone(), 5, false).await;
				fti.remove_document(&ctx, &doc).await.unwrap();
				finish(&ctx, fti).await;
			}

			{
				let (ctx, _, fti) =
					tx_fti(&ctx, &ds, TransactionType::Read, az.clone(), 5, false).await;
				let s = fti.statistics(&ctx).await.unwrap();
				assert_eq!(s.terms.keys_count, 0);
			}
		}
	}
}
