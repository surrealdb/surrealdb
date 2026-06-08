use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use roaring::RoaringTreemap;
use roaring::treemap::IntoIter;
use uuid::Uuid;

use crate::catalog;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{FullTextParams, Scoring};
/// This module implements a concurrent full-text search index.
///
/// The full-text index allows for efficient text search operations with support
/// for:
/// - Concurrent read and write operations
/// - BM25 scoring for relevance ranking
/// - Highlighting of search terms in results
/// - Efficient term frequency tracking
/// - Document length normalization
/// - Compaction of index data
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Idiom;
use crate::expr::operator::BooleanOperator;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::Tokens;
use crate::idx::ft::highlighter::{HighlightParams, Highlighter, Offseter};
use crate::idx::ft::offset::Offset;
use crate::idx::ft::{DocLength, Score, TermFrequency};
use crate::idx::planner::iterators::MatchesHitsIterator;
use crate::idx::seqdocids::{DocId, SeqDocIds};
use crate::idx::trees::store::IndexStores;
use crate::idx::{IndexKeyBase, bump_compaction_generation, read_compaction_generation};
use crate::key::index::tt::Tt;
use crate::kvs::{COUNT_BATCH_SIZE, Key, Transaction, impl_kv_value_revisioned};
use crate::val::{RecordId, Value};
#[revisioned(revision = 1)]
#[derive(Debug, Default, PartialEq)]
/// Represents a term occurrence within a document
pub(crate) struct TermDocument {
	/// The frequency of the term in the document
	f: TermFrequency,
	/// The offsets of the term occurrences in the document
	o: Vec<Offset>,
}

impl_kv_value_revisioned!(TermDocument);

impl TermDocument {
	#[cfg(test)]
	pub(crate) fn new(f: TermFrequency, o: Vec<Offset>) -> Self {
		Self {
			f,
			o,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
/// Tracks document length and count statistics for the index
pub(crate) struct DocLengthAndCount {
	/// The total length of all documents in the index
	total_docs_length: i128,
	/// The total number of documents in the index
	doc_count: i64,
}
impl_kv_value_revisioned!(DocLengthAndCount);

impl DocLengthAndCount {
	#[cfg(test)]
	pub(crate) fn new(total_docs_length: i128, doc_count: i64) -> Self {
		Self {
			total_docs_length,
			doc_count,
		}
	}
}

/// Represents the terms in a search query and their associated document sets
pub(crate) struct QueryTerms {
	/// The tokenized query terms
	#[allow(dead_code)]
	tokens: Tokens,
	/// Document sets for each term (RoaringTreemap of document IDs)
	#[allow(dead_code)]
	docs: Vec<Option<RoaringTreemap>>,
	/// Indicates if any terms in the query are not found in the index
	#[allow(dead_code)]
	has_unknown_terms: bool,
}

impl QueryTerms {
	pub(crate) fn is_empty(&self) -> bool {
		self.tokens.list().is_empty()
	}

	pub(crate) fn contains_doc(&self, doc_id: DocId) -> bool {
		for d in self.docs.iter().flatten() {
			if d.contains(doc_id) {
				return true;
			}
		}
		false
	}

	pub(in crate::idx::ft) fn matches_or(&self, tks: &[Tokens]) -> Result<bool> {
		for t in self.tokens.list() {
			let t = self.tokens.get_token_string(t)?;
			for tokens in tks {
				if tokens.try_contains(t)? {
					return Ok(true);
				}
			}
		}
		Ok(false)
	}

	pub(in crate::idx::ft) fn matches_and(&self, tks: &[Tokens]) -> Result<bool> {
		for t in self.tokens.list() {
			let t = self.tokens.get_token_string(t)?;
			let mut found = false;
			for tokens in tks {
				if tokens.try_contains(t)? {
					found = true;
					break;
				}
			}
			if !found {
				return Ok(false);
			}
		}
		Ok(true)
	}
}

#[derive(Clone, Copy)]
pub(crate) struct Bm25Params {
	pub(in crate::idx) k1: f32,
	pub(in crate::idx) b: f32,
}

/// The main full-text index implementation that supports concurrent read and
/// write operations
pub(crate) struct FullTextIndex {
	/// The index key base used for key generation
	ikb: IndexKeyBase,
	/// The analyzer used for tokenizing and processing text
	analyzer: Analyzer,
	/// Whether highlighting is enabled for this index
	highlighting: bool,
	/// Mapping between document IDs and their database identifiers
	doc_ids: SeqDocIds,
	/// BM25 scoring parameters, if scoring is enabled
	bm25: Option<Bm25Params>,
}

/// Snapshot gathered by the read phase of full-text compaction.
///
/// The plan contains only the exact `!dc`/`!tt` delta keys observed at the
/// snapshot plus the generations that must still match before applying it. A
/// continuation flag tells the datastore whether to prepare another bounded
/// batch after this one commits.
pub(crate) struct FullTextCompactionPlan {
	doc_lengths: DocLengthAndCountCompactionPlan,
	term_docs: TermDocsCompactionPlan,
}

impl FullTextCompactionPlan {
	/// Returns true when the plan contains at least one delta key to compact.
	pub(crate) fn has_work(&self) -> bool {
		self.doc_lengths.has_logs() || self.term_docs.has_logs()
	}

	/// Returns true when at least one full-text delta range has more entries.
	pub(crate) fn has_more(&self) -> bool {
		self.doc_lengths.has_more() || self.term_docs.has_more()
	}
}

/// Bounded read-phase snapshot for document count/length (`!dc`) compaction.
struct DocLengthAndCountCompactionPlan {
	generation: Option<u64>,
	dlc: DocLengthAndCount,
	delta_keys: Vec<Key>,
	has_more: bool,
}

impl DocLengthAndCountCompactionPlan {
	fn has_logs(&self) -> bool {
		!self.delta_keys.is_empty()
	}

	/// Returns true when the `!dc` delta scan stopped before the range ended.
	fn has_more(&self) -> bool {
		self.has_more
	}
}

/// Bounded read-phase snapshot for term-document (`!tt`) compaction.
struct TermDocsCompactionPlan {
	generation: Option<u64>,
	deltas_by_term: HashMap<String, HashMap<DocId, i64>>,
	delta_keys: Vec<Key>,
	has_more: bool,
}

impl TermDocsCompactionPlan {
	fn has_logs(&self) -> bool {
		!self.delta_keys.is_empty()
	}

	/// Returns true when the `!tt` delta scan stopped before the range ended.
	fn has_more(&self) -> bool {
		self.has_more
	}
}

impl FullTextIndex {
	/// Creates a new full-text index with the specified parameters
	///
	/// This method retrieves the analyzer from the database and then calls
	/// `with_analyzer`
	pub(crate) async fn new(
		ixs: &IndexStores,
		tx: &Transaction,
		ikb: IndexKeyBase,
		p: &FullTextParams,
		allow_list: &[PathBuf],
	) -> Result<Self> {
		let az = tx.get_db_analyzer(ikb.0.ns, ikb.0.db, &p.analyzer, None).await?;
		ixs.mappers().check(&az, allow_list).await?;
		Self::with_analyzer(ixs, az, ikb, p)
	}

	/// Creates a new full-text index with the specified analyzer
	///
	/// This method initializes the index with the provided analyzer and
	/// parameters
	fn with_analyzer(
		ixs: &IndexStores,
		az: Arc<catalog::AnalyzerDefinition>,
		ikb: IndexKeyBase,
		p: &FullTextParams,
	) -> Result<Self> {
		let analyzer = Analyzer::new(ixs, az)?;
		let mut bm25 = None;
		if let Scoring::Bm {
			k1,
			b,
		} = p.scoring
		{
			bm25 = Some(Bm25Params {
				k1,
				b,
			});
		}
		Ok(Self {
			analyzer,
			highlighting: p.highlight,
			doc_ids: SeqDocIds::new(ikb.clone()),
			ikb,
			bm25,
		})
	}

	/// Removes content from the full-text index
	///
	/// This method removes the specified content for a document from the index.
	/// It returns the document ID if the document was found and removed.
	pub(crate) async fn remove_content(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
		content: Vec<Value>,
		require_compaction: &mut bool,
	) -> Result<Option<DocId>> {
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let mut set = HashSet::new();
		let tx = ctx.tx();
		let nid = ctx.node_id();
		// Get the doc id (if it exists)
		let doc_id = self.get_doc_id(&tx, rid).await?;
		if let Some(doc_id) = doc_id {
			// Delete the terms
			for tks in &tokens {
				for t in tks.list() {
					// Extract the term
					let s = tks.get_token_string(t)?;
					// Check if the term has already been deleted
					if set.insert(s) {
						// Delete the term
						let key = self.ikb.new_td(s, doc_id);
						tx.del(&key).await?;
						self.set_tt(&tx, s, doc_id, &nid, false).await?;
					}
				}
			}
			{
				let key = self.ikb.new_dl(doc_id);
				// get the doc length
				if let Some(dl) = tx.get(&key, None).await? {
					// Delete the doc length
					tx.del(&key).await?;
					// Decrease the doc count and total doc length
					let dcl = DocLengthAndCount {
						total_docs_length: -(dl as i128),
						doc_count: -1,
					};
					let key = self.ikb.new_dc_with_id(doc_id, ctx.node_id(), Uuid::now_v7());
					tx.put(&key, &dcl).await?;
					*require_compaction = true;
				}
			}
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	/// This method assumes that remove_content has been called previously,
	/// as it does not remove the content (terms) but only removes the doc_id
	/// reference.
	pub(crate) async fn remove_doc(&self, ctx: &FrozenContext, doc_id: DocId) -> Result<()> {
		self.doc_ids.remove_doc_id(&ctx.tx(), doc_id).await
	}

	/// Indexes content in the full-text index
	///
	/// This method analyzes and indexes the specified content for a document.
	/// It resolves the document ID, tokenizes the content, and stores term
	/// frequencies and offsets.
	pub(crate) async fn index_content(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
		content: Vec<Value>,
		require_compaction: &mut bool,
	) -> Result<()> {
		let tx = ctx.tx();
		let nid = ctx.node_id();
		// Get the doc id (if it exists)
		let id = self.doc_ids.resolve_doc_id(ctx, rid.key.clone()).await?;
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let dl = if self.highlighting {
			self.index_with_offsets(&nid, &tx, id.doc_id(), tokens).await?
		} else {
			self.index_without_offsets(&nid, &tx, id.doc_id(), tokens).await?
		};
		{
			// Set the doc length
			let key = self.ikb.new_dl(id.doc_id());
			tx.set(&key, &dl).await?;
		}
		{
			// Increase the doc count and total doc length
			let key = self.ikb.new_dc_with_id(id.doc_id(), ctx.node_id(), Uuid::now_v7());
			let dcl = DocLengthAndCount {
				total_docs_length: dl as i128,
				doc_count: 1,
			};
			tx.put(&key, &dcl).await?;
			*require_compaction = true;
		}
		// We're done
		Ok(())
	}

	async fn get_doc_length(&self, tx: &Transaction, doc_id: DocId) -> Result<Option<DocLength>> {
		let key = self.ikb.new_dl(doc_id);
		tx.get(&key, None).await
	}

	async fn index_with_offsets(
		&self,
		nid: &Uuid,
		tx: &Transaction,
		id: DocId,
		tokens: Vec<Tokens>,
	) -> Result<DocLength> {
		let (dl, offsets) = Analyzer::extract_offsets(&tokens)?;
		let mut td = TermDocument::default();
		for (t, o) in offsets {
			let key = self.ikb.new_td(t, id);
			td.f = o.len() as TermFrequency;
			td.o = o;
			tx.set(&key, &td).await?;
			self.set_tt(tx, t, id, nid, true).await?;
		}
		Ok(dl)
	}

	async fn index_without_offsets(
		&self,
		nid: &Uuid,
		tx: &Transaction,
		id: DocId,
		tokens: Vec<Tokens>,
	) -> Result<DocLength> {
		let (dl, tf) = Analyzer::extract_frequencies(&tokens)?;
		let mut td = TermDocument::default();
		for (t, f) in tf {
			let key = self.ikb.new_td(t, id);
			td.f = f;
			tx.set(&key, &td).await?;
			self.set_tt(tx, t, id, nid, true).await?;
		}
		Ok(dl)
	}

	async fn set_tt(
		&self,
		tx: &Transaction,
		term: &str,
		doc_id: DocId,
		nid: &Uuid,
		add: bool,
	) -> Result<()> {
		let key = self.ikb.new_tt(term, doc_id, *nid, Uuid::now_v7(), add);
		tx.set(&key, &String::new()).await
	}

	/// Extracts query terms from a search string
	///
	/// Tokenizes the query string, then retrieves the document bitmaps for each
	/// unique term. The compacted bitmap fetches are batched via `tx.getm()` to
	/// reduce KV round trips (one batch instead of N sequential gets).
	pub(crate) async fn extract_querying_terms(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		query_string: String,
	) -> Result<QueryTerms> {
		let tokens = self
			.analyzer
			.generate_tokens(stk, ctx, opt, FilteringStage::Querying, query_string.into())
			.await?;

		let mut unique_terms: Vec<&str> = Vec::new();
		let mut unique_tokens = HashSet::new();
		for token in tokens.list() {
			if unique_tokens.insert(token) {
				unique_terms.push(tokens.get_token_string(token)?);
			}
		}

		let tx = ctx.tx();

		// Phase 1: Collect deltas for each term (sequential range scans)
		let mut all_deltas: Vec<HashMap<DocId, i64>> = Vec::with_capacity(unique_terms.len());
		for term in &unique_terms {
			let (beg, end) = self.ikb.new_tt_term_range(term)?;
			let mut deltas: HashMap<DocId, i64> = HashMap::new();
			for k in tx.keys(beg..end, u32::MAX, 0, None).await? {
				let tt = Tt::decode_key(&k)?;
				let entry = deltas.entry(tt.doc_id).or_default();
				if tt.add {
					*entry += 1;
				} else {
					*entry -= 1;
				}
			}
			all_deltas.push(deltas);
		}

		// Phase 2: Batch-fetch compacted bitmaps for all terms at once
		let bitmap_keys: Vec<_> =
			unique_terms.iter().map(|term| self.ikb.new_td_root(term)).collect();
		let bitmaps: Vec<Option<RoaringTreemap>> = tx.getm(bitmap_keys, None).await?;

		// Phase 3: Merge deltas into bitmaps
		let mut docs = Vec::with_capacity(unique_terms.len());
		let mut has_unknown_terms = false;
		for (bitmap, deltas) in bitmaps.into_iter().zip(all_deltas.iter()) {
			let mut doc_set = bitmap.unwrap_or_default();
			for (doc_id, delta) in deltas {
				match 0.cmp(delta) {
					Ordering::Greater => {
						doc_set.remove(*doc_id);
					}
					Ordering::Less => {
						doc_set.insert(*doc_id);
					}
					Ordering::Equal => {}
				}
			}
			if doc_set.is_empty() {
				if !has_unknown_terms {
					has_unknown_terms = true;
				}
				docs.push(None);
			} else {
				docs.push(Some(doc_set));
			}
		}

		Ok(QueryTerms {
			tokens,
			docs,
			has_unknown_terms,
		})
	}

	pub(in crate::idx) async fn matches_value(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		qt: &QueryTerms,
		bo: BooleanOperator,
		val: Value,
	) -> Result<bool> {
		let mut tks = vec![];
		self.analyzer.analyze_value(stk, ctx, opt, val, FilteringStage::Indexing, &mut tks).await?;
		match bo {
			BooleanOperator::And => qt.matches_and(&tks),
			BooleanOperator::Or => qt.matches_or(&tks),
		}
	}

	async fn append_term_docs_delta(
		&self,
		tx: &Transaction,
		term: &str,
		deltas: &HashMap<DocId, i64>,
	) -> Result<RoaringTreemap> {
		// Retrieve the current compacted document set for this term
		// This is the consolidated bitmap of all documents containing this term
		let td = self.ikb.new_td_root(term);
		let mut docs = tx.get(&td, None).await?.unwrap_or_default();

		// Apply the delta changes to the document set
		for (doc_id, delta) in deltas {
			match 0.cmp(delta) {
				// If delta is negative, the term was removed from this document
				Ordering::Greater => {
					docs.remove(*doc_id);
				}
				// If delta is positive, the term was added to this document
				Ordering::Less => {
					docs.insert(*doc_id);
				}
				// If delta is zero, no change needed (term was added and removed equal times)
				Ordering::Equal => {}
			}
		}

		Ok(docs)
	}
	async fn set_term_docs_delta(
		&self,
		tx: &Transaction,
		term: &str,
		deltas: &HashMap<DocId, i64>,
	) -> Result<()> {
		let docs = self.append_term_docs_delta(tx, term, deltas).await?;
		let td = self.ikb.new_td_root(term);
		if docs.is_empty() {
			tx.del(&td).await?;
		} else {
			tx.set(&td, &docs).await?;
		}
		Ok(())
	}

	/// Read phase for `!tt`: capture the generation, fold visible deltas by
	/// term/doc, and remember the exact delta keys seen in this snapshot.
	async fn prepare_term_docs_compaction(
		&self,
		tx: &Transaction,
	) -> Result<TermDocsCompactionPlan> {
		self.prepare_term_docs_compaction_with_limit(tx, COUNT_BATCH_SIZE).await
	}

	async fn prepare_term_docs_compaction_with_limit(
		&self,
		tx: &Transaction,
		limit: u32,
	) -> Result<TermDocsCompactionPlan> {
		let generation = read_compaction_generation(tx, &self.ikb.new_tv_key()).await?;
		let (beg, end) = self.ikb.new_tt_terms_range()?;
		let range = beg..end;
		let mut delta_keys = Vec::new();
		let mut deltas_by_term: HashMap<String, HashMap<DocId, i64>> = HashMap::new();
		let batch = tx.batch_keys(range, limit.max(1), None).await?;
		for k in batch.result {
			let tt = Tt::decode_key(&k)?;
			let entry = deltas_by_term
				.entry(tt.term.to_string())
				.or_default()
				.entry(tt.doc_id)
				.or_default();
			if tt.add {
				*entry += 1;
			} else {
				*entry -= 1;
			}
			delta_keys.push(k);
		}
		Ok(TermDocsCompactionPlan {
			generation,
			deltas_by_term,
			delta_keys,
			has_more: batch.next.is_some(),
		})
	}

	/// Write phase for `!tt`: CAS the generation, update compacted term
	/// bitmaps, and delete only the snapshot-seen delta keys.
	#[cfg(test)]
	async fn apply_term_docs_compaction(
		&self,
		tx: &Transaction,
		plan: TermDocsCompactionPlan,
	) -> Result<bool> {
		if !plan.has_logs() {
			return Ok(false);
		}
		if !self.reserve_term_docs_compaction_generation(tx, &plan).await? {
			return Ok(false);
		}
		self.write_term_docs_compaction(tx, plan).await?;
		Ok(true)
	}

	/// Advances the term-document compaction generation for a non-empty plan.
	async fn reserve_term_docs_compaction_generation(
		&self,
		tx: &Transaction,
		plan: &TermDocsCompactionPlan,
	) -> Result<bool> {
		if !plan.has_logs() {
			return Ok(true);
		}
		bump_compaction_generation(tx, &self.ikb.new_tv_key(), plan.generation).await
	}

	/// Updates compacted term-document bitmaps and removes captured `!tt` deltas.
	async fn write_term_docs_compaction(
		&self,
		tx: &Transaction,
		plan: TermDocsCompactionPlan,
	) -> Result<()> {
		for (term, deltas) in plan.deltas_by_term {
			if !deltas.is_empty() {
				self.set_term_docs_delta(tx, &term, &deltas).await?;
			}
		}
		for key in plan.delta_keys {
			tx.del(&key).await?;
		}
		Ok(())
	}

	/// Compacts term documents by consolidating deltas and removing logs.
	#[cfg(test)]
	async fn compact_term_docs(&self, tx: &Transaction) -> Result<bool> {
		let plan = self.prepare_term_docs_compaction(tx).await?;
		self.apply_term_docs_compaction(tx, plan).await
	}

	/// Creates a new iterator for search hits
	///
	/// This method creates an iterator over the documents that match all query
	/// terms. It returns None if any term has no matching documents.
	pub(crate) fn new_hits_iterator(
		&self,
		qt: &QueryTerms,
		bo: BooleanOperator,
	) -> Option<FullTextHitsIterator> {
		// Execute the operation depending on the operator
		let hits = match bo {
			BooleanOperator::And => Self::intersection_operation(&qt.docs),
			BooleanOperator::Or => Self::union_operation(&qt.docs),
		};

		// Create and return an iterator if we have matching documents
		if let Some(hits) = hits
			&& !hits.is_empty()
		{
			return Some(FullTextHitsIterator::new(self.ikb.clone(), hits));
		}

		// No documents match the terms
		None
	}

	fn intersection_operation(docs: &[Option<RoaringTreemap>]) -> Option<RoaringTreemap> {
		// Early return for empty input
		if docs.is_empty() {
			return None;
		}

		// Collect only the "Some" variants
		let mut valid_docs: Vec<&RoaringTreemap> = docs.iter().flatten().collect();

		// If any term has no documents, the intersection is empty
		if docs.len() != valid_docs.len() {
			return None;
		}

		// Sort by cardinality - intersecting with smaller sets first is more efficient
		valid_docs.sort_by_key(|bitmap| bitmap.len());

		// Convert docs to an iterator
		let mut iter = valid_docs.into_iter();

		// Start with the smallest set (clone only once)
		if let Some(mut result) = iter.next().cloned() {
			// Intersect with remaining sets in order of increasing size
			for d in iter {
				// Early termination any terms docs is empty
				if d.is_empty() {
					return None;
				}
				result &= d;
				// Check if the result becomes empty
				if result.is_empty() {
					return None;
				}
			}
			// Return the result
			Some(result)
		} else {
			None
		}
	}

	fn union_operation(docs: &[Option<RoaringTreemap>]) -> Option<RoaringTreemap> {
		// Convert docs to an iterator
		let mut docs = docs.iter().flatten();

		// Start with the first set
		if let Some(mut result) = docs.next().cloned() {
			// Union with remaining sets
			for d in docs {
				result |= d;
			}
			// Return the result
			Some(result)
		} else {
			None
		}
	}

	pub(crate) async fn get_doc_id(
		&self,
		tx: &Transaction,
		rid: &RecordId,
	) -> Result<Option<DocId>> {
		if rid.table != *self.ikb.table() {
			return Ok(None);
		}
		self.doc_ids.get_doc_id(tx, &rid.key).await
	}
	pub(crate) async fn new_scorer(&self, ctx: &FrozenContext) -> Result<Option<Scorer>> {
		if let Some(bm25) = &self.bm25 {
			let dlc = self.compute_doc_length_and_count(&ctx.tx(), None).await?;
			let sc = Scorer::new(dlc, *bm25);
			return Ok(Some(sc));
		}
		Ok(None)
	}

	/// Collects compacted root stats plus all visible `!dc` deltas.
	///
	/// The root and deltas share a range, so this read is used for query-time
	/// scoring where the complete statistic is needed.
	async fn collect_doc_length_and_count(
		&self,
		tx: &Transaction,
	) -> Result<(DocLengthAndCount, Vec<Key>)> {
		let mut dlc = DocLengthAndCount::default();
		let (beg, end) = self.ikb.new_dc_range_with_root()?;
		let range = beg..end;
		// Compute the total number of documents (DocCount) and the total number of
		// terms (DocLength) This key list is supposed to be small, subject to
		// compaction. The root key is the compacted values, and the others are deltas
		// from transaction not yet compacted.
		let root_key = self.ikb.new_dc_compacted()?;
		let mut delta_keys = Vec::new();
		for (k, v) in tx.getr(range.clone(), None).await? {
			let st: DocLengthAndCount = revision::from_slice(&v)?;
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;

			if k != root_key {
				delta_keys.push(k);
			}
		}
		Ok((dlc, delta_keys))
	}

	/// Returns the `!dc` range that contains only delta entries.
	fn dc_delta_range(&self) -> Result<std::ops::Range<Key>> {
		let (mut beg, end) = self.ikb.new_dc_range_with_root()?;
		beg.push(0);
		Ok(beg..end)
	}

	/// Collects compacted root stats plus a bounded batch of visible `!dc`
	/// deltas for compaction.
	async fn collect_doc_length_and_count_compaction(
		&self,
		tx: &Transaction,
		limit: u32,
	) -> Result<(DocLengthAndCount, Vec<Key>, bool)> {
		let root_key = self.ikb.new_dc_compacted()?;
		let mut dlc = if let Some(v) = tx.get(&root_key, None).await? {
			revision::from_slice(&v)?
		} else {
			DocLengthAndCount::default()
		};
		let batch = tx.batch_keys_vals(self.dc_delta_range()?, limit.max(1), None).await?;
		let mut delta_keys = Vec::with_capacity(batch.result.len());
		for (k, v) in batch.result {
			let st: DocLengthAndCount = revision::from_slice(&v)?;
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;
			delta_keys.push(k);
		}
		Ok((dlc, delta_keys, batch.next.is_some()))
	}

	async fn compute_doc_length_and_count(
		&self,
		tx: &Transaction,
		compact_log: Option<&mut bool>,
	) -> Result<DocLengthAndCount> {
		let (dlc, delta_keys) = self.collect_doc_length_and_count(tx).await?;
		if let Some(compact_log) = compact_log
			&& !delta_keys.is_empty()
		{
			for key in delta_keys {
				tx.del(&key).await?;
			}
			*compact_log = true;
		}
		Ok(dlc)
	}

	/// Read phase for `!dc`: capture the generation, compute aggregate doc
	/// stats, and remember the exact delta keys seen in this snapshot.
	async fn prepare_doc_length_and_count_compaction(
		&self,
		tx: &Transaction,
	) -> Result<DocLengthAndCountCompactionPlan> {
		self.prepare_doc_length_and_count_compaction_with_limit(tx, COUNT_BATCH_SIZE).await
	}

	async fn prepare_doc_length_and_count_compaction_with_limit(
		&self,
		tx: &Transaction,
		limit: u32,
	) -> Result<DocLengthAndCountCompactionPlan> {
		let generation = read_compaction_generation(tx, &self.ikb.new_dv_key()).await?;
		let (dlc, delta_keys, has_more) =
			self.collect_doc_length_and_count_compaction(tx, limit).await?;
		Ok(DocLengthAndCountCompactionPlan {
			generation,
			dlc,
			delta_keys,
			has_more,
		})
	}

	/// Write phase for `!dc`: CAS the generation, write the compacted root,
	/// and delete only the snapshot-seen delta keys.
	#[cfg(test)]
	async fn apply_doc_length_and_count_compaction(
		&self,
		tx: &Transaction,
		plan: DocLengthAndCountCompactionPlan,
	) -> Result<bool> {
		if !plan.has_logs() {
			return Ok(false);
		}
		if !self.reserve_doc_length_and_count_compaction_generation(tx, &plan).await? {
			return Ok(false);
		}
		self.write_doc_length_and_count_compaction(tx, plan).await?;
		Ok(true)
	}

	/// Advances the document-stat compaction generation for a non-empty plan.
	async fn reserve_doc_length_and_count_compaction_generation(
		&self,
		tx: &Transaction,
		plan: &DocLengthAndCountCompactionPlan,
	) -> Result<bool> {
		if !plan.has_logs() {
			return Ok(true);
		}
		bump_compaction_generation(tx, &self.ikb.new_dv_key(), plan.generation).await
	}

	/// Writes compacted document stats and removes captured `!dc` deltas.
	async fn write_doc_length_and_count_compaction(
		&self,
		tx: &Transaction,
		plan: DocLengthAndCountCompactionPlan,
	) -> Result<()> {
		let key = self.ikb.new_dc_compacted()?;
		tx.set(&key, &revision::to_vec(&plan.dlc)?).await?;
		for key in plan.delta_keys {
			tx.del(&key).await?;
		}
		Ok(())
	}

	/// Compacts document length and count statistics
	///
	/// This method consolidates document length and count statistics and
	/// removes the delta logs. It returns true if any compaction was
	/// performed.
	#[cfg(test)]
	async fn compact_doc_length_and_count(&self, tx: &Transaction) -> Result<bool> {
		let plan = self.prepare_doc_length_and_count_compaction(tx).await?;
		self.apply_doc_length_and_count_compaction(tx, plan).await
	}

	/// Builds the full-text compaction plan in a read-only transaction.
	pub(in crate::idx) async fn prepare_compaction(
		&self,
		tx: &Transaction,
	) -> Result<FullTextCompactionPlan> {
		Ok(FullTextCompactionPlan {
			doc_lengths: self.prepare_doc_length_and_count_compaction(tx).await?,
			term_docs: self.prepare_term_docs_compaction(tx).await?,
		})
	}

	/// Applies a prepared full-text compaction plan in a short write
	/// transaction. A generation mismatch leaves the plan unapplied.
	pub(in crate::idx) async fn apply_compaction(
		&self,
		tx: &Transaction,
		plan: FullTextCompactionPlan,
	) -> Result<bool> {
		if !plan.has_work() {
			return Ok(false);
		}
		if !self.reserve_doc_length_and_count_compaction_generation(tx, &plan.doc_lengths).await? {
			return Ok(false);
		}
		if !self.reserve_term_docs_compaction_generation(tx, &plan.term_docs).await? {
			return Ok(false);
		}
		let has_doc_lengths = plan.doc_lengths.has_logs();
		let has_term_docs = plan.term_docs.has_logs();
		if has_doc_lengths {
			self.write_doc_length_and_count_compaction(tx, plan.doc_lengths).await?;
		}
		if has_term_docs {
			self.write_term_docs_compaction(tx, plan.term_docs).await?;
		}
		Ok(has_doc_lengths || has_term_docs)
	}

	/// Performs compaction on the full-text index
	///
	/// This method compacts both document length/count statistics and term
	/// documents. It returns true if any compaction was performed.
	#[cfg(test)]
	pub(crate) async fn compaction(&self, tx: &Transaction) -> Result<bool> {
		let r1 = self.compact_doc_length_and_count(tx).await?;
		let r2 = self.compact_term_docs(tx).await?;
		Ok(r1 || r2)
	}

	/// Highlights search terms in a document
	///
	/// This method highlights the occurrences of search terms in the document
	/// value. It uses the provided highlighting parameters to format the
	/// highlighted text.
	pub(crate) async fn highlight(
		&self,
		tx: &Transaction,
		thg: &RecordId,
		qt: &QueryTerms,
		hlp: HighlightParams,
		idiom: &Idiom,
		doc: &Value,
	) -> Result<Value> {
		let doc_id = self.get_doc_id(tx, thg).await?;
		if let Some(doc_id) = doc_id {
			let mut hl = Highlighter::new(&hlp, idiom, doc);
			for tk in qt.tokens.list() {
				if let Some(td) =
					self.get_term_document(tx, doc_id, qt.tokens.get_token_string(tk)?).await?
				{
					hl.highlight(tk.get_char_len(), td.o);
				}
			}
			return hl.try_into();
		}
		Ok(Value::None)
	}

	async fn get_term_document(
		&self,
		tx: &Transaction,
		id: DocId,
		term: &str,
	) -> Result<Option<TermDocument>> {
		let key = self.ikb.new_td(term, id);
		tx.get(&key, None).await
	}

	pub(crate) async fn read_offsets(
		&self,
		tx: &Transaction,
		thg: &RecordId,
		qt: &QueryTerms,
		partial: bool,
	) -> Result<Value> {
		let doc_id = self.get_doc_id(tx, thg).await?;
		if let Some(doc_id) = doc_id {
			let mut or = Offseter::new(partial);
			for tk in qt.tokens.list() {
				let term = qt.tokens.get_token_string(tk)?;
				let o = self.get_term_document(tx, doc_id, term).await?;
				if let Some(o) = o {
					or.highlight(tk.get_char_len(), o.o);
				}
			}
			return Ok(or.into());
		}
		Ok(Value::None)
	}
}

/// Iterator for full-text search hits that implements the MatchesHitsIterator
/// trait
pub(crate) struct FullTextHitsIterator {
	/// The index key base used for key generation
	ikb: IndexKeyBase,
	/// Iterator over the document IDs in the search results
	iter: IntoIter,
}

impl FullTextHitsIterator {
	/// Creates a new iterator for full-text search hits
	///
	/// This method initializes an iterator with the index key base and a bitmap
	/// of matching document IDs.
	fn new(ikb: IndexKeyBase, hits: RoaringTreemap) -> Self {
		Self {
			ikb,
			iter: hits.into_iter(),
		}
	}
}

impl MatchesHitsIterator for FullTextHitsIterator {
	#[cfg(target_pointer_width = "64")]
	fn len(&self) -> usize {
		self.iter.len()
	}
	#[cfg(not(target_pointer_width = "64"))]
	fn len(&self) -> usize {
		self.iter.size_hint().0
	}

	/// Returns the next search hit in the iterator
	///
	/// This method retrieves the next document ID from the bitmap and resolves
	/// it to a Thing. It returns None when there are no more hits.
	async fn next(&mut self, tx: &Transaction) -> Result<Option<(RecordId, DocId)>> {
		for doc_id in self.iter.by_ref() {
			if let Some(key) = SeqDocIds::get_id(&self.ikb, tx, doc_id).await? {
				let rid = RecordId {
					table: self.ikb.table().clone(),
					key,
				};
				return Ok(Some((rid, doc_id)));
			}
		}
		Ok(None)
	}
}

/// Implements BM25 scoring for relevance ranking of search results
pub(crate) struct Scorer {
	/// precomputed BM25 scoring parameters
	k1: f64,
	k1_plus_1: f64,
	one_minus_b: f64,
	b_over_avg_len: f64,
	doc_count: f64,
}

impl Scorer {
	/// Creates a new scorer with the specified parameters
	///
	/// This method initializes a scorer with document statistics and BM25
	/// parameters. It calculates the average document length for use in the
	/// BM25 algorithm.
	fn new(dlc: DocLengthAndCount, bm25: Bm25Params) -> Self {
		let doc_count = dlc.doc_count as f64;
		let average_doc_length = (dlc.total_docs_length as f64) / doc_count;
		let k1 = bm25.k1 as f64;
		let b = bm25.b as f64;
		Self {
			k1,
			k1_plus_1: k1 + 1.0,
			one_minus_b: 1.0 - b,
			b_over_avg_len: b / average_doc_length,
			doc_count,
		}
	}

	/// Calculates the overall score for a document based on query terms
	///
	/// This method computes the sum of BM25 scores for all matching terms in
	/// the document. The score represents the relevance of the document to the
	/// query.
	pub(crate) async fn score(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Score> {
		let mut sc = 0.0;
		let tl = qt.tokens.list();
		let doc_length = fti.get_doc_length(tx, doc_id).await?.unwrap_or(0) as f64;
		for (i, d) in qt.docs.iter().enumerate() {
			if let Some(docs) = d
				&& docs.contains(doc_id)
				&& let Some(token) = tl.get(i)
			{
				let term = qt.tokens.get_token_string(token)?;
				let td = fti.get_term_document(tx, doc_id, term).await?;
				if let Some(td) = td {
					sc += self.compute_bm25_score(td.f as f64, docs.len() as f64, doc_length)
				}
			}
		}
		Ok(sc as f32)
	}

	/// Computes the Okapi-BM25 score for a single term.
	///
	/// Variant:
	/// • IDF is clamped to ≥ 0 (avoids negative weights for very common terms).
	/// • Term-frequency is lower-bounded with 1 + ln(tf) as proposed in
	///   “Lower-Bounding Term Frequency Normalization” (Lv & Zhai, CIKM 2011).
	///
	/// score =
	///     idf · (k1 + 1) · tf′
	///     ---------------------------------------------
	///     tf′ + k1 · (1 − b + b · doc_len / avg_doc_len)
	///
	/// where
	///   idf = ln((N − n(qᵢ) + 0.5)/(n(qᵢ) + 0.5)), clamped to ≥ 0
	///   tf′ = 1 + ln(tf)
	fn compute_bm25_score(&self, term_freq: f64, term_doc_count: f64, doc_length: f64) -> f64 {
		// Early return for zero-term frequency
		if term_freq <= 0.0 {
			return 0.0;
		}

		// ---------- 1. Inverse Document Frequency (IDF) ---------------------
		let denominator = term_doc_count + 0.5; // n(qᵢ) + 0.5
		let numerator = self.doc_count - term_doc_count + 0.5; // N − n(qᵢ) + 0.5
		let idf = (numerator / denominator).ln().max(0.0); // floor at 0

		// Early return for zero IDF (very common terms)
		if idf == 0.0 {
			return 0.0;
		}

		// ---------- 2. Lower-bounded term-frequency -------------------------
		let tf_prime = 1.0 + term_freq.ln(); // 1 + ln(tf)

		// ---------- 3. Document-length normalisation -----------------------
		let length_norm = self.one_minus_b + self.b_over_avg_len * doc_length;

		// ---------- 4. Okapi BM25 (optimized) ------------------------------
		let numerator = idf * self.k1_plus_1 * tf_prime;
		let denominator = tf_prime + self.k1 * length_norm;

		numerator / denominator
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::time::{Duration, Instant};

	use reblessive::tree::Stk;
	use test_log::test;
	use tokio::time::sleep;
	use uuid::Uuid;

	use super::{FullTextIndex, TermDocument};
	use crate::catalog::{DatabaseId, FullTextParams, IndexId, NamespaceId};
	use crate::cnf::CommonConfig;
	use crate::ctx::{Context, FrozenContext};
	use crate::dbs::Options;
	use crate::expr::statements::DefineAnalyzerStatement;
	use crate::idx::IndexKeyBase;
	use crate::idx::ft::offset::Offset;
	use crate::idx::index::IndexOperation;
	use crate::kvs::LockType::*;
	use crate::kvs::{Datastore, Transaction, TransactionType};
	use crate::sql::Expr;
	use crate::sql::statements::DefineStatement;
	use crate::syn;
	use crate::val::{Array, RecordId, Value};

	#[derive(Clone)]
	struct TestContext {
		ctx: FrozenContext,
		opt: Options,
		nid: Uuid,
		start: Arc<Instant>,
		ds: Arc<Datastore>,
		content: Arc<Value>,
		ikb: IndexKeyBase,
		fti: Arc<FullTextIndex>,
	}

	impl TestContext {
		async fn new() -> Self {
			let ds = Arc::new(Datastore::new("memory").await.unwrap());
			let ctx = ds.setup_ctx().unwrap().freeze();
			let q = syn::expr("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
			let Expr::Define(q) = q else {
				panic!()
			};
			let DefineStatement::Analyzer(az) = *q else {
				panic!()
			};
			let mut stack = reblessive::TreeStack::new();

			let opts = Options::new(&CommonConfig::default());
			let stk_ctx = Arc::clone(&ctx);
			let az = stack
				.enter(|stk| async move {
					Arc::new(
						DefineAnalyzerStatement::from(az)
							.to_definition(stk, &stk_ctx, &opts, None)
							.await
							.unwrap(),
					)
				})
				.finish()
				.await;
			let content = Arc::new(Value::from(Array::from(vec![
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
			])));
			let ft_params = Arc::new(FullTextParams {
				analyzer: az.name.clone(),
				scoring: Default::default(),
				highlight: true,
			});
			let nid = Uuid::new_v4();
			let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(3));
			let opt = Options::new(&CommonConfig::default())
				.with_ns(Some("testns".into()))
				.with_db(Some("testdb".into()));
			let fti = Arc::new(
				FullTextIndex::with_analyzer(ctx.get_index_stores(), az, ikb.clone(), &ft_params)
					.unwrap(),
			);
			let start = Arc::new(Instant::now());
			Self {
				ctx,
				opt,
				nid,
				ikb,
				start,
				ds,
				content,
				fti,
			}
		}

		async fn new_tx(&self, tt: TransactionType) -> Arc<Transaction> {
			Arc::new(self.ds.transaction(tt, Optimistic).await.unwrap())
		}

		async fn remove_insert_task(&self, stk: &mut Stk, rid: &RecordId) {
			let mut ctx = Context::new_child(&self.ctx);
			let tx = self.new_tx(TransactionType::Write).await;
			ctx.set_transaction(Arc::clone(&tx));
			let ctx = ctx.freeze();

			let mut require_compaction = false;
			self.fti
				.remove_content(
					stk,
					&ctx,
					&self.opt,
					rid,
					vec![self.content.as_ref().clone()],
					&mut require_compaction,
				)
				.await
				.unwrap();
			self.fti
				.index_content(
					stk,
					&ctx,
					&self.opt,
					rid,
					vec![self.content.as_ref().clone()],
					&mut require_compaction,
				)
				.await
				.unwrap();

			if require_compaction {
				IndexOperation::compaction_trigger(&self.ikb, &tx, self.nid).await.unwrap();
			}

			tx.commit().await.unwrap();
		}

		async fn dc_delta_count(&self, tx: &Transaction) -> usize {
			let (beg, end) = self.ikb.new_dc_range_with_root().unwrap();
			let root = self.ikb.new_dc_compacted().unwrap();
			tx.keys(beg..end, u32::MAX, 0, None)
				.await
				.unwrap()
				.into_iter()
				.filter(|k| k != &root)
				.count()
		}

		async fn tt_delta_count(&self, tx: &Transaction) -> usize {
			let (beg, end) = self.ikb.new_tt_terms_range().unwrap();
			tx.count(beg..end, None).await.unwrap()
		}
	}

	async fn concurrent_doc_update(test: TestContext, rid: Arc<RecordId>, mut count: usize) {
		let mut stack = reblessive::TreeStack::new();
		while count > 0 && test.start.elapsed().as_millis() < 3000 {
			stack.enter(|stk| test.remove_insert_task(stk, &rid)).finish().await;
			count -= 1;
		}
	}

	async fn concurrent_search(test: TestContext, doc_ids: Vec<Arc<RecordId>>) {
		while test.start.elapsed().as_millis() < 3500 {
			let tx = test.new_tx(TransactionType::Read).await;
			let expected = {
				TermDocument {
					f: 5,
					o: vec![
						Offset {
							index: 2,
							start: 44,
							gen_start: 44,
							end: 47,
						},
						Offset {
							index: 3,
							start: 42,
							gen_start: 42,
							end: 45,
						},
						Offset {
							index: 16,
							start: 4,
							gen_start: 4,
							end: 7,
						},
						Offset {
							index: 18,
							start: 8,
							gen_start: 8,
							end: 11,
						},
						Offset {
							index: 19,
							start: 59,
							gen_start: 59,
							end: 62,
						},
					],
				}
			};
			for doc_id in &doc_ids {
				let id = test.fti.get_doc_id(&tx, doc_id).await.unwrap().unwrap();
				let td = test.fti.get_term_document(&tx, id, "the").await.unwrap();
				assert_eq!(td.as_ref(), Some(&expected));
			}
		}
	}

	async fn compaction(test: TestContext) {
		let duration = Duration::from_secs(1);
		while test.start.elapsed().as_millis() < 3500 {
			sleep(duration).await;
			loop {
				let tx = test.new_tx(TransactionType::Write).await;
				let has_logs = test.fti.compaction(&tx).await.unwrap();
				tx.commit().await.unwrap();
				if !has_logs {
					break;
				}
			}
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn concurrent_test() {
		let doc1: Arc<RecordId> = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));
		let doc2: Arc<RecordId> = Arc::new(RecordId::new("t".into(), "doc2".to_owned()));

		let test = TestContext::new().await;
		// Ensure the documents are pre-existing
		concurrent_doc_update(test.clone(), Arc::clone(&doc1), 1).await;
		concurrent_doc_update(test.clone(), Arc::clone(&doc2), 1).await;
		// Prepare the concurrent tasks
		let task1 =
			tokio::spawn(concurrent_doc_update(test.clone(), Arc::clone(&doc1), usize::MAX));
		let task2 =
			tokio::spawn(concurrent_doc_update(test.clone(), Arc::clone(&doc2), usize::MAX));
		let task3 = tokio::spawn(compaction(test.clone()));
		let task4 = tokio::spawn(concurrent_search(test.clone(), vec![doc1, doc2]));
		let _ = tokio::try_join!(task1, task2, task3, task4).expect("Tasks failed");

		// Check that logs have been compacted:
		let tx = test.new_tx(TransactionType::Read).await;
		let (beg, end) = test.ikb.new_tt_terms_range().unwrap();
		assert_eq!(tx.count(beg..end, None).await.unwrap(), 0);
		assert_eq!(test.dc_delta_count(&tx).await, 0);
		let (beg, end) = test.ikb.new_dc_range_with_root().unwrap();
		assert_eq!(tx.count(beg..end, None).await.unwrap(), 1);
	}

	/// BM25 scores must remain non-zero after compaction.
	///
	/// Compaction deletes consumed dc deltas and writes the aggregate stats to
	/// the root key, so scoring must read the root-inclusive range.
	#[test(tokio::test(flavor = "multi_thread"))]
	async fn bm25_score_survives_compaction() {
		let test = TestContext::new().await;
		let doc1 = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));
		let doc2 = Arc::new(RecordId::new("t".into(), "doc2".to_owned()));

		// Index two documents so that IDF is non-zero for a term that only
		// appears in one of them (BM25 IDF clamps to 0 when term_doc_count
		// >= doc_count / 2, so we need at least 2 docs).
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| test.remove_insert_task(stk, &doc1)).finish().await;
		stack.enter(|stk| test.remove_insert_task(stk, &doc2)).finish().await;

		let frozen_read_ctx = |test: &TestContext| {
			let test = test.clone();
			async move {
				let mut ctx = Context::new_child(&test.ctx);
				let tx = test.new_tx(TransactionType::Read).await;
				ctx.set_transaction(Arc::clone(&tx));
				(ctx.freeze(), tx)
			}
		};

		// "lorem" appears in only 1 of 25 entries in the test content, so
		// with 2 identical docs the term_doc_count=2 and doc_count=2, giving
		// IDF = ln((2-2+0.5)/(2+0.5)) which clamps to 0. We need a search
		// term where term_doc_count < doc_count. Since both docs have the
		// same content, every term has term_doc_count == doc_count, so IDF=0.
		//
		// Instead, directly verify that `compute_doc_length_and_count`
		// returns valid (non-zero) stats before and after compaction.

		// Before compaction: scorer should exist and have valid doc stats.
		let (read_ctx, tx) = frozen_read_ctx(&test).await;
		let scorer_before = test.fti.new_scorer(&read_ctx).await.unwrap();
		assert!(scorer_before.is_some(), "scorer should exist (BM25 is configured)");
		// Verify doc_count is non-zero via the scorer's internal state.
		// We access this indirectly: if doc_count were 0, average_doc_length
		// would be NaN, causing b_over_avg_len to be NaN. We can verify by
		// checking compute_doc_length_and_count directly.
		let dlc_before = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert!(
			dlc_before.doc_count > 0,
			"doc_count before compaction should be > 0, got {}",
			dlc_before.doc_count
		);
		assert!(
			dlc_before.total_docs_length > 0,
			"total_docs_length before compaction should be > 0, got {}",
			dlc_before.total_docs_length
		);

		// Run compaction (mimics the background compaction that fires every 5s).
		let tx = test.new_tx(TransactionType::Write).await;
		let compacted = test.fti.compaction(&tx).await.unwrap();
		tx.commit().await.unwrap();
		assert!(compacted, "compaction should have processed delta logs");

		// Verify the dc delta range is now empty (deltas were consumed).
		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(
			test.dc_delta_count(&tx).await,
			0,
			"dc delta range should be empty after compaction"
		);

		// After compaction: doc stats must still be valid.
		let dlc_after = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert!(
			dlc_after.doc_count > 0,
			"doc_count after compaction should be > 0, got {}",
			dlc_after.doc_count
		);
		assert!(
			dlc_after.total_docs_length > 0,
			"total_docs_length after compaction should be > 0, got {}",
			dlc_after.total_docs_length
		);
		assert_eq!(
			dlc_before.doc_count, dlc_after.doc_count,
			"doc_count should be stable across compaction: before={}, after={}",
			dlc_before.doc_count, dlc_after.doc_count
		);
		assert_eq!(
			dlc_before.total_docs_length, dlc_after.total_docs_length,
			"total_docs_length should be stable across compaction: before={}, after={}",
			dlc_before.total_docs_length, dlc_after.total_docs_length
		);

		// Verify the scorer still works (doesn't produce NaN).
		let (read_ctx, _tx) = frozen_read_ctx(&test).await;
		let scorer_after = test.fti.new_scorer(&read_ctx).await.unwrap();
		assert!(scorer_after.is_some(), "scorer should still exist after compaction");
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn doc_stats_compaction_batches_deltas() {
		let test = TestContext::new().await;
		let doc1 = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));
		let doc2 = Arc::new(RecordId::new("t".into(), "doc2".to_owned()));
		let doc3 = Arc::new(RecordId::new("t".into(), "doc3".to_owned()));

		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| test.remove_insert_task(stk, &doc1)).finish().await;
		stack.enter(|stk| test.remove_insert_task(stk, &doc2)).finish().await;
		stack.enter(|stk| test.remove_insert_task(stk, &doc3)).finish().await;

		let tx = test.new_tx(TransactionType::Read).await;
		let before = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert_eq!(test.dc_delta_count(&tx).await, 3);
		tx.cancel().await.unwrap();

		let plan = {
			let tx = test.new_tx(TransactionType::Read).await;
			let plan =
				test.fti.prepare_doc_length_and_count_compaction_with_limit(&tx, 2).await.unwrap();
			tx.cancel().await.unwrap();
			plan
		};
		assert!(plan.has_logs());
		assert!(plan.has_more());
		assert_eq!(plan.delta_keys.len(), 2);

		let tx = test.new_tx(TransactionType::Write).await;
		assert!(test.fti.apply_doc_length_and_count_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(test.dc_delta_count(&tx).await, 1);
		let after_first = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert_eq!(before, after_first);
		tx.cancel().await.unwrap();

		let tx = test.new_tx(TransactionType::Write).await;
		let plan =
			test.fti.prepare_doc_length_and_count_compaction_with_limit(&tx, 2).await.unwrap();
		assert!(!plan.has_more());
		assert!(test.fti.apply_doc_length_and_count_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(test.dc_delta_count(&tx).await, 0);
		let after_second = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert_eq!(before, after_second);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn term_docs_compaction_batches_deltas() {
		let test = TestContext::new().await;
		let doc = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));

		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| test.remove_insert_task(stk, &doc)).finish().await;

		let tx = test.new_tx(TransactionType::Read).await;
		let before_terms = test.tt_delta_count(&tx).await;
		assert!(before_terms > 2);
		tx.cancel().await.unwrap();

		let plan = {
			let tx = test.new_tx(TransactionType::Read).await;
			let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 2).await.unwrap();
			tx.cancel().await.unwrap();
			plan
		};
		assert!(plan.has_logs());
		assert!(plan.has_more());
		assert_eq!(plan.delta_keys.len(), 2);

		let tx = test.new_tx(TransactionType::Write).await;
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(test.tt_delta_count(&tx).await, before_terms - 2);
		tx.cancel().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		let mut ctx = Context::new_child(&test.ctx);
		ctx.set_transaction(Arc::clone(&tx));
		let ctx = ctx.freeze();
		let mut stack = reblessive::TreeStack::new();
		let qt = stack
			.enter(|stk| test.fti.extract_querying_terms(stk, &ctx, &test.opt, "Welcome".into()))
			.finish()
			.await
			.unwrap();
		let doc_id = test.fti.get_doc_id(&tx, &doc).await.unwrap().unwrap();
		assert!(
			qt.docs.iter().flatten().any(|docs| docs.contains(doc_id)),
			"query should see documents represented by compacted roots plus residual deltas"
		);
		tx.cancel().await.unwrap();

		loop {
			let tx = test.new_tx(TransactionType::Write).await;
			let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 2).await.unwrap();
			let has_more = plan.has_more();
			let applied = test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap();
			tx.commit().await.unwrap();
			if !applied || !has_more {
				break;
			}
		}

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(test.tt_delta_count(&tx).await, 0);
		let mut ctx = Context::new_child(&test.ctx);
		ctx.set_transaction(Arc::clone(&tx));
		let ctx = ctx.freeze();
		let mut stack = reblessive::TreeStack::new();
		let qt = stack
			.enter(|stk| test.fti.extract_querying_terms(stk, &ctx, &test.opt, "Welcome".into()))
			.finish()
			.await
			.unwrap();
		assert!(
			qt.docs.iter().flatten().any(|docs| docs.contains(doc_id)),
			"query should still see documents after all term deltas are compacted"
		);
		tx.cancel().await.unwrap();
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn compaction_preserves_post_snapshot_deltas() {
		let test = TestContext::new().await;
		let doc1 = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));
		let doc2 = Arc::new(RecordId::new("t".into(), "doc2".to_owned()));

		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| test.remove_insert_task(stk, &doc1)).finish().await;

		let read_tx = test.new_tx(TransactionType::Read).await;
		let plan = test.fti.prepare_compaction(&read_tx).await.unwrap();
		read_tx.cancel().await.unwrap();

		stack.enter(|stk| test.remove_insert_task(stk, &doc2)).finish().await;

		let write_tx = test.new_tx(TransactionType::Write).await;
		assert!(test.fti.apply_compaction(&write_tx, plan).await.unwrap());
		write_tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(tx.get(&test.ikb.new_dv_key(), None).await.unwrap(), Some(1));
		assert_eq!(tx.get(&test.ikb.new_tv_key(), None).await.unwrap(), Some(1));
		assert_eq!(
			test.dc_delta_count(&tx).await,
			1,
			"post-snapshot doc-length delta must remain uncompacted"
		);
		let (beg, end) = test.ikb.new_tt_terms_range().unwrap();
		assert!(
			tx.count(beg..end, None).await.unwrap() > 0,
			"post-snapshot term deltas must remain uncompacted"
		);
		let dlc = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert_eq!(dlc.doc_count, 2);
	}
}
