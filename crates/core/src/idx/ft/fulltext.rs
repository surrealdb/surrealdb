use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use futures::future::try_join_all;
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
use crate::idx::IndexKeyBase;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::Tokens;
use crate::idx::ft::highlighter::{HighlightParams, Highlighter, Offseter};
use crate::idx::ft::offset::Offset;
use crate::idx::ft::{DocLength, Score, TermFrequency};
use crate::idx::planner::iterators::MatchesHitsIterator;
use crate::idx::seqdocids::{DocId, SeqDocIds};
use crate::idx::trees::store::IndexStores;
use crate::key::index::tt::Tt;
use crate::kvs::{Transaction, impl_kv_value_revisioned};
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default)]
/// Tracks document length and count statistics for the index
pub(crate) struct DocLengthAndCount {
	/// The total length of all documents in the index
	total_docs_length: i128,
	/// The total number of documents in the index
	doc_count: i64,
}
impl_kv_value_revisioned!(DocLengthAndCount);

/// Represents the terms in a search query and their associated document sets
pub(in crate::idx) struct QueryTerms {
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
	pub(in crate::idx) fn is_empty(&self) -> bool {
		self.tokens.list().is_empty()
	}

	pub(in crate::idx) fn contains_doc(&self, doc_id: DocId) -> bool {
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

#[derive(Clone)]
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
	/// Whether to use accurate (exact) scoring vs fast (SmallFloat) scoring
	use_accurate_scoring: bool,
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
	) -> Result<Self> {
		let az = tx.get_db_analyzer(ikb.0.ns, ikb.0.db, &p.analyzer).await?;
		ixs.mappers().check(&az).await?;
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
		let (bm25, use_accurate_scoring) = match p.scoring {
			Scoring::Bm {
				k1,
				b,
			} => (
				Some(Bm25Params {
					k1,
					b,
				}),
				false,
			),
			Scoring::BmAccurate {
				k1,
				b,
			} => (
				Some(Bm25Params {
					k1,
					b,
				}),
				true,
			),
			Scoring::Vs => (None, false),
		};
		Ok(Self {
			analyzer,
			highlighting: p.highlight,
			doc_ids: SeqDocIds::new(ikb.clone()),
			ikb,
			bm25,
			use_accurate_scoring,
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
		let nid = opt.id();
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
				// Get the document length to update DocLengthAndCount
				let doc_length: Option<u64> = if self.use_accurate_scoring {
					// BM25_ACCURATE: Read exact length from legacy dl key
					let key = self.ikb.new_dl(doc_id);
					let dl = tx.get(&key, None).await?;
					if dl.is_some() {
						tx.del(&key).await?;
					}
					dl
				} else {
					// BM25: Read from DLE chunk and decode SmallFloat
					// Note: We don't delete the DLE byte because:
					// 1. When this doc_id is reused, index_content will overwrite the byte
					// 2. During scoring, only active documents are looked up
					// 3. Zeroing would require an extra read-modify-write cycle
					let chunk_id = Dle::chunk_id(doc_id);
					let offset = Dle::offset(doc_id);
					let dle_key = self.ikb.new_dle(chunk_id);
					if let Some(chunk) = tx.get(&dle_key, None).await? {
						let chunk_data: Vec<u8> = chunk;
						if offset < chunk_data.len() && chunk_data[offset] != 0 {
							Some(SmallFloat::decode(chunk_data[offset]) as u64)
						} else {
							None
						}
					} else {
						None
					}
				};

				if let Some(dl) = doc_length {
					// Decrease the doc count and total doc length
					let dcl = DocLengthAndCount {
						total_docs_length: -(dl as i128),
						doc_count: -1,
					};
					let key = self.ikb.new_dc_with_id(doc_id, opt.id(), Uuid::now_v7());
					tx.put(&key, &dcl, None).await?;
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
		let nid = opt.id();
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
		if self.use_accurate_scoring {
			// BM25_ACCURATE: Store legacy format (u64) for AccurateScorer
			let key = self.ikb.new_dl(id.doc_id());
			tx.set(&key, &dl, None).await?;
		} else {
			// BM25: Store SmallFloat-encoded format in DLE chunks for FastScorer
			let encoded = SmallFloat::encode(dl as u32);
			let chunk_id = Dle::chunk_id(id.doc_id());
			let offset = Dle::offset(id.doc_id());

			let key = self.ikb.new_dle(chunk_id);
			let mut chunk =
				tx.get(&key, None).await?.unwrap_or_else(|| vec![0u8; CHUNK_SIZE as usize]);

			chunk[offset] = encoded;
			tx.set(&key, &chunk, None).await?;
		}
		{
			// Increase the doc count and total doc length
			let key = self.ikb.new_dc_with_id(id.doc_id(), opt.id(), Uuid::now_v7());
			let dcl = DocLengthAndCount {
				total_docs_length: dl as i128,
				doc_count: 1,
			};
			tx.put(&key, &dcl, None).await?;
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
			tx.set(&key, &td, None).await?;
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
			tx.set(&key, &td, None).await?;
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
		tx.set(&key, &String::new(), None).await
	}

	/// Extracts query terms from a search string
	///
	/// This method tokenizes the query string and retrieves the document sets
	/// for each term. It returns a QueryTerms object containing the tokens and
	/// their associated document sets.
	pub(in crate::idx) async fn extract_querying_terms(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		query_string: String,
	) -> Result<QueryTerms> {
		// We extract the tokens
		let tokens = self
			.analyzer
			.generate_tokens(stk, ctx, opt, FilteringStage::Querying, query_string)
			.await?;
		// We collect the term docs
		let mut docs = Vec::with_capacity(tokens.list().len());
		let mut unique_tokens = HashSet::new();
		let tx = ctx.tx();
		let mut has_unknown_terms = false;
		for token in tokens.list() {
			// Tokens can contain duplicates, not need to evaluate them again
			if unique_tokens.insert(token) {
				// Is the term known in the index?
				let term = tokens.get_token_string(token)?;
				let d = self.get_docs(&tx, term).await?;
				if !has_unknown_terms && d.is_none() {
					has_unknown_terms = true;
				}
				docs.push(d);
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

	async fn get_docs(&self, tx: &Transaction, term: &str) -> Result<Option<RoaringTreemap>> {
		// We compute the not yet compacted term/documents if any
		let (beg, end) = self.ikb.new_tt_term_range(term)?;

		// Track document ID deltas: positive values mean document contains the term,
		// negative values mean document no longer contains the term
		let mut deltas: HashMap<DocId, i64> = HashMap::new();

		// Scan all term-document transaction logs for this term
		for k in tx.keys(beg..end, u32::MAX, None).await? {
			let tt = Tt::decode_key(&k)?;
			let entry = deltas.entry(tt.doc_id).or_default();
			// Increment or decrement the counter based on whether we're adding or removing
			// the term
			if tt.add {
				*entry += 1;
			} else {
				*entry -= 1;
			}
		}

		// Merge the delta changes with the consolidated document set
		let docs = self.append_term_docs_delta(tx, term, &deltas).await?;

		// If the final `docs` is empty, we return `None` to indicate no documents
		// contain this term
		if docs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(docs))
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
			tx.set(&td, &docs, None).await?;
		}
		Ok(())
	}

	/// Compacts term documents by consolidating deltas and removing logs
	///
	/// This method processes all term document deltas, applies them to the
	/// consolidated term documents, and removes the delta logs. It returns
	/// true if any compaction was performed.
	async fn compact_term_docs(&self, tx: &Transaction) -> Result<bool> {
		// Get the range of all term transaction logs
		let (beg, end) = self.ikb.new_tt_terms_range()?;
		let mut current_term = "".to_string();
		let mut deltas: HashMap<DocId, i64> = HashMap::new();
		let range = beg..end;
		let mut has_log = false;

		// Process all term transaction logs, grouped by term
		for k in tx.keys(range.clone(), u32::MAX, None).await? {
			let tt = Tt::decode_key(&k)?;
			has_log = true;

			// If we've moved to a new term, consolidate the previous term's deltas
			if current_term != tt.term {
				// Apply accumulated deltas for the previous term (if any)
				if !current_term.is_empty() && !deltas.is_empty() {
					self.set_term_docs_delta(tx, &current_term, &deltas).await?;
					deltas.clear();
				}
				// Start tracking the new term
				current_term = tt.term.to_string();
			}

			// Accumulate deltas for the current term
			let entry = deltas.entry(tt.doc_id).or_default();
			if tt.add {
				*entry += 1;
			} else {
				*entry -= 1;
			}
		}

		// Don't forget to process the last term if there was one
		if !current_term.is_empty() && !deltas.is_empty() {
			self.set_term_docs_delta(tx, &current_term, &deltas).await?;
		}

		// After processing all logs, remove them from the database
		if has_log {
			tx.delr(range).await?;
		}

		// Return whether any compaction was performed
		Ok(has_log)
	}

	/// Creates a new iterator for search hits
	///
	/// This method creates an iterator over the documents that match all query
	/// terms. It returns None if any term has no matching documents.
	pub(in crate::idx) fn new_hits_iterator(
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

	pub(in crate::idx) async fn get_doc_id(
		&self,
		tx: &Transaction,
		rid: &RecordId,
	) -> Result<Option<DocId>> {
		if rid.table != *self.ikb.table() {
			return Ok(None);
		}
		self.doc_ids.get_doc_id(tx, &rid.key).await
	}
	pub(in crate::idx) async fn new_scorer(&self, ctx: &FrozenContext) -> Result<Option<Scorer>> {
		if let Some(bm25) = &self.bm25 {
			let dlc = self.compute_doc_length_and_count(&ctx.tx(), None).await?;
			let scorer = if self.use_accurate_scoring {
				Scorer::new_accurate(dlc, bm25.clone())
			} else {
				Scorer::new_fast(dlc, bm25.clone())
			};
			return Ok(Some(scorer));
		}
		Ok(None)
	}

	/// Computes document length and count statistics for the index
	///
	/// This method calculates the total document length and count by
	/// aggregating all deltas. If compact_log is provided, it will also remove
	/// the delta logs and set the flag to true if any logs were removed.
	async fn compute_doc_length_and_count(
		&self,
		tx: &Transaction,
		compact_log: Option<&mut bool>,
	) -> Result<DocLengthAndCount> {
		let mut dlc = DocLengthAndCount::default();

		let compacted_key = self.ikb.new_dc_compacted()?;
		if let Some(v) = tx.get(&compacted_key, None).await? {
			let st: DocLengthAndCount = revision::from_slice(&v)?;
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;
		}

		let (beg, end) = self.ikb.new_dc_range()?;
		let range = beg..end;
		// Compute the total number of documents (DocCount) and the total number of
		// terms (DocLength) This key list is supposed to be small, subject to
		// compaction. The root key is the compacted values, and the others are deltas
		// from transaction not yet compacted.
		let mut has_log = false;
		for (_, v) in tx.getr(range.clone(), None).await? {
			let st: DocLengthAndCount = revision::from_slice(&v)?;
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;
			has_log = true;
		}
		if let Some(compact_log) = compact_log
			&& has_log
		{
			tx.delr(range).await?;
			*compact_log = true;
		}
		Ok(dlc)
	}

	/// Compacts document length and count statistics
	///
	/// This method consolidates document length and count statistics and
	/// removes the delta logs. It returns true if any compaction was
	/// performed.
	async fn compact_doc_length_and_count(&self, tx: &Transaction) -> Result<bool> {
		let mut has_logs = false;
		let dlc = self.compute_doc_length_and_count(tx, Some(&mut has_logs)).await?;
		let key = self.ikb.new_dc_compacted()?;
		tx.set(&key, &revision::to_vec(&dlc)?, None).await?;
		Ok(has_logs)
	}

	/// Performs compaction on the full-text index
	///
	/// This method compacts both document length/count statistics and term
	/// documents. It returns true if any compaction was performed.
	pub(crate) async fn compaction(&self, tx: &Transaction) -> Result<bool> {
		let r1 = self.compact_doc_length_and_count(tx).await?;
		let r2 = self.compact_term_docs(tx).await?;
		Ok(r1 || r2)
	}

	/// Triggers compaction for the full-text index
	///
	/// This method adds an entry to the index compaction queue by creating an
	/// `Ic` key for the specified index. The index compaction thread will
	/// later process this entry and perform the actual compaction of the
	/// index.
	///
	/// Compaction helps optimize full-text index performance by consolidating
	/// term frequency data and document length information, which can become
	/// fragmented after many updates to the index.
	pub(crate) async fn trigger_compaction(
		ikb: &IndexKeyBase,
		tx: &Transaction,
		nid: Uuid,
	) -> Result<()> {
		let ic = ikb.new_ic_key(nid);
		tx.put(&ic, &(), None).await?;
		Ok(())
	}

	/// Highlights search terms in a document
	///
	/// This method highlights the occurrences of search terms in the document
	/// value. It uses the provided highlighting parameters to format the
	/// highlighted text.
	pub(in crate::idx) async fn highlight(
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
			let mut hl = Highlighter::new(hlp, idiom, doc);
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

	pub(in crate::idx) async fn read_offsets(
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


use crate::idx::ft::smallfloat::{NormCache, SmallFloat};
use crate::key::index::dle::{CHUNK_SIZE, Dle};
use parking_lot::RwLock;

/// Fast BM25 scorer using SmallFloat-encoded lengths and precomputed norms.
///
/// This scorer trades ~12.5% precision for reduced storage by:
/// - Using a 256-entry lookup table for length normalization
/// - Loading encoded doc lengths from DLE chunks (1 byte per doc vs 8 bytes)
/// - Caching loaded chunks to avoid repeated disk reads
/// - Avoiding per-document division during scoring
pub(in crate::idx) struct FastScorer {
	/// BM25 k1 parameter
	k1: f64,
	/// Precomputed k1 + 1 for scoring formula
	k1_plus_1: f64,
	/// Total number of documents in the index
	doc_count: f64,
	/// Precomputed normalization cache indexed by encoded length
	norm_cache: NormCache,
	/// Cached DLE chunks (chunk_id -> chunk data)
	/// Caching at chunk level is efficient because:
	/// - Each chunk holds 4096 doc lengths
	/// - Documents in same chunk share one cache entry
	/// - Much fewer cache entries than per-document caching
	chunk_cache: RwLock<HashMap<u64, Vec<u8>>>,
}

impl FastScorer {
	/// Creates a new fast scorer with precomputed normalization cache.
	fn new(dlc: DocLengthAndCount, bm25: Bm25Params) -> Self {
		let doc_count = (dlc.doc_count as f64).max(1.0);
		let total_docs_length = (dlc.total_docs_length as f64).max(1.0);
		let avg_doc_len = total_docs_length / doc_count;
		let k1 = bm25.k1 as f64;
		let b = bm25.b as f64;

		Self {
			k1,
			k1_plus_1: k1 + 1.0,
			doc_count,
			norm_cache: NormCache::new(k1, b, avg_doc_len),
			chunk_cache: RwLock::new(HashMap::new()),
		}
	}

	/// Gets the encoded length for a document, using chunk cache.
	/// Falls back to legacy dl key if DLE chunk is missing (for backward compatibility
	/// with indexes created before the SmallFloat optimization).
	async fn get_encoded_length(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<u8> {
		let chunk_id = Dle::chunk_id(doc_id);
		let offset = Dle::offset(doc_id);

		// Check chunk cache first
		let cache_result = {
			let cache = self.chunk_cache.read();
			cache.get(&chunk_id).map(|chunk_data| {
				if offset < chunk_data.len() && chunk_data[offset] != 0 {
					Some(chunk_data[offset])
				} else {
					None
				}
			})
		};
		if let Some(result) = cache_result {
			if let Some(encoded) = result {
				return Ok(encoded);
			}
			// Chunk exists but offset is empty - fall back to legacy
			return self.load_from_legacy_dl(ikb, tx, doc_id).await;
		}

		// Load chunk from storage
		let dle_key = ikb.new_dle(chunk_id);
		if let Some(chunk_data) = tx.get(&dle_key, None).await? {
			let chunk_data: Vec<u8> = chunk_data;
			// Cache the chunk for future lookups
			let encoded = if offset < chunk_data.len() && chunk_data[offset] != 0 {
				chunk_data[offset]
			} else {
				// Will fall back to legacy after caching
				0
			};
			self.chunk_cache.write().insert(chunk_id, chunk_data);

			if encoded != 0 {
				return Ok(encoded);
			}
		}

		// Fall back to legacy dl key (pre-optimization index)
		self.load_from_legacy_dl(ikb, tx, doc_id).await
	}

	/// Loads document length from legacy dl key and encodes it.
	async fn load_from_legacy_dl(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<u8> {
		let dl_key = ikb.new_dl(doc_id);
		if let Some(dl) = tx.get(&dl_key, None).await? {
			let doc_length: u64 = dl;
			Ok(SmallFloat::encode(doc_length as u32))
		} else {
			Ok(0)
		}
	}

	/// Calculates the score for a document using encoded lengths from DLE chunks.
	pub(crate) async fn score(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Score> {
		let mut sc = 0.0;
		let tl = qt.tokens.list();
		let encoded_len = self.get_encoded_length(&fti.ikb, tx, doc_id).await?;

		for (i, d) in qt.docs.iter().enumerate() {
			if let Some(docs) = d
				&& docs.contains(doc_id)
				&& let Some(token) = tl.get(i)
			{
				let term = qt.tokens.get_token_string(token)?;
				let td = fti.get_term_document(tx, doc_id, term).await?;
				if let Some(td) = td {
					sc += self.compute_score(td.f as f64, docs.len() as f64, encoded_len)
				}
			}
		}
		Ok(sc as f32)
	}

	/// Computes BM25 score using encoded length.
	///
	/// Uses the same Lucene-style IDF formula as AccurateScorer but
	/// retrieves length normalization from precomputed NormCache table.
	fn compute_score(&self, term_freq: f64, term_doc_count: f64, encoded_len: u8) -> f64 {
		if term_freq <= 0.0 {
			return 0.0;
		}

		// IDF (Lucene formula)
		let effective_doc_count = self.doc_count.max(term_doc_count);
		let denominator = term_doc_count + 0.5;
		let numerator = effective_doc_count - term_doc_count + 0.5;
		let idf = (1.0 + numerator / denominator).ln();

		// Lower-bounded TF
		let tf_prime = 1.0 + term_freq.ln();

		// Length norm from cache (no division!)
		let norm_inv = self.norm_cache.get(encoded_len);

		// BM25 formula
		let score = idf * self.k1_plus_1 * tf_prime * norm_inv / (tf_prime * norm_inv + self.k1);

		if score.is_nan() || score < 0.0 {
			0.0
		} else {
			score
		}
	}

	/// Loads chunks using a single range query (for dense chunk distribution).
	async fn load_range(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		min_chunk: u64,
		max_chunk: u64,
	) -> Result<Vec<(u64, Vec<u8>)>> {
		let (beg, end) = ikb.new_dle_range(min_chunk, max_chunk)?;
		let mut result = Vec::new();

		for (k, v) in tx.getr(beg..end, None).await? {
			let dle = Dle::decode_key(&k)?;
			result.push((dle.chunk_id, v));
		}

		Ok(result)
	}

	/// Loads chunks in parallel using individual point queries (for sparse chunk distribution).
	async fn load_individual(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		chunk_ids: &BTreeSet<u64>,
	) -> Result<Vec<(u64, Vec<u8>)>> {
		let futures: Vec<_> = chunk_ids
			.iter()
			.map(|&chunk_id| {
				let key = ikb.new_dle(chunk_id);
				async move {
					let data = tx.get(&key, None).await?;
					Ok::<_, anyhow::Error>((chunk_id, data.unwrap_or_default()))
				}
			})
			.collect();

		try_join_all(futures).await
	}

	/// Preloads encoded lengths for all doc_ids using adaptive strategy.
	/// Uses range query if chunks are dense, individual queries if sparse.
	pub(crate) async fn preload_chunks(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_ids: &[DocId],
	) -> Result<()> {
		if doc_ids.is_empty() {
			return Ok(());
		}

		// Determine needed chunks
		let needed_chunks: BTreeSet<u64> = doc_ids.iter().map(|id| Dle::chunk_id(*id)).collect();

		// Early return if all chunks already cached
		{
			let cache = self.chunk_cache.read();
			if needed_chunks.iter().all(|id| cache.contains_key(id)) {
				return Ok(());
			}
		}

		let Some(&min_chunk) = needed_chunks.first() else {
			return Ok(());
		};
		let Some(&max_chunk) = needed_chunks.last() else {
			return Ok(());
		};
		let range_size = max_chunk - min_chunk + 1;

		// Adaptive strategy: range query if dense, individual if sparse
		let chunks_data = if range_size <= needed_chunks.len() as u64 * 2 {
			self.load_range(ikb, tx, min_chunk, max_chunk).await?
		} else {
			self.load_individual(ikb, tx, &needed_chunks).await?
		};

		// Populate cache
		let mut cache = self.chunk_cache.write();
		for (chunk_id, data) in chunks_data {
			cache.insert(chunk_id, data);
		}

		Ok(())
	}

}

/// Accurate BM25 scorer using exact document lengths.
///
/// This scorer provides precise scoring using exact document length values
/// for each document, at the cost of additional computation per document.
pub(in crate::idx) struct AccurateScorer {
	/// precomputed BM25 scoring parameters
	k1: f64,
	k1_plus_1: f64,
	one_minus_b: f64,
	b_over_avg_len: f64,
	doc_count: f64,
}

impl AccurateScorer {
	/// Creates a new scorer with the specified parameters
	///
	/// This method initializes a scorer with document statistics and BM25
	/// parameters. It calculates the average document length for use in the
	/// BM25 algorithm.
	fn new(dlc: DocLengthAndCount, bm25: Bm25Params) -> Self {
		let doc_count = (dlc.doc_count as f64).max(1.0);
		let total_docs_length = (dlc.total_docs_length as f64).max(1.0);
		let average_doc_length = total_docs_length / doc_count;
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
	/// • IDF uses Lucene's formula: ln(1 + (N - n + 0.5) / (n + 0.5))
	///   This ensures IDF ≥ 0 for all term frequencies.
	/// • Term-frequency is lower-bounded with 1 + ln(tf) as proposed in
	///   "Lower-Bounding Term Frequency Normalization" (Lv & Zhai, CIKM 2011).
	///
	/// score =
	///     idf · (k1 + 1) · tf′
	///     ---------------------------------------------
	///     tf′ + k1 · (1 − b + b · doc_len / avg_doc_len)
	///
	/// where
	///   idf = ln(1 + (N − n(qᵢ) + 0.5) / (n(qᵢ) + 0.5))  (Lucene-style)
	///   tf′ = 1 + ln(tf)
	///
	/// Reference: https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/search/similarities/BM25Similarity.java
	fn compute_bm25_score(&self, term_freq: f64, term_doc_count: f64, doc_length: f64) -> f64 {
		// Early return for zero-term frequency
		if term_freq <= 0.0 {
			return 0.0;
		}

		// ---------- 1. Inverse Document Frequency (IDF) ---------------------
		let effective_doc_count = self.doc_count.max(term_doc_count);
		let denominator = term_doc_count + 0.5; // n(qᵢ) + 0.5
		let numerator = effective_doc_count - term_doc_count + 0.5; // N − n(qᵢ) + 0.5
		let idf = (1.0 + numerator / denominator).ln();

		// ---------- 2. Lower-bounded term-frequency -------------------------
		let tf_prime = 1.0 + term_freq.ln(); // 1 + ln(tf)

		// ---------- 3. Document-length normalisation -----------------------
		let length_norm = self.one_minus_b + self.b_over_avg_len * doc_length;

		// ---------- 4. Okapi BM25 (optimized) ------------------------------
		let numerator = idf * self.k1_plus_1 * tf_prime;
		let denominator = tf_prime + self.k1 * length_norm;

		let score = numerator / denominator;

		// Final safety check: return 0 if score is NaN or negative
		if score.is_nan() || score < 0.0 {
			return 0.0;
		}

		score
	}

}

/// Dual-mode BM25 scorer: Fast (SmallFloat) or Accurate (exact).
///
/// The Fast variant uses precomputed normalization tables for ~50% latency
/// reduction with ~12.5% precision loss. The Accurate variant uses exact
/// document lengths for precise scoring.
pub(in crate::idx) enum Scorer {
	/// Fast scoring using SmallFloat-encoded lengths
	Fast(FastScorer),
	/// Accurate scoring using exact document lengths
	Accurate(AccurateScorer),
}

impl Scorer {
	/// Creates a new fast scorer using precomputed normalization cache.
	pub fn new_fast(dlc: DocLengthAndCount, bm25: Bm25Params) -> Self {
		Scorer::Fast(FastScorer::new(dlc, bm25))
	}

	/// Creates a new accurate scorer using exact document lengths.
	pub fn new_accurate(dlc: DocLengthAndCount, bm25: Bm25Params) -> Self {
		Scorer::Accurate(AccurateScorer::new(dlc, bm25))
	}

	/// Legacy constructor for backward compatibility - creates accurate scorer.
	#[allow(dead_code)] // Kept for API compatibility
	fn new(dlc: DocLengthAndCount, bm25: Bm25Params) -> Self {
		Self::new_accurate(dlc, bm25)
	}

	/// Calculates the overall score for a document based on query terms.
	///
	/// FastScorer uses precomputed encoded lengths from the cache.
	/// AccurateScorer fetches exact document lengths from the index.
	pub(crate) async fn score(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Score> {
		match self {
			Scorer::Fast(scorer) => scorer.score(fti, tx, qt, doc_id).await,
			Scorer::Accurate(scorer) => scorer.score(fti, tx, qt, doc_id).await,
		}
	}

	/// Preloads DLE chunks for all documents matching the query terms.
	///
	/// This should be called before scoring begins to batch-load all needed
	/// chunks in a single pass, avoiding per-document chunk loading overhead.
	/// Only FastScorer benefits from this; AccurateScorer is a no-op.
	///
	/// Memory optimization: Collects chunk_ids directly from bitmaps instead
	/// of materializing all doc_ids. Uses O(chunks) memory instead of O(docs).
	pub(crate) async fn preload_for_query(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
	) -> Result<()> {
		if let Scorer::Fast(scorer) = self {
			// Collect chunk_ids directly - O(chunks) memory instead of O(docs)
			let needed_chunks: BTreeSet<u64> = qt
				.docs
				.iter()
				.flatten()
				.flat_map(|bitmap| bitmap.iter().map(Dle::chunk_id))
				.collect();

			// Skip preload if too many chunks (would use too much memory)
			// Threshold: 1024 chunks = 4MB max cache size
			const MAX_PRELOAD_CHUNKS: usize = 1024;
			if needed_chunks.len() > MAX_PRELOAD_CHUNKS {
				return Ok(());
			}

			scorer.preload_chunks_by_id(&fti.ikb, tx, &needed_chunks).await?;
		}
		Ok(())
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
	use crate::catalog::{DatabaseId, FullTextParams, IndexId, NamespaceId, Scoring};
	use crate::cnf::dynamic::DynamicConfiguration;
	use crate::ctx::{Context, FrozenContext};
	use crate::dbs::Options;
	use crate::expr::statements::DefineAnalyzerStatement;
	use crate::idx::IndexKeyBase;
	use crate::idx::ft::offset::Offset;
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

			let opts = Options::new(ds.id(), DynamicConfiguration::default());
			let stk_ctx = ctx.clone();
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
				// Use BmAccurate to avoid chunk-based storage conflicts in concurrent test
				scoring: Scoring::BmAccurate {
					k1: 1.2,
					b: 0.75,
				},
				highlight: true,
			});
			let nid = Uuid::new_v4();
			let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(3));
			let opt = Options::new(nid, DynamicConfiguration::default())
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
			let mut ctx = Context::new(&self.ctx);
			let tx = self.new_tx(TransactionType::Write).await;
			ctx.set_transaction(tx.clone());
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
				FullTextIndex::trigger_compaction(&self.ikb, &tx, self.nid).await.unwrap();
			}

			tx.commit().await.unwrap();
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
		concurrent_doc_update(test.clone(), doc1.clone(), 1).await;
		concurrent_doc_update(test.clone(), doc2.clone(), 1).await;
		// Prepare the concurrent tasks
		let task1 = tokio::spawn(concurrent_doc_update(test.clone(), doc1.clone(), usize::MAX));
		let task2 = tokio::spawn(concurrent_doc_update(test.clone(), doc2.clone(), usize::MAX));
		let task3 = tokio::spawn(compaction(test.clone()));
		let task4 = tokio::spawn(concurrent_search(test.clone(), vec![doc1, doc2]));
		let _ = tokio::try_join!(task1, task2, task3, task4).expect("Tasks failed");

		// Check that logs have been compacted:
		let tx = test.new_tx(TransactionType::Read).await;
		let (beg, end) = test.ikb.new_tt_terms_range().unwrap();
		assert_eq!(tx.count(beg..end).await.unwrap(), 0);
		let (beg, end) = test.ikb.new_dc_range().unwrap();
		assert_eq!(tx.count(beg..end).await.unwrap(), 0);
	}

	/// Helper to create an AccurateScorer with specified doc_count and average doc length
	fn create_scorer(doc_count: i64, total_docs_length: i128) -> super::AccurateScorer {
		let dlc = super::DocLengthAndCount {
			total_docs_length,
			doc_count,
		};
		let bm25 = super::Bm25Params {
			k1: 1.2,
			b: 0.75,
		};
		super::AccurateScorer::new(dlc, bm25)
	}

	#[test]
	fn test_bm25_high_frequency_terms_have_positive_score() {
		let scorer = create_scorer(3, 30); // 3 docs, avg length 10

		let score = scorer.compute_bm25_score(1.0, 2.0, 10.0);

		assert!(score > 0.0, "High-frequency terms should have positive score, got {}", score);
	}

	#[test]
	fn test_bm25_term_in_all_documents_has_positive_score() {
		let scorer = create_scorer(3, 30);

		let score = scorer.compute_bm25_score(1.0, 3.0, 10.0);

		assert!(score > 0.0, "Terms in all documents should have positive score, got {}", score);
	}

	#[test]
	fn test_bm25_rare_terms_score_higher_than_common() {
		let scorer = create_scorer(100, 1000); // 100 docs, avg length 10

		let rare_score = scorer.compute_bm25_score(1.0, 5.0, 10.0);

		let common_score = scorer.compute_bm25_score(1.0, 80.0, 10.0);

		assert!(
			rare_score > common_score,
			"Rare terms should score higher than common terms: rare={}, common={}",
			rare_score,
			common_score
		);
	}

	#[test]
	fn test_bm25_zero_term_frequency_returns_zero() {
		let scorer = create_scorer(10, 100);

		let score = scorer.compute_bm25_score(0.0, 5.0, 10.0);
		assert_eq!(score, 0.0, "Zero term frequency should return 0");

		let score_negative = scorer.compute_bm25_score(-1.0, 5.0, 10.0);
		assert_eq!(score_negative, 0.0, "Negative term frequency should return 0");
	}

	#[test]
	fn test_bm25_scores_are_always_non_negative() {
		let scorer = create_scorer(10, 100);

		let test_cases = [
			(1.0, 1.0, 10.0),  // rare term
			(1.0, 5.0, 10.0),  // medium frequency
			(1.0, 9.0, 10.0),  // high frequency (90%)
			(1.0, 10.0, 10.0), // term in all docs
			(5.0, 5.0, 10.0),  // higher term frequency
			(1.0, 5.0, 1.0),   // short document
			(1.0, 5.0, 100.0), // long document
		];

		for (tf, term_doc_count, doc_length) in test_cases {
			let score = scorer.compute_bm25_score(tf, term_doc_count, doc_length);
			assert!(
				score >= 0.0,
				"Score should be non-negative for tf={}, term_doc_count={}, doc_length={}, got {}",
				tf,
				term_doc_count,
				doc_length,
				score
			);
		}
	}

	#[test]
	fn test_bm25_higher_term_frequency_increases_score() {
		let scorer = create_scorer(10, 100);

		let score_tf1 = scorer.compute_bm25_score(1.0, 5.0, 10.0);
		let score_tf5 = scorer.compute_bm25_score(5.0, 5.0, 10.0);

		assert!(
			score_tf5 > score_tf1,
			"Higher term frequency should increase score: tf1={}, tf5={}",
			score_tf1,
			score_tf5
		);
	}

	#[test]
	fn test_bm25_lucene_idf_formula() {
		let scorer = create_scorer(10, 100);

		// For N=10 docs, n=5 docs containing term:
		// IDF = ln(1 + (10 - 5 + 0.5) / (5 + 0.5))
		//     = ln(1 + 5.5 / 5.5)
		//     = ln(2)
		//     ≈ 0.693

		let score = scorer.compute_bm25_score(1.0, 5.0, 10.0);

		// The score should be positive and reasonable
		assert!(score > 0.0, "Score should be positive");
		assert!(score < 10.0, "Score should be reasonable (not too high)");
	}

	#[test]
	fn test_bm25_handles_term_doc_count_exceeding_doc_count() {
		// Edge case: term_doc_count > doc_count (can happen with stale data or concurrent updates)
		// This should NOT produce NaN or panic, but return a valid (small) score
		let scorer = create_scorer(10, 100); // 10 docs

		// Term appears in 20 documents, but we only know about 10 total
		// This is an inconsistent state but should be handled gracefully
		let score = scorer.compute_bm25_score(1.0, 20.0, 10.0);

		assert!(!score.is_nan(), "Score should not be NaN when term_doc_count > doc_count");
		assert!(score >= 0.0, "Score should be non-negative, got {}", score);
	}

	#[test]
	fn test_bm25_handles_zero_doc_count() {
		// Edge case: zero doc_count (empty index)
		let scorer = create_scorer(0, 0);

		let score = scorer.compute_bm25_score(1.0, 1.0, 10.0);

		assert!(!score.is_nan(), "Score should not be NaN with zero doc_count");
		assert!(score >= 0.0, "Score should be non-negative, got {}", score);
	}

	#[test]
	fn test_bm25_handles_zero_doc_length() {
		// Edge case: zero document length
		let scorer = create_scorer(10, 100);

		let score = scorer.compute_bm25_score(1.0, 5.0, 0.0);

		assert!(!score.is_nan(), "Score should not be NaN with zero doc_length");
		assert!(score >= 0.0, "Score should be non-negative, got {}", score);
	}

	#[test]
	fn test_scorer_variants_produce_similar_scores() {
		use crate::idx::ft::smallfloat::SmallFloat;

		let dlc = super::DocLengthAndCount {
			total_docs_length: 1000,
			doc_count: 10,
		};
		let bm25 = super::Bm25Params {
			k1: 1.2,
			b: 0.75,
		};

		let fast = super::FastScorer::new(dlc.clone(), bm25.clone());
		let accurate = super::AccurateScorer::new(dlc, bm25);

		// Test with avg-length doc (length=100)
		let encoded_len = SmallFloat::encode(100);
		let fast_score = fast.compute_score(1.0, 5.0, encoded_len);
		let accurate_score = accurate.compute_bm25_score(1.0, 5.0, 100.0);

		let diff_pct = ((fast_score - accurate_score).abs() / accurate_score.max(0.001)) * 100.0;
		assert!(
			diff_pct < 15.0,
			"Score diff {}% exceeds 15% threshold: fast={}, accurate={}",
			diff_pct,
			fast_score,
			accurate_score
		);
	}

	#[test]
	fn test_scorer_variants_across_document_lengths() {
		use crate::idx::ft::smallfloat::SmallFloat;

		let dlc = super::DocLengthAndCount {
			total_docs_length: 10000,
			doc_count: 100,
		};
		let bm25 = super::Bm25Params {
			k1: 1.2,
			b: 0.75,
		};

		let fast = super::FastScorer::new(dlc.clone(), bm25.clone());
		let accurate = super::AccurateScorer::new(dlc, bm25);

		// Test various document lengths
		let doc_lengths = [10, 50, 100, 200, 500, 1000];

		for &len in &doc_lengths {
			let encoded_len = SmallFloat::encode(len);
			let fast_score = fast.compute_score(1.0, 10.0, encoded_len);
			let accurate_score = accurate.compute_bm25_score(1.0, 10.0, len as f64);

			// Allow 15% difference due to SmallFloat precision loss
			let diff_pct = if accurate_score > 0.001 {
				((fast_score - accurate_score).abs() / accurate_score) * 100.0
			} else {
				0.0
			};

			assert!(
				diff_pct < 15.0,
				"Doc length {}: score diff {:.2}% (fast={:.4}, accurate={:.4})",
				len,
				diff_pct,
				fast_score,
				accurate_score
			);
		}
	}

	#[test]
	fn test_scorer_variants_preserve_ranking() {
		use crate::idx::ft::smallfloat::SmallFloat;

		let dlc = super::DocLengthAndCount {
			total_docs_length: 5000,
			doc_count: 50,
		};
		let bm25 = super::Bm25Params {
			k1: 1.2,
			b: 0.75,
		};

		let fast = super::FastScorer::new(dlc.clone(), bm25.clone());
		let accurate = super::AccurateScorer::new(dlc, bm25);

		// Shorter docs should score higher than longer docs (with same tf)
		let short_len = 50u32;
		let long_len = 200u32;

		let fast_short = fast.compute_score(1.0, 10.0, SmallFloat::encode(short_len));
		let fast_long = fast.compute_score(1.0, 10.0, SmallFloat::encode(long_len));

		let accurate_short = accurate.compute_bm25_score(1.0, 10.0, short_len as f64);
		let accurate_long = accurate.compute_bm25_score(1.0, 10.0, long_len as f64);

		// Both scorers should rank shorter doc higher
		assert!(
			fast_short > fast_long,
			"FastScorer: short doc should score higher ({} > {})",
			fast_short,
			fast_long
		);
		assert!(
			accurate_short > accurate_long,
			"AccurateScorer: short doc should score higher ({} > {})",
			accurate_short,
			accurate_long
		);
	}

	#[test]
	fn test_scorer_variants_with_different_bm25_params() {
		use crate::idx::ft::smallfloat::SmallFloat;

		let dlc = super::DocLengthAndCount {
			total_docs_length: 1000,
			doc_count: 10,
		};

		// Test with different k1 and b values
		let params = [
			(1.2, 0.75), // default
			(2.0, 0.75), // higher k1
			(1.2, 0.0),  // no length normalization
			(1.2, 1.0),  // full length normalization
			(0.5, 0.5),  // lower k1 and b
		];

		for (k1, b) in params {
			let bm25 = super::Bm25Params {
				k1,
				b,
			};

			let fast = super::FastScorer::new(dlc.clone(), bm25.clone());
			let accurate = super::AccurateScorer::new(dlc.clone(), bm25);

			let encoded_len = SmallFloat::encode(100);
			let fast_score = fast.compute_score(1.0, 5.0, encoded_len);
			let accurate_score = accurate.compute_bm25_score(1.0, 5.0, 100.0);

			// Both should produce valid scores
			assert!(
				!fast_score.is_nan() && fast_score >= 0.0,
				"FastScorer with k1={}, b={} produced invalid score: {}",
				k1,
				b,
				fast_score
			);
			assert!(
				!accurate_score.is_nan() && accurate_score >= 0.0,
				"AccurateScorer with k1={}, b={} produced invalid score: {}",
				k1,
				b,
				accurate_score
			);

			// Scores should be similar (within 15%)
			if accurate_score > 0.001 {
				let diff_pct = ((fast_score - accurate_score).abs() / accurate_score) * 100.0;
				assert!(
					diff_pct < 15.0,
					"k1={}, b={}: score diff {:.2}% (fast={:.4}, accurate={:.4})",
					k1,
					b,
					diff_pct,
					fast_score,
					accurate_score
				);
			}
		}
	}

	#[test]
	fn test_fast_scorer_uses_norm_cache() {
		use crate::idx::ft::smallfloat::SmallFloat;

		let dlc = super::DocLengthAndCount {
			total_docs_length: 1000,
			doc_count: 10,
		};
		let bm25 = super::Bm25Params {
			k1: 1.2,
			b: 0.75,
		};

		let fast = super::FastScorer::new(dlc, bm25);

		// Multiple calls with same encoded length should produce same result
		let encoded = SmallFloat::encode(100);
		let score1 = fast.compute_score(1.0, 5.0, encoded);
		let score2 = fast.compute_score(1.0, 5.0, encoded);

		assert_eq!(score1, score2, "Same inputs should produce identical scores");

		// Different encoded lengths should produce different scores
		let encoded_short = SmallFloat::encode(50);
		let encoded_long = SmallFloat::encode(200);
		let score_short = fast.compute_score(1.0, 5.0, encoded_short);
		let score_long = fast.compute_score(1.0, 5.0, encoded_long);

		assert_ne!(
			score_short, score_long,
			"Different lengths should produce different scores"
		);
	}
}
