use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use roaring::RoaringTreemap;
use roaring::treemap::IntoIter;
use uuid::Uuid;

use crate::catalog;
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
use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::Idiom;
use crate::expr::operator::BooleanOperator;
use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::docids::seqdocids::SeqDocIds;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::Tokens;
use crate::idx::ft::highlighter::{HighlightParams, Highlighter, Offseter};
use crate::idx::ft::offset::Offset;
use crate::idx::ft::search::Bm25Params;
use crate::idx::ft::{DocLength, Score, TermFrequency};
use crate::idx::planner::iterators::MatchesHitsIterator;
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
#[derive(Debug, Default)]
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

impl FullTextIndex {
	/// Creates a new full-text index with the specified parameters
	///
	/// This method retrieves the analyzer from the database and then calls
	/// `with_analyzer`
	pub(crate) async fn new(
		nid: Uuid,
		ixs: &IndexStores,
		tx: &Transaction,
		ikb: IndexKeyBase,
		p: &FullTextParams,
	) -> Result<Self> {
		let az = tx.get_db_analyzer(ikb.0.ns, ikb.0.db, &p.analyzer).await?;
		ixs.mappers().check(&az).await?;
		Self::with_analyzer(nid, ixs, az, ikb, p)
	}

	/// Creates a new full-text index with the specified analyzer
	///
	/// This method initializes the index with the provided analyzer and
	/// parameters
	fn with_analyzer(
		nid: Uuid,
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
			doc_ids: SeqDocIds::new(nid, ikb.clone()),
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
		ctx: &Context,
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
		let nid = opt.id()?;
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
					let key = self.ikb.new_dc_with_id(doc_id, opt.id()?, Uuid::now_v7());
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
	pub(crate) async fn remove_doc(&self, ctx: &Context, doc_id: DocId) -> Result<()> {
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
		ctx: &Context,
		opt: &Options,
		rid: &RecordId,
		content: Vec<Value>,
		require_compaction: &mut bool,
	) -> Result<()> {
		let tx = ctx.tx();
		let nid = opt.id()?;
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
			tx.set(&key, &dl, None).await?;
		}
		{
			// Increase the doc count and total doc length
			let key = self.ikb.new_dc_with_id(id.doc_id(), opt.id()?, Uuid::now_v7());
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
		ctx: &Context,
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
		if let Some(hits) = hits {
			if !hits.is_empty() {
				return Some(FullTextHitsIterator::new(self.ikb.clone(), hits));
			}
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
		if rid.table != self.ikb.table() {
			return Ok(None);
		}
		self.doc_ids.get_doc_id(tx, &rid.key).await
	}
	pub(in crate::idx) async fn new_scorer(&self, ctx: &Context) -> Result<Option<Scorer>> {
		if let Some(bm25) = &self.bm25 {
			let dlc = self.compute_doc_length_and_count(&ctx.tx(), None).await?;
			let sc = Scorer::new(dlc, bm25.clone());
			return Ok(Some(sc));
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
		if let Some(compact_log) = compact_log {
			if has_log {
				tx.delr(range).await?;
				*compact_log = true;
			}
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
			return or.try_into().map_err(anyhow::Error::new);
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
					table: self.ikb.table().to_string(),
					key,
				};
				return Ok(Some((rid, doc_id)));
			}
		}
		Ok(None)
	}
}

/// Implements BM25 scoring for relevance ranking of search results
pub(in crate::idx) struct Scorer {
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
			if let Some(docs) = d {
				if docs.contains(doc_id) {
					if let Some(token) = tl.get(i) {
						let term = qt.tokens.get_token_string(token)?;
						let td = fti.get_term_document(tx, doc_id, term).await?;
						if let Some(td) = td {
							sc +=
								self.compute_bm25_score(td.f as f64, docs.len() as f64, doc_length)
						}
					}
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
	use crate::catalog::{DatabaseId, FullTextParams, NamespaceId};
	use crate::ctx::{Context, MutableContext};
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
		ctx: Context,
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
			let az = Arc::new(DefineAnalyzerStatement::from(az).to_definition());
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
			let nid = Uuid::from_u128(1);
			let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t", "i");
			let opt = Options::default()
				.with_id(nid)
				.with_ns(Some("testns".into()))
				.with_db(Some("testdb".into()));
			let fti = Arc::new(
				FullTextIndex::with_analyzer(
					nid,
					ctx.get_index_stores(),
					az.clone(),
					ikb.clone(),
					&ft_params,
				)
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
			let mut ctx = MutableContext::new(&self.ctx);
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
		let doc1: Arc<RecordId> =
			Arc::new(RecordId::new("t".to_owned(), strand!("doc1").to_owned()));
		let doc2: Arc<RecordId> =
			Arc::new(RecordId::new("t".to_owned(), strand!("doc2").to_owned()));

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
}
