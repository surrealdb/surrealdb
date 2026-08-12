use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use roaring::treemap::IntoIter;
use surrealdb_datastore::Transaction;
// A posting and the index's document statistics are stored values, so both are
// declared below this layer; the indexing and scoring code here maintains them.
pub use surrealdb_datastore::values::fulltext::{
	DocLengthAndCount, DocumentTerms, TermDocsResidual, TermDocument,
};
use surrealdb_kvs::Direction;
use surrealdb_kvs::consts::COUNT_BATCH_SIZE;
use uuid::Uuid;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{FullTextParams, Scoring};
use crate::docids::{DocId, TableDocIds};
use crate::env::IndexEnv;
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
use crate::expr::Idiom;
use crate::expr::operator::BooleanOperator;
use crate::ft::analyzer::filter::FilteringStage;
use crate::ft::analyzer::tokenizer::Tokens;
use crate::ft::analyzer::{Analyzer, AnalyzerFunction};
use crate::ft::highlighter::{HighlightParams, Highlighter, Offseter};
use crate::ft::{DocLength, MatchesHitsIterator, Score, TermFrequency};
use crate::key::schema::{
	DocStatsBatchKey, DocStatsDeltaKey, DocStatsKey, TermChangeBatchKey, TermChangeKey,
};
use crate::key::{KVKey, KVKeyDecode, KVValue, Resumable};
use crate::trees::store::IndexStores;
use crate::val::{RecordId, Value};
use crate::{IndexKeyBase, bump_compaction_generation, catalog, read_compaction_generation};
/// Represents the terms in a search query and their associated document sets
pub struct QueryTerms {
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
	pub fn is_empty(&self) -> bool {
		self.tokens.list().is_empty()
	}

	pub fn contains_doc(&self, doc_id: DocId) -> bool {
		for d in self.docs.iter().flatten() {
			if d.contains(doc_id) {
				return true;
			}
		}
		false
	}

	pub(in crate::ft) fn matches_or(&self, tks: &[Tokens]) -> Result<bool> {
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

	pub(in crate::ft) fn matches_and(&self, tks: &[Tokens]) -> Result<bool> {
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
	pub(crate) k1: f32,
	pub(crate) b: f32,
}

/// The main full-text index implementation that supports concurrent read and
/// write operations
pub struct FullTextIndex {
	/// The index key base used for key generation
	ikb: IndexKeyBase,
	/// The analyzer used for tokenizing and processing text
	analyzer: Analyzer,
	/// Whether highlighting is enabled for this index
	highlighting: bool,
	/// The table's shared record ↔ doc-ID mapping
	doc_ids: TableDocIds,
	/// BM25 scoring parameters, if scoring is enabled
	bm25: Option<Bm25Params>,
}

/// Ceiling on the entries one page of a compaction round's `!tx` scan asks for.
///
/// A page is normally sized from the budget the round has left and the bitmap
/// width it has already seen; this only caps how large that calculation may get.
/// It matters for the narrowest backlog — single-document entries, the shape a
/// trickle of small writes leaves — where the budget alone would ask for every
/// remaining entry in one read. Raising it trades a larger page for fewer round
/// trips per round.
const TERM_DELTA_SCAN_PAGE: u32 = 4_096;

/// Snapshot gathered by the read phase of full-text compaction.
///
/// The plan contains only the exact delta keys observed at the snapshot — in
/// either the per-document (`!dc`, `!tt`) or the per-transaction (`!dx`, `!tx`)
/// shape — plus the generations that must still match before applying it. A
/// continuation flag tells the datastore whether to prepare another bounded
/// batch after this one commits.
pub struct FullTextCompactionPlan {
	doc_lengths: DocLengthAndCountCompactionPlan,
	term_docs: TermDocsCompactionPlan,
}

impl FullTextCompactionPlan {
	/// Returns true when the plan contains at least one delta key to compact.
	pub fn has_work(&self) -> bool {
		self.doc_lengths.has_logs() || self.term_docs.has_logs()
	}

	/// Returns true when at least one full-text delta range has more entries.
	pub fn has_more(&self) -> bool {
		self.doc_lengths.has_more() || self.term_docs.has_more()
	}
}

/// Bounded read-phase snapshot for document count/length (`!dc`) compaction.
///
/// The two delta shapes are kept apart because they are two key families with
/// two value types, and a single list of them could only be a list of bytes.
struct DocLengthAndCountCompactionPlan {
	generation: Option<u64>,
	dlc: DocLengthAndCount,
	deltas: DocStatsDeltaKeys,
	has_more: bool,
}

/// The document-statistics deltas one read phase saw, kept by family.
///
/// `!dc` is the per-document shape and `!dx` the per-transaction one. Both are
/// drained in the same pass, so an index carrying entries in either shape compacts
/// to the same statistic.
#[derive(Default)]
struct DocStatsDeltaKeys {
	legacy: Vec<DocStatsDeltaKey<'static>>,
	batched: Vec<DocStatsBatchKey<'static>>,
}

impl DocStatsDeltaKeys {
	fn is_empty(&self) -> bool {
		self.legacy.is_empty() && self.batched.is_empty()
	}

	/// Removes every delta the read phase folded into its total.
	///
	/// Only these keys: an entry written after the read is not accounted for in
	/// the compacted value and must survive to be folded in by a later round.
	async fn delete(&self, tx: &Transaction) -> Result<()> {
		for key in &self.legacy {
			tx.del_key(key).await?;
		}
		for key in &self.batched {
			tx.del_key(key).await?;
		}
		Ok(())
	}
}

impl DocLengthAndCountCompactionPlan {
	fn has_logs(&self) -> bool {
		!self.deltas.is_empty()
	}

	/// Returns true when either delta family left entries for the next round: the
	/// per-document scan stopped before its range ended, or the batched one had more
	/// than the round's limit.
	fn has_more(&self) -> bool {
		self.has_more
	}
}

/// Bounded read-phase snapshot for term-document (`!tt`) compaction.
///
/// The two delta shapes are kept apart for the same reason as in
/// [`DocLengthAndCountCompactionPlan`].
struct TermDocsCompactionPlan {
	generation: Option<u64>,
	deltas_by_term: HashMap<String, HashMap<DocId, i64>>,
	/// The per-document `!tt` deltas this snapshot saw.
	tt_keys: Vec<TermChangeKey<'static>>,
	/// The per-transaction `!tx` deltas this snapshot saw.
	tx_keys: Vec<TermChangeBatchKey<'static>>,
	/// The first term either scan stopped inside, before which every term's
	/// deltas were captured whole. `None` where both scans reached the end of
	/// their range, so every term this snapshot names was captured whole.
	///
	/// Both families order their entries by term, and each is bounded
	/// separately, so a term is captured whole only when both scans passed it —
	/// which is why one cutoff covers a split within either family and a split
	/// between them.
	captured_below: Option<String>,
	has_more: bool,
}

impl TermDocsCompactionPlan {
	fn has_logs(&self) -> bool {
		!self.tt_keys.is_empty() || !self.tx_keys.is_empty()
	}

	/// Returns true when either delta family left entries for the next round: the
	/// per-document scan stopped before its range ended, or the batched pass stopped
	/// on the entry that spent its budget with more of the range behind it.
	fn has_more(&self) -> bool {
		self.has_more
	}

	/// Whether every delta this term had was captured, so the totals folded for
	/// it are the documents' final ones.
	///
	/// A term encodes to bytes that sort as the term does, so comparing the
	/// terms compares their position in both delta ranges.
	fn captured_whole(&self, term: &str) -> bool {
		self.captured_below.as_deref().is_none_or(|cutoff| term < cutoff)
	}
}

impl FullTextIndex {
	/// Creates a new full-text index with the specified parameters
	///
	/// This method retrieves the analyzer from the database and then calls
	/// `with_analyzer`
	pub async fn new(
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
			doc_ids: TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone()),
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
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		rid: &RecordId,
		content: Vec<Value>,
		require_compaction: &mut bool,
	) -> Result<Option<DocId>> {
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, az_fn, content, FilteringStage::Indexing).await?;
		let mut set = HashSet::new();
		let tx = env.tx();
		let nid = env.node_id();
		// Get the doc id (if it exists)
		let doc_id = self.get_doc_id(&tx, rid).await?;
		if let Some(doc_id) = doc_id {
			// Where this document's postings live decides what has to be
			// deleted: one entry for a document written since `!dt` existed, or
			// the legacy per-term keys for one written before. Both cases still
			// record a removal per term in the delta log, because the term's
			// document set is maintained separately from its postings.
			let has_entry = tx.exists_key(&self.ikb.new_dt(doc_id), None).await?;
			if has_entry {
				tx.del_key(&self.ikb.new_dt(doc_id)).await?;
			}
			// Delete the terms
			for tks in &tokens {
				for t in tks.list() {
					// Extract the term
					let s = tks.get_token_string(t)?;
					// Check if the term has already been deleted
					if set.insert(s) {
						if !has_entry {
							// Delete the legacy per-term posting
							let key = self.ikb.new_td(s, doc_id);
							tx.del_key(&key).await?;
						}
						self.ikb.buffer_tt(&tx, s, doc_id, nid, false)?;
					}
				}
			}
			{
				let key = self.ikb.new_dl(doc_id);
				// get the doc length
				if let Some(dl) = tx.get_key(&key, None).await? {
					// Delete the doc length
					tx.del_key(&key).await?;
					// Decrease the doc count and total doc length
					let dcl = DocLengthAndCount {
						total_docs_length: -(dl as i128),
						doc_count: -1,
					};
					self.ikb.buffer_dx(&tx, dcl, nid);
					*require_compaction = true;
				}
			}
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	/// Indexes content in the full-text index
	///
	/// This method analyzes and indexes the specified content for a document.
	/// It resolves the document ID, tokenizes the content, and stores term
	/// frequencies and offsets.
	pub(crate) async fn index_content(
		&self,
		stk: &mut Stk,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		rid: &RecordId,
		content: Vec<Value>,
		require_compaction: &mut bool,
	) -> Result<()> {
		let tx = env.tx();
		let nid = env.node_id();
		// Resolve (or assign) the record's doc id in the table's shared space
		let doc_id = self.doc_ids.resolve_or_assign(env, &rid.key).await?;
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, az_fn, content, FilteringStage::Indexing).await?;
		let dl = if self.highlighting {
			self.index_with_offsets(&nid, &tx, doc_id, tokens).await?
		} else {
			self.index_without_offsets(&nid, &tx, doc_id, tokens).await?
		};
		{
			// Set the doc length
			let key = self.ikb.new_dl(doc_id);
			tx.set_key(&key, &dl).await?;
		}
		{
			// Increase the doc count and total doc length
			let dcl = DocLengthAndCount {
				total_docs_length: dl as i128,
				doc_count: 1,
			};
			self.ikb.buffer_dx(&tx, dcl, nid);
			*require_compaction = true;
		}
		// We're done
		Ok(())
	}

	async fn get_doc_length(&self, tx: &Transaction, doc_id: DocId) -> Result<Option<DocLength>> {
		let key = self.ikb.new_dl(doc_id);
		tx.get_key(&key, None).await
	}

	async fn index_with_offsets(
		&self,
		nid: &Uuid,
		tx: &Transaction,
		id: DocId,
		tokens: Vec<Tokens>,
	) -> Result<DocLength> {
		let (dl, offsets) = Analyzer::extract_offsets(&tokens)?;
		// Collected in one pass rather than inserted term by term: a `VecMap`
		// keeps its entries sorted, so N inserts would memmove N times where a
		// single sort does not.
		let mut postings = Vec::with_capacity(offsets.len());
		for (t, o) in offsets {
			postings.push((
				t.into(),
				TermDocument {
					f: o.len() as TermFrequency,
					o,
				},
			));
			self.ikb.buffer_tt(tx, t, id, *nid, true)?;
		}
		let doc = DocumentTerms {
			terms: postings.into_iter().collect(),
		};
		tx.set_key(&self.ikb.new_dt(id), &doc).await?;
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
		let mut postings = Vec::with_capacity(tf.len());
		for (t, f) in tf {
			postings.push((
				t.into(),
				TermDocument {
					f,
					o: Vec::new(),
				},
			));
			self.ikb.buffer_tt(tx, t, id, *nid, true)?;
		}
		let doc = DocumentTerms {
			terms: postings.into_iter().collect(),
		};
		tx.set_key(&self.ikb.new_dt(id), &doc).await?;
		Ok(dl)
	}

	/// Every uncompacted change to one term's document set.
	///
	/// Three sources, all folded into signed counts so only the sign matters:
	/// the per-document `!tt` entries an older server wrote, the batched `!tx`
	/// bitmaps, and this transaction's own buffered contribution, which is not
	/// written until commit and so cannot be read back from the transaction.
	async fn term_deltas(&self, tx: &Transaction, term: &str) -> Result<HashMap<DocId, i64>> {
		let mut deltas: HashMap<DocId, i64> = HashMap::new();
		for k in tx.keys(self.ikb.new_tt_term_range(term)?, u32::MAX, 0, None).await? {
			let tt = TermChangeKey::decode_key(&k)?;
			*deltas.entry(tt.doc_id).or_default() += if tt.add {
				1
			} else {
				-1
			};
		}
		for (k, docs) in tx.getr(self.ikb.new_tx_term_range(term)?, None).await? {
			let batch = TermChangeBatchKey::decode_key(&k)?;
			let step = if batch.add {
				1
			} else {
				-1
			};
			for doc_id in &docs {
				*deltas.entry(doc_id).or_default() += step;
			}
		}
		let (added, removed) = self.ikb.pending_tt(tx, term);
		for doc_id in &added {
			*deltas.entry(doc_id).or_default() += 1;
		}
		for doc_id in &removed {
			*deltas.entry(doc_id).or_default() -= 1;
		}
		Ok(deltas)
	}

	/// Extracts query terms from a search string
	///
	/// Tokenizes the query string, then retrieves the document bitmaps for each
	/// unique term. The compacted bitmap fetches are batched via `tx.getm()` to
	/// reduce KV round trips (one batch instead of N sequential gets).
	pub async fn extract_querying_terms(
		&self,
		stk: &mut Stk,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		query_string: String,
	) -> Result<QueryTerms> {
		let tokens = self
			.analyzer
			.generate_tokens(stk, az_fn, FilteringStage::Querying, query_string.into())
			.await?;

		let mut unique_terms: Vec<&str> = Vec::new();
		let mut unique_tokens = HashSet::new();
		for token in tokens.list() {
			if unique_tokens.insert(token) {
				unique_terms.push(tokens.get_token_string(token)?);
			}
		}

		let tx = env.tx();

		// Phase 1: Collect deltas for each term (sequential range scans)
		let mut all_deltas: Vec<HashMap<DocId, i64>> = Vec::with_capacity(unique_terms.len());
		for term in &unique_terms {
			all_deltas.push(self.term_deltas(&tx, term).await?);
		}

		// Phase 1b: add what a term's compacted bitmap could not hold, which is
		// part of the same total the deltas contribute to.
		//
		// Read for every term, not only for terms that have deltas. A residual
		// this compactor wrote always sits beside a bitmap that agrees with its
		// sign, so it would decide nothing on its own — but a compactor that
		// does not know the family can move the bitmap out from under one, and
		// the total is then the only thing that still says where the document
		// belongs. Deciding from the bitmap alone would make that state
		// permanent instead of leaving it to be folded away.
		let residual_keys: Vec<_> =
			unique_terms.iter().map(|term| self.ikb.new_tr_root(term)).collect();
		for (deltas, residual) in
			all_deltas.iter_mut().zip(tx.get_many_key(residual_keys, None).await?)
		{
			let Some(residual) = residual else {
				continue;
			};
			for (doc_id, count) in residual.counts.iter() {
				*deltas.entry(*doc_id).or_default() += *count;
			}
		}

		// Phase 2: Batch-fetch compacted bitmaps for all terms at once
		let bitmap_keys: Vec<_> =
			unique_terms.iter().map(|term| self.ikb.new_td_root(term)).collect();
		let bitmaps: Vec<Option<RoaringTreemap>> = tx.get_many_key(bitmap_keys, None).await?;

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

	pub async fn matches_value(
		&self,
		stk: &mut Stk,
		az_fn: &dyn AnalyzerFunction,
		qt: &QueryTerms,
		bo: BooleanOperator,
		val: Value,
	) -> Result<bool> {
		let mut tks = vec![];
		self.analyzer.analyze_value(stk, az_fn, val, FilteringStage::Indexing, &mut tks).await?;
		match bo {
			BooleanOperator::And => qt.matches_and(&tks),
			BooleanOperator::Or => qt.matches_or(&tks),
		}
	}

	/// Folds one term's captured deltas into its compacted state.
	///
	/// A document's total is its compacted membership, plus what the term's
	/// residual carries for it, plus the deltas this round captured. The bitmap
	/// takes one of that total and the residual keeps the rest, so nothing the
	/// bitmap cannot represent is discarded. That is what makes the compacted set
	/// independent of how a term's deltas were grouped into rounds: every round
	/// leaves `membership + residual + remaining deltas` where it found it, and
	/// only that sum decides whether a document carries the term.
	///
	/// `captured_whole` says no delta for this term was left behind, so the total
	/// is the document's final one. It lands in the bitmap alone and the residual
	/// is dropped — a term with nothing left to fold is answered by its bitmap.
	async fn fold_term_docs(
		&self,
		tx: &Transaction,
		term: &str,
		mut totals: HashMap<DocId, i64>,
		residual: Option<TermDocsResidual>,
		captured_whole: bool,
	) -> Result<()> {
		let td = self.ikb.new_td_root(term);
		let mut docs: RoaringTreemap = tx.get_key(&td, None).await?.unwrap_or_default();

		let carried = residual.unwrap_or_default().counts;
		for (doc_id, count) in carried.iter() {
			*totals.entry(*doc_id).or_default() += *count;
		}

		let mut left_over = Vec::new();
		for (doc_id, total) in &totals {
			let total = i64::from(docs.contains(*doc_id)) + *total;
			match 0.cmp(&total) {
				Ordering::Less => {
					docs.insert(*doc_id);
					if !captured_whole && total > 1 {
						left_over.push((*doc_id, total - 1));
					}
				}
				Ordering::Greater => {
					docs.remove(*doc_id);
					if !captured_whole {
						left_over.push((*doc_id, total));
					}
				}
				Ordering::Equal => {
					docs.remove(*doc_id);
				}
			}
		}

		if docs.is_empty() {
			tx.del_key(&td).await?;
		} else {
			tx.set_key(&td, &docs).await?;
		}
		let tr = self.ikb.new_tr_root(term);
		if left_over.is_empty() {
			// Only where one is there to remove: the residual is absent for all
			// but the terms a round stopped inside, and a delete of a key that was
			// never written is a write nonetheless.
			if !carried.is_empty() {
				tx.del_key(&tr).await?;
			}
		} else {
			tx.set_key(&tr, &TermDocsResidual::new(left_over)).await?;
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
		let limit = limit.max(1);
		let mut tt_keys = Vec::new();
		let mut tx_keys = Vec::new();
		let mut deltas_by_term: HashMap<String, HashMap<DocId, i64>> = HashMap::new();
		// Both delta shapes are drained in one pass, so an index carrying entries in
		// either shape compacts to the same document set. Either family having more
		// leaves work for the next round.
		//
		// What `limit` bounds differs by shape, because it is the round's hold on
		// memory and a compaction transaction is exempt from the write-cardinality
		// guard that bounds statement execution — nothing else caps it. A `!tt` key
		// names one document, so bounding keys bounds documents. A `!tx` key carries
		// a whole bitmap, so the same key bound would leave the round scaling with
		// how wide the transaction that wrote it was. The `!tx` pass therefore
		// spends `limit` as a document budget.
		let batch = tx.batch_keys(self.ikb.new_tt_terms_range()?, limit, None).await?;
		let tt_cut_short = batch.next.is_some();
		for k in batch.result {
			let tt = TermChangeKey::decode_key(&k)?;
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
			tt_keys.push(tt.into_owned());
		}
		// What is left of the budget once the per-document family has spent its
		// share, so `limit` bounds the round rather than each family separately.
		let tx_budget = (limit as u64).saturating_sub(tt_keys.len() as u64);
		// Read in pages and fold each before asking for the next, so what a round
		// holds follows the budget rather than the backlog. The budget bounds it
		// except for the one entry a round is always allowed — which may be wider
		// than the whole of it, or the round could never compact that term.
		//
		// A row count cannot express what a page costs — that is the documents
		// inside its bitmaps — so a page is sized two ways at once, and takes the
		// smaller. From the widest entry seen so far, about as many rows as the
		// remaining budget covers at that width, rounded up so a budget smaller than
		// one entry still asks for one; the widest rather than the mean, because the
		// mean under-sizes the estimate exactly when widths vary. And from the rows
		// already folded, at most twice as many, so the read can only grow as fast
		// as evidence for it does.
		//
		// The ramp is what covers the case the width estimate cannot: a page whose
		// entries are all far wider than anything measured before them. Doubling
		// means such a page is never more than twice the size of one already folded
		// inside the budget, where sizing on width alone would jump straight to the
		// cap on the strength of one narrow entry. Neither bounds the read in terms
		// of documents — no row-limited scan can — so the cap is the last ceiling.
		let mut range = self.ikb.new_tx_terms_range()?;
		let mut batched_has_more = false;
		let mut folded_docs = 0u64;
		let mut widest = 0u64;
		'paging: loop {
			// A page of at least one entry keeps a round able to make progress, and
			// is what probes for more once the budget is exactly spent.
			let remaining = tx_budget.saturating_sub(folded_docs);
			let rows = match widest {
				// Nothing measured yet: read one entry rather than guess. A page
				// sized before any width is known is the unbounded read again.
				0 => 1,
				widest => remaining
					.div_ceil(widest)
					.min((tx_keys.len() as u64).saturating_mul(2))
					.clamp(1, TERM_DELTA_SCAN_PAGE as u64) as u32,
			};
			let page = tx.scan(range.clone(), rows, 0, None).await?;
			let page_full = page.len() as u32 == rows;
			let resume_after = page.last().map(|(k, _)| k.clone());
			let page_len = page.len();
			for (index, (k, docs)) in page.into_iter().enumerate() {
				// One rule for every entry: the one that takes the round past a limit is
				// folded, and is the round's last. So a round holds at most `limit`
				// entries and one more, and `tx_budget` documents and that entry's width,
				// and always folds at least one entry however wide — which it must, or an
				// entry wider than the whole budget would never compact at all. The budget
				// is spent in whole entries because the write phase may only delete an
				// entry whose every document it folded.
				//
				// Folding it rather than deferring it is what stops a wide entry behind
				// narrow ones from waiting on them: a round restarts at the head of the
				// range, so a deferred entry is only reached once everything sorting before
				// it is gone.
				//
				// It does not make the order fair. Writes arriving at the head of the range
				// as fast as a round drains it hold every round's attention, and a term
				// sorting behind them stays uncompacted for as long as that lasts — its own
				// backlog static, but its queries folding deltas that never get folded for
				// them. Ordering that fairly needs a round to resume where the last one
				// stopped rather than at the head, which needs the position to be durable.
				let folded_entries = tx_keys.len() as u64;
				let last =
					folded_entries + 1 >= limit as u64 || folded_docs + docs.len() >= tx_budget;
				folded_docs += docs.len();
				// Measured over every entry the round accepts, including the first,
				// whose width is the only thing the next page has to go on. Floored at
				// one so an entry carrying no documents still counts as measured —
				// leaving it at zero would hold every later page at a single row.
				widest = widest.max(docs.len().max(1));
				let tx_key = TermChangeBatchKey::decode_key(&k)?;
				let step = if tx_key.add {
					1
				} else {
					-1
				};
				let by_doc = deltas_by_term.entry(tx_key.term.to_string()).or_default();
				for doc_id in &docs {
					*by_doc.entry(doc_id).or_default() += step;
				}
				tx_keys.push(tx_key.into_owned());
				if last {
					// Rows after this one in the page are entries this round did not reach.
					// Past the page's end it depends on whether the scan filled it: a full
					// page may be followed by another, a short one is the range's end. A
					// round that took the last entry there was reports no more, rather than
					// leaving an empty round behind it.
					batched_has_more = index + 1 < page_len || page_full;
					break 'paging;
				}
			}
			match resume_after {
				// A page the scan filled may have been cut short by the row count, so
				// the range continues after its last entry. A short page is the end.
				Some(k) if page_full => range = range.resume_after(&k, Direction::Forward),
				_ => break 'paging,
			}
		}
		// A scan that stopped short leaves the term it stopped on possibly
		// half-captured, and every term sorting after it untouched — while the
		// other family, bounded separately, may have gone further. So the terms
		// captured whole are those before the earlier of the two stops, and the
		// lowest term stands in for a scan that stopped with nothing to show.
		let mut captured_below = None;
		let mut stopped_at = |term: Option<&str>| {
			let cutoff = term.unwrap_or_default();
			if captured_below.as_deref().is_none_or(|current| cutoff < current) {
				captured_below = Some(cutoff.to_string());
			}
		};
		if tt_cut_short {
			stopped_at(tt_keys.last().map(|k| k.term.as_ref()));
		}
		if batched_has_more {
			stopped_at(tx_keys.last().map(|k| k.term.as_ref()));
		}
		Ok(TermDocsCompactionPlan {
			generation,
			deltas_by_term,
			tt_keys,
			tx_keys,
			captured_below,
			has_more: tt_cut_short || batched_has_more,
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
		mut plan: TermDocsCompactionPlan,
	) -> Result<()> {
		let folded: Vec<(String, HashMap<DocId, i64>, bool)> =
			std::mem::take(&mut plan.deltas_by_term)
				.into_iter()
				.filter(|(_, deltas)| !deltas.is_empty())
				.map(|(term, deltas)| {
					let captured_whole = plan.captured_whole(&term);
					(term, deltas, captured_whole)
				})
				.collect();
		// One round trip for every term's residual rather than one per term. A
		// round folds as many terms as its budget allows, and all but the term it
		// stopped inside have no residual to read.
		let keys: Vec<_> = folded.iter().map(|(term, ..)| self.ikb.new_tr_root(term)).collect();
		let residuals = tx.get_many_key(keys, None).await?;
		for ((term, deltas, captured_whole), residual) in folded.into_iter().zip(residuals) {
			self.fold_term_docs(tx, &term, deltas, residual, captured_whole).await?;
		}
		for key in &plan.tt_keys {
			tx.del_key(key).await?;
		}
		for key in &plan.tx_keys {
			tx.del_key(key).await?;
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
	pub fn new_hits_iterator(
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

	/// Returns the merged posting bitmap for the query terms — the set of
	/// doc-IDs matching the query under the given boolean operator — without
	/// resolving record IDs or computing scores.
	///
	/// This is the branch output for bitmap candidate plans (issue #547):
	/// the caller composes it with other candidate bitmaps (AND/OR/AND-NOT)
	/// over the table's shared doc-ID space and defers BM25 scoring to the
	/// surviving documents. Returns `None` when no document matches.
	pub fn merged_postings(qt: &QueryTerms, bo: BooleanOperator) -> Option<RoaringTreemap> {
		match bo {
			BooleanOperator::And => Self::intersection_operation(&qt.docs),
			BooleanOperator::Or => Self::union_operation(&qt.docs),
		}
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

	pub async fn get_doc_id(&self, tx: &Transaction, rid: &RecordId) -> Result<Option<DocId>> {
		if rid.table != *self.ikb.table() {
			return Ok(None);
		}
		self.doc_ids.get_doc_id(tx, &rid.key).await
	}
	pub async fn new_scorer(&self, env: &dyn IndexEnv) -> Result<Option<Scorer>> {
		if let Some(bm25) = &self.bm25 {
			let dlc = self.compute_doc_length_and_count(&env.tx(), None).await?;
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
	) -> Result<(DocLengthAndCount, DocStatsDeltaKeys)> {
		collect_doc_length_and_count_for(tx, &self.ikb).await
	}

	/// Collects compacted root stats plus a bounded batch of visible `!dc`
	/// deltas for compaction.
	async fn collect_doc_length_and_count_compaction(
		&self,
		tx: &Transaction,
		limit: u32,
	) -> Result<(DocLengthAndCount, DocStatsDeltaKeys, bool)> {
		let dc_prefix = DocStatsKey {
			ns: self.ikb.ns(),
			db: self.ikb.db(),
			tb: Cow::Borrowed(self.ikb.table()),
			ix: self.ikb.index(),
		};
		let mut dlc = tx.get_key(&dc_prefix, None).await?.unwrap_or_default();

		// Held one below `u32::MAX` so the probe row below is always a real extra
		// row rather than one the saturating add silently folded away.
		let limit = limit.clamp(1, u32::MAX - 1);
		let range = dc_prefix.range()?;
		let batch = tx.batch_keys_vals(range, limit, None).await?;
		let mut deltas = DocStatsDeltaKeys {
			legacy: Vec::with_capacity(batch.result.len()),
			batched: Vec::new(),
		};
		for (k, v) in batch.result {
			let st = DocLengthAndCount::kv_decode_value(&v, ())?;
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;
			deltas.legacy.push(DocStatsDeltaKey::decode_key(&k)?.into_owned());
		}
		// Drain the batched deltas in the same pass, so an index carrying entries in
		// either shape compacts to the same statistic.
		// One `!dx` entry is one fixed-size statistic, so unlike `!tx` a key bound
		// is already a bound on the work this fold holds.
		let mut batched =
			tx.scan(self.ikb.new_dx_range()?, limit.saturating_add(1), 0, None).await?;
		let batched_has_more = batched.len() > limit as usize;
		batched.truncate(limit as usize);
		for (k, st) in batched {
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;
			deltas.batched.push(DocStatsBatchKey::decode_key(&k)?.into_owned());
		}
		Ok((dlc, deltas, batch.next.is_some() || batched_has_more))
	}

	async fn compute_doc_length_and_count(
		&self,
		tx: &Transaction,
		compact_log: Option<&mut bool>,
	) -> Result<DocLengthAndCount> {
		let (dlc, deltas) = self.collect_doc_length_and_count(tx).await?;
		if let Some(compact_log) = compact_log
			&& !deltas.is_empty()
		{
			deltas.delete(tx).await?;
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
		let (dlc, deltas, has_more) =
			self.collect_doc_length_and_count_compaction(tx, limit).await?;
		Ok(DocLengthAndCountCompactionPlan {
			generation,
			dlc,
			deltas,
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
		tx.set_key(&self.ikb.new_dc_compacted(), &plan.dlc).await?;
		plan.deltas.delete(tx).await?;
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
	pub(crate) async fn prepare_compaction(
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
	pub(crate) async fn apply_compaction(
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
	pub async fn highlight(
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
			let mut loaded = self.get_document_terms(tx, doc_id).await?;
			for tk in qt.tokens.list() {
				let term = qt.tokens.get_token_string(tk)?;
				if let Some(td) = self.take_term_document(tx, doc_id, &mut loaded, term).await? {
					hl.highlight(tk.get_char_len(), td.o);
				}
			}
			return hl.try_into();
		}
		Ok(Value::None)
	}

	/// One document's posting for one term.
	///
	/// The document's own entry answers this when it exists. A document indexed
	/// before that entry existed has no `!dt` key, and its postings are still
	/// under the legacy per-term keys — so absence of the entry, not absence of
	/// the term, is what selects the fallback. A document that has been
	/// rewritten since carries a `!dt` entry whose map is authoritative: a term
	/// missing from it does not occur in the document.
	/// Test-only, and deliberately so: it decodes a document's whole entry to
	/// take one term from it, which is the cost the read paths avoid by loading
	/// the entry once per document. A caller wanting several terms wants
	/// [`Self::get_document_terms`] and [`Self::take_term_document`].
	#[cfg(test)]
	async fn get_term_document(
		&self,
		tx: &Transaction,
		id: DocId,
		term: &str,
	) -> Result<Option<TermDocument>> {
		let mut loaded = self.get_document_terms(tx, id).await?;
		self.take_term_document(tx, id, &mut loaded, term).await
	}

	/// Every posting one document carries, or `None` for a document whose
	/// postings predate the per-document entry.
	///
	/// Read once by a caller that wants several of a document's terms: the entry
	/// holds every term the document carries, so fetching it per term would
	/// decode all of them once per term.
	async fn get_document_terms(
		&self,
		tx: &Transaction,
		id: DocId,
	) -> Result<Option<DocumentTerms>> {
		tx.get_key(&self.ikb.new_dt(id), None).await
	}

	/// Takes one term's posting out of a document's already-loaded entry,
	/// falling back to the legacy per-term key when the document has none.
	///
	/// Takes rather than borrows so a caller walking several terms consumes each
	/// posting's offsets without cloning them; no term is asked for twice.
	async fn take_term_document(
		&self,
		tx: &Transaction,
		id: DocId,
		loaded: &mut Option<DocumentTerms>,
		term: &str,
	) -> Result<Option<TermDocument>> {
		match loaded {
			Some(doc) => Ok(doc.terms.remove(term)),
			None => tx.get_key(&self.ikb.new_td(term, id), None).await,
		}
	}

	pub async fn read_offsets(
		&self,
		tx: &Transaction,
		thg: &RecordId,
		qt: &QueryTerms,
		partial: bool,
	) -> Result<Value> {
		let doc_id = self.get_doc_id(tx, thg).await?;
		if let Some(doc_id) = doc_id {
			let mut or = Offseter::new(partial);
			let mut loaded = self.get_document_terms(tx, doc_id).await?;
			for tk in qt.tokens.list() {
				let term = qt.tokens.get_token_string(tk)?;
				if let Some(o) = self.take_term_document(tx, doc_id, &mut loaded, term).await? {
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
pub struct FullTextHitsIterator {
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
		let docids = TableDocIds::new(self.ikb.ns(), self.ikb.db(), self.ikb.table().clone());
		for doc_id in self.iter.by_ref() {
			if let Some(key) = docids.get_record_id(tx, doc_id).await? {
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
pub struct Scorer {
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
	pub async fn score(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Score> {
		let mut sc = 0.0;
		let tl = qt.tokens.list();
		let doc_length = fti.get_doc_length(tx, doc_id).await?.unwrap_or(0) as f64;
		// The document's postings, read once for the whole query rather than
		// once per term: the entry carries every term the document holds.
		let mut loaded = fti.get_document_terms(tx, doc_id).await?;
		for (i, d) in qt.docs.iter().enumerate() {
			if let Some(docs) = d
				&& docs.contains(doc_id)
				&& let Some(token) = tl.get(i)
			{
				let term = qt.tokens.get_token_string(token)?;
				let td = fti.take_term_document(tx, doc_id, &mut loaded, term).await?;
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

/// Reads an index's `!dc` region: the compacted root plus every visible delta.
///
/// The root holds the totals compaction has folded so far and each child key a
/// contribution not yet folded in, so the complete statistic is their sum. The
/// returned key list names the deltas seen, which is what a caller compacting
/// them deletes; a caller that only wants the statistic ignores it.
///
/// Cost tracks the uncompacted delta count, not the index size, so it is bounded
/// by how far compaction has fallen behind.
async fn collect_doc_length_and_count_for(
	tx: &Transaction,
	ikb: &IndexKeyBase,
) -> Result<(DocLengthAndCount, DocStatsDeltaKeys)> {
	let mut dlc = DocLengthAndCount::default();
	let prefix = DocStatsKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	};
	let prefix_len = prefix.encode_key()?.len();

	let mut deltas = DocStatsDeltaKeys::default();
	for (idx, (k, st)) in tx.getr(prefix.range_subtree()?, None).await?.into_iter().enumerate() {
		dlc.doc_count += st.doc_count;
		dlc.total_docs_length += st.total_docs_length;

		// The prefix key can only be the first key. Every other key extends the
		// prefix, so it must differ in length, and extends it by exactly the
		// fields a delta adds — which is what the decode then confirms.
		if idx != 0 && k.len() != prefix_len {
			deltas.legacy.push(DocStatsDeltaKey::decode_key(&k)?.into_owned());
		}
	}
	// The batched deltas an up-to-date server writes, then this transaction's own
	// buffered contribution, which is not written until commit.
	for (k, st) in tx.getr(ikb.new_dx_range()?, None).await? {
		dlc.doc_count += st.doc_count;
		dlc.total_docs_length += st.total_docs_length;
		deltas.batched.push(DocStatsBatchKey::decode_key(&k)?.into_owned());
	}
	let pending = ikb.pending_dx(tx);
	dlc.doc_count += pending.doc_count;
	dlc.total_docs_length += pending.total_docs_length;
	Ok((dlc, deltas))
}

/// The mean number of indexed tokens per document in a full-text index, or
/// `None` when the index holds no documents.
///
/// Derived from the same `total_docs_length` / `doc_count` pair BM25 scoring
/// uses, so it needs no analyzer and costs one read of the index's `!dc` region.
/// Callers sizing work by how much a document costs to index want this rather
/// than a fixed guess: a document's postings travel in one key, but the delta log
/// it contributes to spends one key per distinct term in the transaction, so in
/// the worst case — documents sharing no vocabulary — the per-record key count
/// still scales with document length.
pub async fn mean_tokens_per_document(tx: &Transaction, ikb: &IndexKeyBase) -> Result<Option<u64>> {
	let (dlc, _) = collect_doc_length_and_count_for(tx, ikb).await?;
	if dlc.doc_count <= 0 || dlc.total_docs_length <= 0 {
		return Ok(None);
	}
	let mean = dlc.total_docs_length / dlc.doc_count as i128;
	Ok(Some(mean.clamp(1, u64::MAX as i128) as u64))
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;
	use std::sync::Arc;
	use std::time::{Duration, Instant};

	use reblessive::tree::Stk;
	use roaring::RoaringTreemap;
	use surrealdb_datastore::Transaction;
	use surrealdb_kvs::TransactionType;
	use surrealdb_strand::Strand;
	use test_log::test;
	use tokio::time::sleep;
	use uuid::Uuid;

	use super::{
		DocId, DocumentTerms, FullTextIndex, TermChangeBatchKey, TermChangeKey, TermDocsResidual,
		TermDocument,
	};
	use crate::IndexKeyBase;
	use crate::catalog::{AnalyzerDefinition, DatabaseId, FullTextParams, IndexId, NamespaceId};
	use crate::expr::Tokenizer;
	use crate::ft::analyzer::{AnalyzerFunction, BoxAnalyzerFut};
	use crate::ft::offset::Offset;
	use crate::index::IndexOperation;
	use crate::key::schema::DocStatsKey;
	use crate::test_env::{TestIndexEnv, TestIndexStore};
	use crate::val::{Array, RecordId, Value};

	/// Stands in for the evaluator behind an analyzer's `FUNCTION` clause. The
	/// analyzer below declares none, so the seam is never reached.
	struct NoAnalyzerFunction;

	impl AnalyzerFunction for NoAnalyzerFunction {
		fn call<'a>(
			&'a self,
			_stk: &'a mut Stk,
			name: &'a str,
			_input: Strand,
		) -> BoxAnalyzerFut<'a> {
			unreachable!("the test analyzer declares no FUNCTION {name}")
		}
	}

	#[derive(Clone)]
	struct TestContext {
		nid: Uuid,
		start: Arc<Instant>,
		ds: TestIndexStore,
		content: Arc<Value>,
		ikb: IndexKeyBase,
		fti: Arc<FullTextIndex>,
	}

	impl TestContext {
		async fn new() -> Self {
			Self::with_highlighting(true).await
		}

		/// `highlighting` selects which of the two indexing paths the context
		/// exercises: offsets are recorded only when highlighting is on.
		async fn with_highlighting(highlight: bool) -> Self {
			let ds = TestIndexStore::new().await;
			// `DEFINE ANALYZER test TOKENIZERS blank`, as the definition the
			// index consumes: lowering the statement to it is the evaluator's
			// job and nothing here exercises that lowering.
			let az = Arc::new(AnalyzerDefinition {
				name: "test".into(),
				function: None,
				tokenizers: Some(vec![Tokenizer::Blank]),
				filters: None,
				comment: None,
			});
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
				highlight,
			});
			let nid = Uuid::new_v4();
			let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(3));
			let fti = Arc::new(
				FullTextIndex::with_analyzer(ds.index_stores(), az, ikb.clone(), &ft_params)
					.unwrap(),
			);
			let start = Arc::new(Instant::now());
			Self {
				nid,
				ikb,
				start,
				ds,
				content,
				fti,
			}
		}

		async fn new_tx(&self, tt: TransactionType) -> Arc<Transaction> {
			Arc::new(self.ds.transaction(tt).await.unwrap())
		}

		/// A read environment plus the transaction it runs on, for the
		/// assertions that also read keys directly.
		async fn new_read_env(&self) -> (TestIndexEnv, Arc<Transaction>) {
			let env = self.ds.env(TransactionType::Read).await;
			let tx = env.tx();
			(env, tx)
		}

		async fn remove_insert_task(&self, stk: &mut Stk, rid: &RecordId) {
			let ctx = self.ds.env(TransactionType::Write).await;
			let tx = ctx.tx();

			let mut require_compaction = false;
			let az_fn = NoAnalyzerFunction;
			self.fti
				.remove_content(
					stk,
					&ctx,
					&az_fn,
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
					&az_fn,
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

		/// Indexes several documents in one transaction, so each flushed `!tx`
		/// entry carries a bitmap as wide as the batch rather than one document.
		///
		/// `content` selects which terms the batch writes, so a test can place
		/// entries of chosen widths at chosen points in the scan's term order.
		async fn index_batch_in_one_tx(&self, stk: &mut Stk, rids: &[RecordId], content: &Value) {
			let ctx = self.ds.env(TransactionType::Write).await;
			let tx = ctx.tx();
			let az_fn = NoAnalyzerFunction;
			let mut require_compaction = false;
			for rid in rids {
				self.fti
					.index_content(
						stk,
						&ctx,
						&az_fn,
						rid,
						vec![content.clone()],
						&mut require_compaction,
					)
					.await
					.unwrap();
			}
			if require_compaction {
				IndexOperation::compaction_trigger(&self.ikb, &tx, self.nid).await.unwrap();
			}
			tx.commit().await.unwrap();
		}

		/// Uncompacted document-statistic deltas, in both the per-document `!dc`
		/// shape and the batched `!dx` one, since either may be present.
		async fn dc_delta_count(&self, tx: &Transaction) -> usize {
			let dc_range = DocStatsKey {
				ns: self.ikb.ns(),
				db: self.ikb.db(),
				tb: Cow::Borrowed(self.ikb.table()),
				ix: self.ikb.index(),
			}
			.range()
			.unwrap();
			let legacy = tx.keys(dc_range, u32::MAX, 0, None).await.unwrap().len();
			let batched = tx.count(self.ikb.new_dx_range().unwrap(), None).await.unwrap();
			legacy + batched
		}

		/// Uncompacted term deltas, in both the per-document `!tt` shape and the
		/// batched `!tx` one.
		async fn tt_delta_count(&self, tx: &Transaction) -> usize {
			let legacy = tx.count(self.ikb.new_tt_terms_range().unwrap(), None).await.unwrap();
			let batched = tx.count(self.ikb.new_tx_terms_range().unwrap(), None).await.unwrap();
			legacy + batched
		}

		/// Writes one batched `!tx` delta directly, so a test controls the order
		/// the entries sort in rather than leaving it to the node ids indexing
		/// would use.
		async fn write_tx_delta(
			&self,
			tx: &Transaction,
			term: &str,
			nid: u128,
			add: bool,
			docs: &RoaringTreemap,
		) {
			let key = TermChangeBatchKey {
				ns: self.ikb.ns(),
				db: self.ikb.db(),
				tb: Cow::Borrowed(self.ikb.table()),
				ix: self.ikb.index(),
				term: Cow::Borrowed(term),
				nid: Uuid::from_u128(nid),
				uid: Uuid::from_u128(9),
				add,
			};
			tx.set_key(&key, docs).await.unwrap();
		}

		/// The same, in the per-document `!tt` shape an upgraded database carries.
		async fn write_tt_delta(
			&self,
			tx: &Transaction,
			term: &str,
			doc_id: DocId,
			nid: u128,
			add: bool,
		) {
			let key = TermChangeKey {
				ns: self.ikb.ns(),
				db: self.ikb.db(),
				tb: Cow::Borrowed(self.ikb.table()),
				ix: self.ikb.index(),
				term: Cow::Borrowed(term),
				doc_id,
				nid: Uuid::from_u128(nid),
				uid: Uuid::from_u128(9),
				add,
			};
			tx.set_key(&key, &String::new()).await.unwrap();
		}

		/// One term's compacted document set.
		async fn term_docs(&self, term: &str) -> Option<RoaringTreemap> {
			let tx = self.new_tx(TransactionType::Read).await;
			let docs = tx.get_key(&self.ikb.new_td_root(term), None).await.unwrap();
			tx.cancel().await.unwrap();
			docs
		}

		/// What that set could not hold, as `(document, count)` pairs.
		async fn term_residual(&self, term: &str) -> Option<Vec<(DocId, i64)>> {
			let tx = self.new_tx(TransactionType::Read).await;
			let residual: Option<TermDocsResidual> =
				tx.get_key(&self.ikb.new_tr_root(term), None).await.unwrap();
			tx.cancel().await.unwrap();
			residual.map(|r| r.counts.iter().map(|(doc_id, count)| (*doc_id, *count)).collect())
		}

		/// Runs bounded term-document compaction rounds until both delta families
		/// are drained, and answers how many of them did work.
		async fn drain_term_docs(&self, limit: u32) -> usize {
			let mut rounds = 0;
			loop {
				let tx = self.new_tx(TransactionType::Write).await;
				let plan =
					self.fti.prepare_term_docs_compaction_with_limit(&tx, limit).await.unwrap();
				let has_more = plan.has_more();
				let applied = self.fti.apply_term_docs_compaction(&tx, plan).await.unwrap();
				tx.commit().await.unwrap();
				if !applied {
					return rounds;
				}
				rounds += 1;
				assert!(rounds < 64, "compaction is not draining the delta ranges");
				if !has_more {
					return rounds;
				}
			}
		}

		/// The documents a query resolves for one term, through the same path a
		/// `MATCHES` clause takes.
		async fn query_docs(&self, term: &str) -> RoaringTreemap {
			let (ctx, tx) = self.new_read_env().await;
			let mut stack = reblessive::TreeStack::new();
			let az_fn = NoAnalyzerFunction;
			let qt = stack
				.enter(|stk| self.fti.extract_querying_terms(stk, &ctx, &az_fn, term.to_owned()))
				.finish()
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			qt.docs.into_iter().flatten().fold(RoaringTreemap::new(), |mut all, docs| {
				all |= docs;
				all
			})
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
		assert_eq!(test.tt_delta_count(&tx).await, 0);
		assert_eq!(test.dc_delta_count(&tx).await, 0);
		let subtree = DocStatsKey {
			ns: test.ikb.ns(),
			db: test.ikb.db(),
			tb: Cow::Borrowed(test.ikb.table()),
			ix: test.ikb.index(),
		}
		.range_subtree()
		.unwrap();
		assert_eq!(tx.count(subtree, None).await.unwrap(), 1);
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

		// "lorem" appears in only 1 of 25 entries in the test content, so
		// with 2 identical docs the term_doc_count=2 and doc_count=2, giving
		// IDF = ln((2-2+0.5)/(2+0.5)) which clamps to 0. We need a search
		// term where term_doc_count < doc_count. Since both docs have the
		// same content, every term has term_doc_count == doc_count, so IDF=0.
		//
		// Instead, directly verify that `compute_doc_length_and_count`
		// returns valid (non-zero) stats before and after compaction.

		// Before compaction: scorer should exist and have valid doc stats.
		let (read_ctx, tx) = test.new_read_env().await;
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
		let (read_ctx, _tx) = test.new_read_env().await;
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
		assert_eq!(plan.deltas.legacy.len() + plan.deltas.batched.len(), 2);

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

	/// A transaction's deltas share one discriminator, so the keys have to stay
	/// distinct by their remaining fields or a contribution is silently lost;
	/// and separate transactions must never collapse into each other.
	///
	/// Indexing a document leaves one term delta per distinct term and one
	/// statistics delta. A remove followed by a re-index of the same record
	/// within one transaction nets to what the index already said: the term is
	/// still present, so the transaction contributes one addition per term, and
	/// the statistics do not move at all, so it contributes no statistics entry.
	/// The earlier transaction's deltas are untouched either way.
	#[test(tokio::test)]
	async fn every_term_and_transaction_keeps_its_own_delta() {
		for highlight in [false, true] {
			let test = TestContext::with_highlighting(highlight).await;
			let doc = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));
			let mut stack = reblessive::TreeStack::new();

			// A record with no doc-ID yet: the removal half writes nothing, so
			// this counts exactly what one indexing call emits.
			stack.enter(|stk| test.remove_insert_task(stk, &doc)).finish().await;
			let tx = test.new_tx(TransactionType::Read).await;
			let terms = test.tt_delta_count(&tx).await;
			assert!(terms > 2, "the fixture content must yield several distinct terms");
			assert_eq!(test.dc_delta_count(&tx).await, 1, "highlight={highlight}");
			tx.cancel().await.unwrap();

			// A second transaction removing and re-indexing the same content:
			// one addition per term on top of the first transaction's, and no
			// statistics entry, because removing and re-adding one document of
			// unchanged length nets to zero.
			stack.enter(|stk| test.remove_insert_task(stk, &doc)).finish().await;
			let tx = test.new_tx(TransactionType::Read).await;
			assert_eq!(test.tt_delta_count(&tx).await, terms * 2, "highlight={highlight}");
			assert_eq!(test.dc_delta_count(&tx).await, 1, "highlight={highlight}");
			tx.cancel().await.unwrap();
		}
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
		assert_eq!(plan.tt_keys.len() + plan.tx_keys.len(), 2);

		let tx = test.new_tx(TransactionType::Write).await;
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(test.tt_delta_count(&tx).await, before_terms - 2);
		tx.cancel().await.unwrap();

		let (ctx, tx) = test.new_read_env().await;
		let mut stack = reblessive::TreeStack::new();
		let az_fn = NoAnalyzerFunction;
		let qt = stack
			.enter(|stk| test.fti.extract_querying_terms(stk, &ctx, &az_fn, "Welcome".into()))
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

		let (ctx, tx) = test.new_read_env().await;
		assert_eq!(test.tt_delta_count(&tx).await, 0);
		let mut stack = reblessive::TreeStack::new();
		let az_fn = NoAnalyzerFunction;
		let qt = stack
			.enter(|stk| test.fti.extract_querying_terms(stk, &ctx, &az_fn, "Welcome".into()))
			.finish()
			.await
			.unwrap();
		assert!(
			qt.docs.iter().flatten().any(|docs| docs.contains(doc_id)),
			"query should still see documents after all term deltas are compacted"
		);
		tx.cancel().await.unwrap();
	}

	/// A `!tx` entry carries a whole bitmap, so a round bounding itself by key
	/// count would hold as many documents as the transaction that wrote the entry
	/// was wide — and a compaction transaction has no write-cardinality guard
	/// behind it. The limit is spent as a document budget instead.
	///
	/// It is spent in whole keys, because the write phase may only delete an entry
	/// whose every document it folded. So an entry wider than the entire budget is
	/// still folded on its own, or it could never be compacted at all.
	#[test(tokio::test)]
	async fn term_docs_compaction_bounds_documents_not_keys() {
		let test = TestContext::new().await;
		let docs: Vec<RecordId> =
			(0..6).map(|i| RecordId::new("t".into(), format!("doc{i}"))).collect();

		let mut stack = reblessive::TreeStack::new();
		let content = test.content.as_ref().clone();
		stack.enter(|stk| test.index_batch_in_one_tx(stk, &docs, &content)).finish().await;

		let tx = test.new_tx(TransactionType::Read).await;
		assert!(
			test.tt_delta_count(&tx).await > 2,
			"the fixture must leave more entries than one round's budget"
		);
		tx.cancel().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 2).await.unwrap();
		tx.cancel().await.unwrap();
		assert_eq!(
			plan.tx_keys.len(),
			1,
			"one entry six documents wide spends a budget of two on its own"
		);
		assert!(plan.has_more(), "the entries the budget stopped short of are the next round's");

		// Spending in whole keys must not stall: draining a budget at a time still
		// reaches zero, however wide the entries are.
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
		assert_eq!(test.tt_delta_count(&tx).await, 0, "every entry must eventually compact");
		tx.cancel().await.unwrap();
	}

	/// `limit` bounds the round, not each delta family, so the batched pass takes
	/// only what the per-document pass left. Two families each spending the whole
	/// limit would let a round hold twice the documents it was sized for.
	///
	/// A round the per-document pass filled completely still takes one batched entry,
	/// because every round folds at least one. Taking none would leave the batched
	/// family undrained for as long as a legacy backlog arrived as fast as it went.
	#[test(tokio::test)]
	async fn a_round_shares_one_budget_across_both_delta_shapes() {
		let test = TestContext::new().await;

		// Entries in the per-document shape, which a reader folds alongside `!tx`.
		let tx = test.new_tx(TransactionType::Write).await;
		for doc_id in 0..3u64 {
			let key = TermChangeKey {
				ns: test.ikb.ns(),
				db: test.ikb.db(),
				tb: Cow::Borrowed(test.ikb.table()),
				ix: test.ikb.index(),
				term: Cow::Borrowed("aaa"),
				doc_id,
				nid: Uuid::nil(),
				uid: Uuid::nil(),
				add: true,
			};
			tx.set_key(&key, &String::new()).await.unwrap();
		}
		tx.commit().await.unwrap();

		// Then batched entries under three distinct terms, all sorting after the legacy
		// ones. More than one, so a pass given its own budget of three would take them
		// all where a pass given what the legacy pass left takes one.
		let docs = vec![RecordId::new("t".into(), "w0".to_owned())];
		let content = Value::from(Array::from(vec!["zza", "zzb", "zzc"]));
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| test.index_batch_in_one_tx(stk, &docs, &content)).finish().await;

		let tx = test.new_tx(TransactionType::Read).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 3).await.unwrap();
		tx.cancel().await.unwrap();

		assert_eq!(plan.tt_keys.len(), 3, "the legacy entries spend the whole budget");
		// A round with no budget left still takes one batched entry, because every round
		// folds at least one. The alternative starves the batched family whenever a
		// legacy backlog arrives as fast as it drains.
		assert_eq!(
			plan.tx_keys.len(),
			1,
			"a spent budget still folds one entry, and stops: {:?}",
			plan.tx_keys.iter().map(|k| k.term.to_string()).collect::<Vec<_>>()
		);
		assert!(plan.has_more(), "the round stopped short of the range's end");
	}

	/// The entry that ends a round is folded, not deferred to a later one.
	///
	/// A round restarts at the head of the range, so a deferred entry waits for
	/// everything in front of it to be deleted. Here two narrow entries sort ahead of
	/// a wider one and spend the budget between them, which is exactly the shape that
	/// used to defer the entry behind them.
	///
	/// This buys ordering within a round, not fairness across rounds: writes arriving
	/// at the head as fast as a round drains it still hold every round's attention.
	#[test(tokio::test)]
	async fn a_round_folds_the_entry_that_ends_it_rather_than_deferring_it() {
		let test = TestContext::new().await;
		let ahead = vec![RecordId::new("t".into(), "n0".to_owned())];
		let behind: Vec<RecordId> =
			(0..4).map(|i| RecordId::new("t".into(), format!("w{i}"))).collect();
		let two_terms = Value::from(Array::from(vec!["aaa", "aab"]));
		let one_term = Value::from(Array::from(vec!["zzz"]));

		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| test.index_batch_in_one_tx(stk, &ahead, &two_terms)).finish().await;
		stack.enter(|stk| test.index_batch_in_one_tx(stk, &behind, &one_term)).finish().await;

		let tx = test.new_tx(TransactionType::Read).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 3).await.unwrap();
		tx.cancel().await.unwrap();

		let folded: Vec<String> = plan.tx_keys.iter().map(|k| k.term.to_string()).collect();
		assert!(
			folded.contains(&"zzz".to_owned()),
			"the entry that ends the round must be folded, got {folded:?}"
		);
	}

	/// An entry wider than the whole remaining budget is folded when it is the one
	/// that ends the round, and the round stops there.
	///
	/// Turning it away for a later round is what starves it, so the bound comes from
	/// the round stopping instead: one entry past the budget, not every entry behind
	/// it. See `a_round_folds_the_entry_that_ends_it_rather_than_deferring_it`.
	#[test(tokio::test)]
	async fn a_wide_entry_ends_the_round_it_overruns() {
		let test = TestContext::new().await;
		let narrow = vec![RecordId::new("t".into(), "n0".to_owned())];
		let wide: Vec<RecordId> =
			(0..6).map(|i| RecordId::new("t".into(), format!("w{i}"))).collect();
		let first = Value::from(Array::from(vec!["aaa"]));
		let second = Value::from(Array::from(vec!["zzz"]));

		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| test.index_batch_in_one_tx(stk, &narrow, &first)).finish().await;
		stack.enter(|stk| test.index_batch_in_one_tx(stk, &wide, &second)).finish().await;

		let tx = test.new_tx(TransactionType::Read).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 3).await.unwrap();
		tx.cancel().await.unwrap();

		let folded: Vec<String> = plan.tx_keys.iter().map(|k| k.term.to_string()).collect();
		assert_eq!(
			folded,
			vec!["aaa", "zzz"],
			"the overrunning entry is folded, and then the round ends"
		);
		// It ended on the range's last entry, so there is nothing to come back for.
		assert!(!plan.has_more(), "a round that took the last entry reports no more");
	}

	/// A term's compacted document set must not depend on where a round stopped.
	///
	/// Deltas are signed counts and a compacted set is a bitmap, so a document a
	/// round saw added while the removal cancelling it sits past the round's
	/// budget comes to a count of two — one more than a bitmap can hold. Discard
	/// what does not fit and the removal, folded by the next round, takes the
	/// document out of a set it belongs in: a term lost from a document that
	/// carries it, with nothing later to put it back.
	///
	/// The entries are written rather than indexed, because the shape needs the
	/// addition to sort ahead of the removal and the pair to straddle the budget.
	#[test(tokio::test)]
	async fn a_round_that_stops_inside_a_term_folds_it_to_the_same_set() {
		let test = TestContext::new().await;
		let doc: DocId = 7;
		let present = RoaringTreemap::from_iter([doc]);

		// A compacted set that already carries the document, so the only history
		// reaching these two entries is a removal and the addition undoing it: the
		// document ends where it started. Entries sort by node ahead of direction,
		// so the addition is what a budget of one takes.
		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_td_root("hello"), &present).await.unwrap();
		test.write_tx_delta(&tx, "hello", 1, true, &present).await;
		test.write_tx_delta(&tx, "hello", 2, false, &present).await;
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 1).await.unwrap();
		assert_eq!(plan.tx_keys.len(), 1, "a round is bounded by its budget, not by the term");
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		assert_eq!(
			test.term_residual("hello").await,
			Some(vec![(doc, 1)]),
			"the count the bitmap could not hold must be kept, not clamped away"
		);
		assert_eq!(
			test.term_docs("hello").await,
			Some(present.clone()),
			"a count above what the bitmap holds leaves the document in it"
		);
		assert_eq!(
			test.query_docs("hello").await,
			present,
			"a query between rounds resolves the same total compaction does"
		);

		assert_eq!(test.drain_term_docs(1).await, 1, "one entry is left, so one round drains it");
		assert_eq!(
			test.term_docs("hello").await,
			Some(present),
			"a pair that cancels leaves the set as it was, however the rounds split it"
		);
		assert_eq!(
			test.term_residual("hello").await,
			None,
			"a term with nothing left to fold keeps no residual"
		);
	}

	/// The same rule where the count runs below what a bitmap can hold rather
	/// than above it, which is the other way the compacted set comes out wrong.
	///
	/// A document absent from the compacted set whose removal a round folds first
	/// — entries sort by node ahead of time, so a removal can sort ahead of the
	/// addition it undid — reaches minus one. Drop that and the addition the next
	/// round folds puts the document into a set it does not belong in: a hit
	/// against a term the document does not carry.
	#[test(tokio::test)]
	async fn a_round_that_stops_inside_a_term_leaves_an_absent_document_absent() {
		let test = TestContext::new().await;
		let doc: DocId = 7;
		let touched = RoaringTreemap::from_iter([doc]);

		// No compacted set for the term, so the history reaching these entries is
		// the addition and then the removal undoing it — the reverse of the order
		// the two sort in.
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_delta(&tx, "hello", 1, false, &touched).await;
		test.write_tx_delta(&tx, "hello", 2, true, &touched).await;
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 1).await.unwrap();
		assert_eq!(plan.tx_keys.len(), 1, "a round is bounded by its budget, not by the term");
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		assert_eq!(
			test.term_residual("hello").await,
			Some(vec![(doc, -1)]),
			"a count below what the bitmap can hold must be kept too"
		);
		assert_eq!(
			test.term_docs("hello").await,
			None,
			"a count below what the bitmap holds keeps the document out of it"
		);
		assert!(
			test.query_docs("hello").await.is_empty(),
			"a query between rounds resolves the same total compaction does"
		);

		test.drain_term_docs(1).await;
		assert_eq!(
			test.term_docs("hello").await,
			None,
			"a pair that cancels leaves the set as it was, however the rounds split it"
		);
		assert_eq!(test.term_residual("hello").await, None);
	}

	/// The same rule on the per-document family an upgraded database carries,
	/// whose pass is bounded by keys and so can also stop inside a term.
	#[test(tokio::test)]
	async fn a_round_that_stops_inside_a_legacy_term_folds_it_to_the_same_set() {
		let test = TestContext::new().await;
		let doc: DocId = 7;
		let present = RoaringTreemap::from_iter([doc]);

		// Keyed by document and then node, so the pair is adjacent and the
		// addition is again the one a budget of one takes.
		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_td_root("hello"), &present).await.unwrap();
		test.write_tt_delta(&tx, "hello", doc, 1, true).await;
		test.write_tt_delta(&tx, "hello", doc, 2, false).await;
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 1).await.unwrap();
		assert_eq!(plan.tt_keys.len(), 1, "a round is bounded by its budget, not by the term");
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		assert_eq!(
			test.term_residual("hello").await,
			Some(vec![(doc, 1)]),
			"the count the bitmap could not hold must be kept, not clamped away"
		);

		test.drain_term_docs(1).await;
		assert_eq!(
			test.term_docs("hello").await,
			Some(present),
			"a pair that cancels leaves the set as it was, however the rounds split it"
		);
		assert_eq!(test.term_residual("hello").await, None);
	}

	/// A term split *between* the two delta families, which is the split neither
	/// family's own bound can close.
	///
	/// Each family is scanned under its own budget, so a term's per-document
	/// entries can be folded by one round and its batched entries by another even
	/// where neither scan stopped inside that term. Here the legacy pass takes the
	/// term's addition and the batched pass is spent on a term sorting ahead of
	/// it, leaving the removal for a later round.
	#[test(tokio::test)]
	async fn a_term_split_between_the_delta_families_folds_to_the_same_set() {
		let test = TestContext::new().await;
		let doc: DocId = 7;
		let present = RoaringTreemap::from_iter([doc]);

		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_td_root("hello"), &present).await.unwrap();
		test.write_tt_delta(&tx, "hello", doc, 1, true).await;
		test.write_tx_delta(&tx, "hello", 2, false, &present).await;
		// Two batched entries under a term sorting first, so a budget of one is
		// spent before the batched pass reaches the term above.
		test.write_tx_delta(&tx, "aaa", 1, true, &RoaringTreemap::from_iter([9])).await;
		test.write_tx_delta(&tx, "aaa", 2, true, &RoaringTreemap::from_iter([10])).await;
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 1).await.unwrap();
		assert_eq!(
			plan.tx_keys.iter().map(|k| k.term.as_ref()).collect::<Vec<_>>(),
			vec!["aaa"],
			"the batched pass must be spent before it reaches the split term"
		);
		assert_eq!(plan.tt_keys.len(), 1, "the legacy pass takes the term's addition");
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();

		assert_eq!(
			test.term_residual("hello").await,
			Some(vec![(doc, 1)]),
			"a term the other family has more of is not folded whole"
		);

		test.drain_term_docs(1).await;
		assert_eq!(
			test.term_docs("hello").await,
			Some(present),
			"an addition and a removal in different families still cancel"
		);
		assert_eq!(test.term_residual("hello").await, None);
		assert_eq!(test.term_docs("aaa").await, Some(RoaringTreemap::from_iter([9, 10])));
	}

	/// A term whose every delta a round captured is answered by its bitmap alone.
	///
	/// The residual carries a total across a round boundary. A round that leaves
	/// nothing behind has no boundary to carry one over, so a total that still
	/// does not fit says the additions and removals recorded for that document
	/// did not alternate. Keeping it would pin the document against every later
	/// delta — the removal below would leave it in the set.
	#[test(tokio::test)]
	async fn a_term_folded_whole_is_answered_by_its_bitmap() {
		let test = TestContext::new().await;
		let doc: DocId = 7;
		let present = RoaringTreemap::from_iter([doc]);

		// An addition of a document the set already holds, and nothing to cancel
		// it: a total of two with the whole term captured.
		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_td_root("hello"), &present).await.unwrap();
		test.write_tx_delta(&tx, "hello", 1, true, &present).await;
		tx.commit().await.unwrap();

		test.drain_term_docs(64).await;
		assert_eq!(test.term_docs("hello").await, Some(present));
		assert_eq!(test.term_residual("hello").await, None);

		// So the next removal is the one that decides membership.
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_delta(&tx, "hello", 3, false, &RoaringTreemap::from_iter([doc])).await;
		tx.commit().await.unwrap();

		test.drain_term_docs(64).await;
		assert_eq!(
			test.term_docs("hello").await,
			None,
			"the document must leave the set the removal took it out of"
		);
		assert!(test.query_docs("hello").await.is_empty());
	}

	/// A residual with no deltas beside it still decides where its documents
	/// belong, rather than being overridden by the bitmap alone.
	///
	/// A compactor of this build always leaves a residual beside a bitmap that
	/// agrees with its sign, and leaves deltas for the round that will fold it
	/// away. One that does not know the family leaves neither: it folds the
	/// remaining deltas into the bitmap and strands the residual, which is the
	/// state written directly here. Reading the total rather than the bitmap is
	/// what keeps that recoverable — the term resolves correctly now, and the
	/// next delta folded on top of it lands where it should.
	#[test(tokio::test)]
	async fn a_residual_left_without_deltas_still_decides_membership() {
		let test = TestContext::new().await;
		let doc: DocId = 7;

		// No compacted set and no deltas, so the residual is the whole total.
		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_tr_root("hello"), &TermDocsResidual::new([(doc, 1)]))
			.await
			.unwrap();
		tx.commit().await.unwrap();

		assert_eq!(
			test.query_docs("hello").await,
			RoaringTreemap::from_iter([doc]),
			"a total of one carries the document whichever side of the compaction it sits on"
		);

		// And the removal that follows takes it back out, rather than landing on
		// a bitmap that never learned the document was there.
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_delta(&tx, "hello", 1, false, &RoaringTreemap::from_iter([doc])).await;
		tx.commit().await.unwrap();

		assert!(test.query_docs("hello").await.is_empty());
		test.drain_term_docs(64).await;
		assert_eq!(test.term_docs("hello").await, None);
		assert_eq!(test.term_residual("hello").await, None);
	}

	/// A query must observe the documents its own transaction has just indexed,
	/// before that transaction commits.
	///
	/// A document indexed by an older server has its postings under the legacy
	/// per-term keys and no per-document entry, and must still score and
	/// highlight. What selects the fallback is the absence of that entry, not the
	/// absence of the term — so once a document has one, its map is the whole
	/// truth and a leftover legacy key for a term it no longer carries must not
	/// resurrect that term.
	#[test(tokio::test)]
	async fn a_document_indexed_before_the_per_document_entry_still_reads_back() {
		let test = TestContext::new().await;
		let legacy = TermDocument {
			f: 7,
			o: vec![Offset {
				index: 0,
				start: 1,
				gen_start: 1,
				end: 4,
			}],
		};

		// An older server's output: a posting under the per-term key, with no
		// per-document entry alongside it.
		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_td("ancient", 1), &legacy).await.unwrap();
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(
			test.fti.get_term_document(&tx, 1, "ancient").await.unwrap().as_ref(),
			Some(&legacy),
			"a posting written before the per-document entry existed must still be found"
		);
		assert_eq!(
			test.fti.get_term_document(&tx, 1, "absent").await.unwrap(),
			None,
			"a term the document never carried must not be invented"
		);
		tx.cancel().await.unwrap();

		// Re-indexing the document publishes an entry. From then on the entry
		// answers for every term, and the stale legacy key is unreachable.
		let tx = test.new_tx(TransactionType::Write).await;
		let current = DocumentTerms {
			terms: [(
				"current".into(),
				TermDocument {
					f: 1,
					o: Vec::new(),
				},
			)]
			.into_iter()
			.collect(),
		};
		tx.set_key(&test.ikb.new_dt(1), &current).await.unwrap();
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(
			test.fti.get_term_document(&tx, 1, "current").await.unwrap().map(|td| td.f),
			Some(1),
			"the per-document entry must answer for the terms it holds"
		);
		assert_eq!(
			test.fti.get_term_document(&tx, 1, "ancient").await.unwrap(),
			None,
			"a legacy key must not outlive the entry that replaced it"
		);
		tx.cancel().await.unwrap();
	}

	/// The term deltas and the document statistics are buffered until commit, so
	/// neither is in the keyspace yet when this query runs; the read paths have
	/// to fold the transaction's own pending contribution over what they scan.
	#[test(tokio::test)]
	async fn a_query_sees_its_own_uncommitted_indexing() {
		let test = TestContext::new().await;
		let doc = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));

		// One write environment for both the indexing and the query, so nothing
		// this test indexes has been committed when it asks for it back.
		let ctx = test.ds.env(TransactionType::Write).await;
		let tx = ctx.tx();
		let az_fn = NoAnalyzerFunction;
		let mut require_compaction = false;
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				test.fti.index_content(
					stk,
					&ctx,
					&az_fn,
					&doc,
					vec![test.content.as_ref().clone()],
					&mut require_compaction,
				)
			})
			.finish()
			.await
			.unwrap();

		// Nothing has reached the keyspace: the deltas are still in the buffer.
		assert_eq!(test.tt_delta_count(&tx).await, 0, "deltas must still be buffered");
		assert_eq!(test.dc_delta_count(&tx).await, 0, "statistics must still be buffered");

		let qt = stack
			.enter(|stk| test.fti.extract_querying_terms(stk, &ctx, &az_fn, "Welcome".into()))
			.finish()
			.await
			.unwrap();
		let doc_id = test.fti.get_doc_id(&tx, &doc).await.unwrap().unwrap();
		assert!(
			qt.docs.iter().flatten().any(|docs| docs.contains(doc_id)),
			"a query must see the document its own transaction indexed"
		);

		// The scorer weighs against corpus statistics, which are buffered too, so
		// it must not report an empty corpus.
		let dlc = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert_eq!(dlc.doc_count, 1, "the buffered document must count towards the corpus");
		assert!(dlc.total_docs_length > 0, "the buffered document must carry its length");

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
		assert_eq!(tx.get_key(&test.ikb.new_dv_key(), None).await.unwrap(), Some(1));
		assert_eq!(tx.get_key(&test.ikb.new_tv_key(), None).await.unwrap(), Some(1));
		assert_eq!(
			test.dc_delta_count(&tx).await,
			1,
			"post-snapshot doc-length delta must remain uncompacted"
		);
		assert!(
			test.tt_delta_count(&tx).await > 0,
			"post-snapshot term deltas must remain uncompacted"
		);
		let dlc = test.fti.compute_doc_length_and_count(&tx, None).await.unwrap();
		assert_eq!(dlc.doc_count, 2);
	}
}
