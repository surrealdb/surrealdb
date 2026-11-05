use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;
// A posting and the index's document statistics are stored values, so both are
// declared below this layer; the indexing and scoring code here maintains them.
pub use surrealdb_datastore::values::fulltext::{
	BlockSummary, ChangeBatch, ChangePostings, DocLengthAndCount, DocumentTerms, PostingBlock,
	TermBlocks, TermDocsResidual, TermDocument,
};
use surrealdb_kvs::Direction;
use surrealdb_kvs::consts::COUNT_BATCH_SIZE;
use uuid::Uuid;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{AnalyzerDefinition, FullTextParams, Scoring};
use crate::docids::{DocId, LegacyFullTextDocIds, TableDocIds};
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
use crate::ft::highlighter::HighlightParams;
use crate::ft::{DocLength, MatchesHitsIterator, Position, Score, TermFrequency};
use crate::kernels::postings::{BlockWindow, TermPostings};
use crate::key::schema::{
	DocStatsBatchKey, DocStatsDeltaKey, DocStatsKey, TermChangeBatchKey, TermChangeKey,
	TermDocsResidualKey,
};
use crate::key::{KVKey, KVKeyDecode, KVValue, Resumable};
use crate::trees::store::IndexStores;
use crate::val::{RecordId, Value};
use crate::{
	CompactionApply, IndexKeyBase, bump_compaction_generation, kernels, read_compaction_generation,
};

/// One document's frequency of each term a compaction round folds that it
/// carries, in term order, and its length: what its postings (`!dt`) and
/// length (`!dl`) hold for the round.
type StoredDocument = (Vec<(String, TermFrequency)>, DocLength);

/// A document's frequency of a term and its length, as a change carries them.
type Posting = (TermFrequency, DocLength);

/// The node and transaction ids a change is tagged with.
type ChangeTags = (Uuid, Uuid);

/// A round's additions or removals of a term, by document: the posting and tags
/// of its one addition, or the tags of its one removal, `None` for a document
/// more than one names.
type OneChange<T> = HashMap<DocId, Option<T>>;

/// One distinct term of a query, scored on its own: a document's score sums
/// the term's contribution over the terms it carries, whatever word of the
/// query the term belongs to.
struct ScoredTerm {
	term: String,
	docs: TermDocs,
	/// The term's compacted postings, where the index keeps them in blocks: a
	/// document no term holds uncompacted state for is scored from them.
	postings: Option<Arc<TermPostings>>,
}

/// Where a [`ScoredTerm`]'s document set is held.
enum TermDocs {
	/// The set of the query word that is this term alone, an index into
	/// [`QueryTerms::docs`].
	Word(usize),
	/// The set of a term only in words of several terms, which no word's set
	/// holds.
	Own(Option<RoaringTreemap>),
}

/// Represents the terms in a search query and their associated document sets
pub struct QueryTerms {
	/// The tokenized query terms
	#[allow(dead_code)]
	tokens: Tokens,
	/// The query's words, each as the terms it requires together: a word the
	/// analyzer split into several terms, such as the windows of a long query
	/// word under an n-gram analyzer, matches a document only when the document
	/// carries all of them. Each entry is the term strings of one word.
	groups: Vec<Vec<String>>,
	/// The terms of each word of `groups`, as indices into `scored`.
	words: Vec<Vec<usize>>,
	/// Document sets for each word (RoaringTreemap of document IDs), in the
	/// order of `groups`: the documents that carry every term of the word, or
	/// `None` when the word has no document set.
	docs: Vec<Option<RoaringTreemap>>,
	/// Indicates if any words in the query have no document set in the index
	#[allow(dead_code)]
	has_unknown_terms: bool,
	/// Each distinct term of the query, as a document's score takes it.
	scored: Vec<ScoredTerm>,
	/// Whether a term's compacted documents were read from a bitmap, which
	/// holds no frequency and so bounds no score.
	bitmaps: bool,
	/// Every document any of the terms holds uncompacted state for: a change
	/// not yet folded, one the reading transaction buffered, or a residual.
	/// Such a document may have been rewritten since the generation the
	/// node's cache is keyed by, so its stored state is never served from or
	/// put into the cache ([`crate::ft::cache`]).
	uncompacted: RoaringTreemap,
	/// The scope the terms were read at, when the node's cache may serve
	/// this query.
	scope: Option<crate::ft::cache::TermScope>,
	/// What each term of `scored` was resolved from beyond its set, in the same
	/// order, for terms read to judge documents while the reading transaction
	/// writes ([`FullTextIndex::extract_judging_terms`]): a document it writes
	/// after the terms were read is judged as it then stands
	/// ([`QueryTerms::contains_doc_now`]). Empty for any other terms, and for a
	/// transaction that cannot write, whose terms' sets answer for every
	/// document.
	bases: Vec<TermBasis>,
	/// How many documents carry each term of `scored`, in the same order, as
	/// the reading transaction held the index when a score last asked
	/// ([`Self::docs_now`]); `None` for a term no score has asked about. Empty
	/// until a score of terms with `bases` first asks.
	counts: parking_lot::Mutex<Vec<Option<TermCount>>>,
}

/// How many documents carry one term, as the reading transaction held the
/// index when the count was last brought up to date, with the changes it had
/// buffered to the term then: a document whose buffered change has not moved
/// since is counted as it stands now, so bringing the count up to date judges
/// only the documents whose change has ([`QueryTerms::docs_now`]).
struct TermCount {
	added: RoaringTreemap,
	removed: RoaringTreemap,
	docs: u64,
}

/// What one term's document set was resolved from beyond the set itself:
/// enough to judge one document's membership again once the reading
/// transaction's buffered change to it has moved.
///
/// A document carries the term when its compacted membership plus its total
/// change comes to one or more ([`kernels::fulltext::carries`]). Within a
/// transaction only the buffered part of that total moves: the compacted
/// documents, the residual and the uncompacted changes it reads change when a
/// transaction commits or a compaction folds them, neither of which a running
/// transaction observes. The buffered part is flushed when the transaction
/// commits.
struct TermBasis {
	/// The documents the transaction had added to the term and removed from it
	/// when the terms were read; a document in both had no net change.
	pending_added: RoaringTreemap,
	pending_removed: RoaringTreemap,
	/// Each document's committed change: its residual and its uncompacted
	/// changes. A document absent has none.
	committed: ahash::HashMap<DocId, i64>,
	/// Of the documents whose total change was not zero when the terms were
	/// read, those the term's compacted documents hold. Every other document's
	/// compacted membership is its membership of the term's set.
	compacted: RoaringTreemap,
}

impl TermBasis {
	/// The basis of a term whose compacted documents are `compacted` and whose
	/// per-document totals were `totals`: `committed` with the buffered changes
	/// `pending_added` and `pending_removed` added.
	fn new(
		pending_added: RoaringTreemap,
		pending_removed: RoaringTreemap,
		committed: ahash::HashMap<DocId, i64>,
		compacted: Option<&RoaringTreemap>,
		totals: &ahash::HashMap<DocId, i64>,
	) -> Self {
		let compacted = compacted
			.map(|compacted| {
				totals
					.iter()
					.filter(|(doc_id, total)| **total != 0 && compacted.contains(**doc_id))
					.map(|(doc_id, _)| *doc_id)
					.collect()
			})
			.unwrap_or_default();
		Self {
			pending_added,
			pending_removed,
			committed,
			compacted,
		}
	}

	/// The transaction's net change to document `doc_id`'s membership of the
	/// term when the terms were read.
	fn pending(&self, doc_id: DocId) -> i64 {
		Self::net(&self.pending_added, &self.pending_removed, doc_id)
	}

	/// The net change to document `doc_id`'s membership of a term that buffered
	/// changes adding the documents `added` and removing those `removed` name.
	fn net(added: &RoaringTreemap, removed: &RoaringTreemap, doc_id: DocId) -> i64 {
		i64::from(added.contains(doc_id)) - i64::from(removed.contains(doc_id))
	}

	/// Whether document `doc_id`, `resolved` its membership of the term's set,
	/// carries the term once `pending` stands in for the transaction's net change
	/// to it when the terms were read.
	fn carries(&self, doc_id: DocId, pending: i64, resolved: bool) -> bool {
		let committed = self.committed.get(&doc_id).copied().unwrap_or(0);
		let compacted = if committed + self.pending(doc_id) == 0 {
			resolved
		} else {
			self.compacted.contains(doc_id)
		};
		kernels::fulltext::carries(compacted, committed + pending)
	}
}

/// A query's terms, analyzed, and what each reads of the index
/// ([`FullTextIndex::plan_query`]).
pub struct QueryPlan {
	tokens: Tokens,
	terms: Vec<String>,
	/// The indices into `terms` each word requires together, in query order.
	groups: Vec<Vec<usize>>,
	reads: Vec<kernels::fulltext::TermReads>,
}

/// What one term's reads answered, with the changes the reading transaction
/// buffered.
pub struct TermState {
	/// The keys of [`kernels::fulltext::TermReads::changes`].
	pub change_keys: Vec<Vec<u8>>,
	/// The keys and changes of [`kernels::fulltext::TermReads::batches`].
	pub batches: Vec<(Vec<u8>, ChangeBatch)>,
	pub pending_added: RoaringTreemap,
	pub pending_removed: RoaringTreemap,
	/// The value at [`kernels::fulltext::TermReads::residual`].
	pub residual: Option<TermDocsResidual>,
	/// The term's compacted documents, as
	/// [`kernels::fulltext::TermReads::compacted`] keeps them.
	pub compacted: Compacted,
}

/// A term's compacted documents, as the index's format keeps them.
#[derive(Clone, Default)]
pub enum Compacted {
	/// None: the term holds no compacted document.
	#[default]
	Absent,
	/// The value at a [`kernels::fulltext::CompactedReads::Bitmap`] key.
	Bitmap(RoaringTreemap),
	/// The blocks a [`kernels::fulltext::CompactedReads::Blocks`] range holds.
	Postings(Arc<TermPostings>),
}

impl Compacted {
	/// The documents a term's compacted bitmap holds, absent for a term with
	/// none.
	pub fn from_bitmap(bitmap: Option<RoaringTreemap>) -> Self {
		bitmap.map_or(Self::Absent, Self::Bitmap)
	}

	/// The postings `blocks` hold, given in order of start. Fails on a block
	/// whose bytes do not decode to its entries.
	pub fn from_blocks(blocks: &[PostingBlock]) -> Result<Self> {
		if blocks.is_empty() {
			return Ok(Self::Absent);
		}
		Ok(Self::Postings(Arc::new(TermPostings::from_blocks(blocks)?)))
	}

	/// The compacted document set.
	fn docs(&self) -> Option<RoaringTreemap> {
		match self {
			Self::Absent => None,
			Self::Bitmap(bitmap) => Some(bitmap.clone()),
			Self::Postings(postings) => Some(postings.docs().clone()),
		}
	}

	/// The postings, where the index keeps them in blocks.
	fn postings(&self) -> Option<Arc<TermPostings>> {
		match self {
			Self::Postings(postings) => Some(Arc::clone(postings)),
			Self::Absent | Self::Bitmap(_) => None,
		}
	}
}

impl QueryPlan {
	/// The distinct terms, in query order.
	pub fn terms(&self) -> &[String] {
		&self.terms
	}

	/// What each term of [`Self::terms`] reads, in the same order.
	pub fn reads(&self) -> &[kernels::fulltext::TermReads] {
		&self.reads
	}

	/// The query's words with their document sets, from `states`, one per
	/// term of [`Self::terms`] in the same order. A word's document set is the
	/// intersection of its terms' sets.
	pub fn resolve(self, states: Vec<TermState>) -> Result<QueryTerms> {
		self.resolve_judging(states, false)
	}

	/// [`Self::resolve`], keeping beside each term's set what the set was
	/// resolved from where `judging`, so that a document the reading
	/// transaction writes after the terms were read is judged as it then stands
	/// ([`QueryTerms::contains_doc_now`]).
	fn resolve_judging(self, states: Vec<TermState>, judging: bool) -> Result<QueryTerms> {
		anyhow::ensure!(
			states.len() == self.terms.len(),
			"{} term states for {} terms",
			states.len(),
			self.terms.len()
		);
		let mut term_docs = Vec::with_capacity(states.len());
		let mut term_postings = Vec::with_capacity(states.len());
		let mut bases = Vec::with_capacity(if judging {
			states.len()
		} else {
			0
		});
		let mut bitmaps = false;
		let mut uncompacted = RoaringTreemap::new();
		for state in states {
			let mut committed = kernels::fulltext::term_deltas(&kernels::fulltext::TermChanges {
				change_keys: &state.change_keys,
				batches: &state.batches,
			})?;
			if let Some(residual) = &state.residual {
				kernels::fulltext::add_residual(&mut committed, residual);
			}
			let mut totals = if judging {
				committed.clone()
			} else {
				std::mem::take(&mut committed)
			};
			kernels::fulltext::add_pending(
				&mut totals,
				&state.pending_added,
				&state.pending_removed,
			);
			uncompacted.extend(totals.keys().copied());
			bitmaps |= matches!(state.compacted, Compacted::Bitmap(_));
			term_postings.push(state.compacted.postings());
			let compacted = state.compacted.docs();
			if judging {
				bases.push(TermBasis::new(
					state.pending_added,
					state.pending_removed,
					committed,
					compacted.as_ref(),
					&totals,
				));
			}
			term_docs.push(kernels::fulltext::term_docs(compacted, None, totals));
		}
		let mut docs = Vec::with_capacity(self.groups.len());
		let mut groups = Vec::with_capacity(self.groups.len());
		for members in &self.groups {
			let sets = members.iter().map(|&i| term_docs[i].as_ref()).collect::<Option<Vec<_>>>();
			docs.push(sets.and_then(|sets| {
				let mut sets = sets;
				sets.sort_by_key(|set| set.len());
				let mut sets = sets.into_iter();
				let mut result = sets.next()?.clone();
				for set in sets {
					result &= set;
				}
				(!result.is_empty()).then_some(result)
			}));
			groups.push(members.iter().map(|&i| self.terms[i].clone()).collect());
		}
		let scored = self
			.terms
			.into_iter()
			.zip(term_docs.iter_mut().zip(term_postings))
			.enumerate()
			.map(|(i, (term, (own, postings)))| ScoredTerm {
				term,
				docs: match self.groups.iter().position(|members| members.as_slice() == [i]) {
					Some(word) => TermDocs::Word(word),
					None => TermDocs::Own(own.take()),
				},
				postings,
			})
			.collect();
		let has_unknown_terms = docs.iter().any(Option::is_none);
		Ok(QueryTerms {
			tokens: self.tokens,
			groups,
			words: self.groups,
			docs,
			has_unknown_terms,
			scored,
			bitmaps,
			uncompacted,
			scope: None,
			bases,
			counts: parking_lot::Mutex::new(Vec::new()),
		})
	}
}

impl QueryTerms {
	/// The terms, read at `scope`: the node's cache may serve their documents'
	/// stored state.
	pub fn with_scope(mut self, scope: Option<crate::ft::cache::TermScope>) -> Self {
		self.scope = scope;
		self
	}

	/// These terms, scoring every document from its stored postings and
	/// bounding no score: for a statement that may rewrite a document after
	/// reading them, whose blocks then no longer describe it.
	pub fn scored_as_stored(mut self) -> Self {
		self.scored.iter_mut().for_each(|term| term.postings = None);
		self.bitmaps = true;
		self
	}

	/// The documents that carry `term`, `None` when none does.
	fn docs_of<'a>(&'a self, term: &'a ScoredTerm) -> Option<&'a RoaringTreemap> {
		match &term.docs {
			TermDocs::Word(word) => self.docs[*word].as_ref(),
			TermDocs::Own(docs) => docs.as_ref(),
		}
	}

	/// The scope a document's stored state may be cached and served at for
	/// these terms: `None` for a query the cache does not serve, and for a
	/// document the terms hold uncompacted state for.
	fn document_scope(&self, doc_id: DocId) -> Option<&crate::ft::cache::TermScope> {
		self.scope.as_ref().filter(|_| !self.uncompacted.contains(doc_id))
	}

	pub fn is_empty(&self) -> bool {
		self.tokens.list().is_empty()
	}

	/// Whether the terms hold uncompacted state for document `doc_id`, which
	/// is then scored from its stored postings rather than from any block.
	pub fn is_uncompacted(&self, doc_id: DocId) -> bool {
		self.uncompacted.contains(doc_id)
	}

	/// Upper bounds on the score `scorer` gives these terms in a document,
	/// over ranges of doc ids, from the impacts of the terms' posting blocks;
	/// `None` where no bound holds: a term's compacted documents were read
	/// from a bitmap, or the scorer's parameters do not make its score rise
	/// with frequency and fall with length
	/// ([`kernels::fulltext::Bm25::bounded_by_extremes`]).
	///
	/// A range starts at the first document of any term's block and ends at
	/// the next such start. Its bound sums, over each distinct term, the best
	/// score among the impacts of the term's block covering the range
	/// ([`TermPostings::block_impacts`]), so it is at least the score of every
	/// document in the range that the terms hold no uncompacted state for: such
	/// a document carries a term only where that term's blocks hold it, at a
	/// frequency and length one of the block's impacts dominates. It says nothing of an uncompacted
	/// document ([`Self::is_uncompacted`]).
	///
	/// Each such term's ceiling, the best score over all of its blocks, is kept
	/// beside the ranges: a document carrying only some of the terms scores no
	/// more than the sum of their ceilings ([`BlockBounds::passed_terms`]).
	pub fn block_bounds(&self, scorer: &Scorer) -> Option<BlockBounds> {
		if self.bitmaps || !scorer.bm25.bounded_by_extremes() {
			return None;
		}
		// Each distinct term once, as a document's score takes it.
		let mut terms = Vec::new();
		for (at, term) in self.scored.iter().enumerate() {
			let Some(docs) = self.docs_of(term) else {
				continue;
			};
			if let Some(postings) = &term.postings {
				terms.push((at, postings, docs.len() as f64));
			}
		}
		// Each term's blocks, scored once: a block's first document and the best score
		// among its impacts.
		let scored: Vec<Vec<(DocId, f64)>> = terms
			.iter()
			.map(|(_, postings, n)| {
				postings
					.block_impacts()
					.map(|(first, impacts)| {
						let best = impacts.iter().fold(0.0f64, |best, (frequency, length)| {
							best.max(scorer.bm25.score(*frequency as f64, *n, *length as f64))
						});
						(first, best)
					})
					.collect()
			})
			.collect();
		let mut starts: Vec<DocId> =
			scored.iter().flat_map(|blocks| blocks.iter().map(|(first, _)| *first)).collect();
		starts.sort_unstable();
		starts.dedup();
		// The ranges in order, each term's block covering a range found by a cursor
		// that only moves forward.
		let mut at = vec![0usize; scored.len()];
		let bounds = starts
			.iter()
			.map(|start| {
				let mut bound = 0.0f64;
				for (blocks, at) in scored.iter().zip(at.iter_mut()) {
					while blocks.get(*at + 1).is_some_and(|(first, _)| first <= start) {
						*at += 1;
					}
					if let Some((first, best)) = blocks.get(*at)
						&& first <= start
					{
						bound += best;
					}
				}
				round_up(bound)
			})
			.collect();
		let terms = terms
			.iter()
			.zip(&scored)
			.map(|((at, ..), blocks)| {
				(*at, blocks.iter().fold(0.0f64, |best, (_, b)| best.max(*b)))
			})
			.collect();
		Some(BlockBounds {
			starts,
			bounds,
			terms,
		})
	}

	/// Whether the document matches the query under `op`: it carries every
	/// term for `AND`, any term for `OR`. A term no document carries has no
	/// document set, so it can never be carried.
	pub fn contains_doc(&self, doc_id: DocId, op: BooleanOperator) -> bool {
		match op {
			BooleanOperator::And => {
				!self.docs.is_empty()
					&& self.docs.iter().all(|d| d.as_ref().is_some_and(|d| d.contains(doc_id)))
			}
			BooleanOperator::Or => self.docs.iter().flatten().any(|d| d.contains(doc_id)),
		}
	}

	/// Whether document `doc_id` carries any of the query's words.
	pub fn carries_any(&self, doc_id: DocId) -> bool {
		self.docs.iter().flatten().any(|d| d.contains(doc_id))
	}

	/// The documents carrying any of the query's words
	/// ([`Self::carries_any`]).
	pub fn carriers(&self) -> RoaringTreemap {
		kernels::fulltext::combine(&self.docs, BooleanOperator::Or).unwrap_or_default()
	}

	/// The documents the terms hold uncompacted state for
	/// ([`Self::is_uncompacted`]).
	pub fn uncompacted(&self) -> &RoaringTreemap {
		&self.uncompacted
	}

	/// Whether document `doc_id` carries any term of the query, whether or not
	/// it carries the rest of that term's word.
	fn carries_any_term(&self, doc_id: DocId) -> bool {
		self.carries_any(doc_id)
			|| self
				.scored
				.iter()
				.any(|t| matches!(&t.docs, TermDocs::Own(Some(d)) if d.contains(doc_id)))
	}

	/// Whether document `doc_id` carries term `at` of `scored`, as its set
	/// holds it.
	fn carries_term(&self, at: usize, doc_id: DocId) -> bool {
		self.docs_of(&self.scored[at]).is_some_and(|docs| docs.contains(doc_id))
	}

	/// Whether document `doc_id` matches the query under `op`
	/// ([`Self::contains_doc`]) as the reading transaction holds the index now,
	/// its own writes since the terms were read included. `pending` answers the
	/// transaction's net change to the document's membership of a term
	/// ([`FullTextIndex::contains_doc_now`]).
	///
	/// A term whose change stands as it did when the terms were read is judged
	/// by its set, and any other by what its set was resolved from with the
	/// change replaced, so the answer is the one terms read now would give, at
	/// the cost of a lookup per term. Terms that keep nothing beyond their sets
	/// — any [`FullTextIndex::extract_judging_terms`] did not read, or read for
	/// a transaction that cannot write — are judged by their sets alone,
	/// without asking `pending`.
	pub fn contains_doc_now(
		&self,
		doc_id: DocId,
		op: BooleanOperator,
		pending: impl FnMut(&str) -> i64,
	) -> bool {
		let Some(now) = self.moved(doc_id, pending) else {
			return self.contains_doc(doc_id, op);
		};
		let mut words = self.words_carried(&now);
		match op {
			BooleanOperator::And => !self.words.is_empty() && words.all(|carried| carried),
			BooleanOperator::Or => words.any(|carried| carried),
		}
	}

	/// Whether these are terms read to judge documents while the reading
	/// transaction writes ([`FullTextIndex::extract_judging_terms`]), which judge
	/// and score a document as the transaction holds the index when they are
	/// asked about it rather than as it held it when they were read.
	fn judging(&self) -> bool {
		!self.bases.is_empty()
	}

	/// Whether document `doc_id` carries each term of `scored`, in the same
	/// order, as the reading transaction holds the index now, where its
	/// membership of any term has moved since the terms were read; `None` where
	/// none has, so the terms' sets answer for it. `pending` answers the
	/// transaction's net change to the document's membership of a term, and is
	/// asked nothing for terms that keep no basis ([`Self::contains_doc_now`]).
	fn moved(&self, doc_id: DocId, mut pending: impl FnMut(&str) -> i64) -> Option<Vec<bool>> {
		let mut now: Option<Vec<bool>> = None;
		for (at, (term, basis)) in self.scored.iter().zip(&self.bases).enumerate() {
			let change = pending(&term.term);
			if change == basis.pending(doc_id) {
				continue;
			}
			let resolved = self.carries_term(at, doc_id);
			let carried = basis.carries(doc_id, change, resolved);
			if carried != resolved {
				now.get_or_insert_with(|| {
					(0..self.scored.len()).map(|at| self.carries_term(at, doc_id)).collect()
				})[at] = carried;
			}
		}
		now
	}

	/// Whether document `doc_id` carries each term of `scored`, in the same
	/// order, as the reading transaction holds the index now ([`Self::moved`]).
	fn carried_now(&self, doc_id: DocId, pending: impl FnMut(&str) -> i64) -> Vec<bool> {
		self.moved(doc_id, pending).unwrap_or_else(|| {
			(0..self.scored.len()).map(|at| self.carries_term(at, doc_id)).collect()
		})
	}

	/// Whether a document carrying the terms `carried` marks, one flag per term
	/// of `scored`, carries each of the query's words: every term of the word.
	fn words_carried<'a>(&'a self, carried: &'a [bool]) -> impl Iterator<Item = bool> + 'a {
		self.words.iter().map(move |word| word.iter().all(|at| carried[*at]))
	}

	/// [`Self::carries_any_term`] as the reading transaction holds the index now
	/// ([`Self::moved`]).
	fn carries_any_term_now(&self, doc_id: DocId, pending: impl FnMut(&str) -> i64) -> bool {
		match self.moved(doc_id, pending) {
			None => self.carries_any_term(doc_id),
			Some(now) => now.contains(&true),
		}
	}

	/// How many documents carry each term `carried` marks, one flag per term of
	/// `scored`, as the reading transaction holds the index now; `None` for every
	/// other term. `pending` answers the transaction's buffered changes to a
	/// term: the documents it added and those it removed.
	///
	/// A term with no basis is counted by its set. Any other term's count is
	/// kept between calls ([`Self::counts`]): its set's to begin with, then
	/// brought up to date by judging again only the documents whose buffered
	/// change has moved since it last was, each as [`Self::moved`] judges it. A
	/// reader asking after each of a run's writes so judges each written
	/// document once per change, rather than every document the run has
	/// written once per ask; it still compares the term's buffered sets whole
	/// each time, which costs a pass over their containers, not over their
	/// documents.
	fn docs_now(
		&self,
		carried: &[bool],
		mut pending: impl FnMut(&str) -> (RoaringTreemap, RoaringTreemap),
	) -> Vec<Option<u64>> {
		let mut counts = self.counts.lock();
		if counts.len() < self.scored.len() {
			counts.resize_with(self.scored.len(), || None);
		}
		let mut docs = Vec::with_capacity(self.scored.len());
		for (at, (term, carried)) in self.scored.iter().zip(carried).enumerate() {
			if !*carried {
				docs.push(None);
				continue;
			}
			let set = self.docs_of(term).map_or(0, RoaringTreemap::len);
			let Some(basis) = self.bases.get(at) else {
				docs.push(Some(set));
				continue;
			};
			let count = counts[at].get_or_insert_with(|| TermCount {
				added: basis.pending_added.clone(),
				removed: basis.pending_removed.clone(),
				docs: set,
			});
			let (added, removed) = pending(&term.term);
			let moved = (&added ^ &count.added) | (&removed ^ &count.removed);
			for doc_id in &moved {
				let resolved = self.carries_term(at, doc_id);
				let before = basis.carries(
					doc_id,
					TermBasis::net(&count.added, &count.removed, doc_id),
					resolved,
				);
				let after =
					basis.carries(doc_id, TermBasis::net(&added, &removed, doc_id), resolved);
				match (before, after) {
					(false, true) => count.docs += 1,
					(true, false) => count.docs = count.docs.saturating_sub(1),
					_ => {}
				}
			}
			count.added = added;
			count.removed = removed;
			docs.push(Some(count.docs));
		}
		docs
	}

	/// Whether `tks` holds every term of the word `terms`.
	fn word_in(terms: &[String], tks: &[Tokens]) -> Result<bool> {
		'terms: for t in terms {
			for tokens in tks {
				if tokens.try_contains(t)? {
					continue 'terms;
				}
			}
			return Ok(false);
		}
		Ok(true)
	}

	pub(in crate::ft) fn matches_or(&self, tks: &[Tokens]) -> Result<bool> {
		for word in &self.groups {
			if Self::word_in(word, tks)? {
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub(in crate::ft) fn matches_and(&self, tks: &[Tokens]) -> Result<bool> {
		for word in &self.groups {
			if !Self::word_in(word, tks)? {
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
	doc_ids: FullTextDocIds,
	/// BM25 scoring parameters, if scoring is enabled
	bm25: Option<Bm25Params>,
	/// Whether the index keeps its terms' compacted documents in posting
	/// blocks ([`surrealdb_catalog::keeps_posting_blocks`]) rather than in one
	/// bitmap per term.
	blocks: bool,
	/// The node's cache of terms' compacted document sets.
	cache: Arc<crate::ft::cache::FullTextCache>,
}

/// The document-ID resolver used by one full-text index.
#[derive(Clone, Copy)]
enum FullTextDocIds {
	Shared,
	Legacy,
}

impl FullTextDocIds {
	fn new(format_version: u16) -> Self {
		if surrealdb_catalog::uses_shared_doc_ids(format_version) {
			Self::Shared
		} else {
			Self::Legacy
		}
	}

	async fn get_doc_id(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		id: &crate::val::RecordIdKey,
	) -> Result<Option<DocId>> {
		match self {
			Self::Shared => {
				TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone()).get_doc_id(tx, id).await
			}
			Self::Legacy => {
				LegacyFullTextDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone(), ikb.index())
					.get_doc_id(tx, id)
					.await
			}
		}
	}

	async fn lock_unmapped(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		id: &crate::val::RecordIdKey,
	) -> Result<()> {
		match self {
			Self::Shared => {
				TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone())
					.lock_unmapped(tx, id)
					.await
			}
			Self::Legacy => {
				LegacyFullTextDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone(), ikb.index())
					.lock_unmapped(tx, id)
					.await
			}
		}
	}

	fn uses_shared(self) -> bool {
		matches!(self, Self::Shared)
	}

	async fn remove_legacy_doc_id(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<()> {
		LegacyFullTextDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone(), ikb.index())
			.remove_doc_id(tx, doc_id)
			.await
	}

	async fn resolve_or_assign(
		&self,
		ikb: &IndexKeyBase,
		env: &dyn IndexEnv,
		id: &crate::val::RecordIdKey,
	) -> Result<DocId> {
		match self {
			Self::Shared => {
				TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone())
					.resolve_or_assign(env, id)
					.await
			}
			Self::Legacy => {
				LegacyFullTextDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone(), ikb.index())
					.resolve_or_assign(env, id)
					.await
			}
		}
	}

	/// The record keys of `doc_ids`, in one batched read, in input order;
	/// `None` in the slot of an id no record maps to.
	async fn get_record_ids(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_ids: &[DocId],
	) -> Result<Vec<Option<crate::val::RecordIdKey>>> {
		match self {
			Self::Shared => {
				TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone())
					.get_record_ids_batch(tx, doc_ids)
					.await
			}
			Self::Legacy => {
				LegacyFullTextDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone(), ikb.index())
					.get_record_ids_batch(tx, doc_ids)
					.await
			}
		}
	}

	async fn get_record_id(
		&self,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<crate::val::RecordIdKey>> {
		match self {
			Self::Shared => {
				TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone())
					.get_record_id(tx, doc_id)
					.await
			}
			Self::Legacy => {
				LegacyFullTextDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone(), ikb.index())
					.get_record_id(tx, doc_id)
					.await
			}
		}
	}
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

/// Terms whose stored postings (`!tk` block lists or `!td` bitmaps) one read of
/// a compaction round's fold fetches together, along with, for an index that
/// keeps its postings in blocks, the stored postings and lengths (`!dt`, `!dl`)
/// of the documents those terms fold.
///
/// A round is bounded by the entries it captures, not by the size of what its
/// terms already store, so reading every term's postings at once would hold the
/// sum of them in memory where folding one term at a time holds one. Reading
/// them a chunk at a time keeps the batched round trip while bounding what is
/// resident to this many terms' postings and documents. A document named by
/// terms in several chunks is read once per chunk.
const TERM_FOLD_PREFETCH: usize = 64;

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

	/// Returns the number of delta and residual keys captured in this plan.
	pub fn captured_len(&self) -> usize {
		self.doc_lengths.deltas.legacy.len()
			+ self.doc_lengths.deltas.batched.len()
			+ self.term_docs.tt_keys.len()
			+ self.term_docs.tx_keys.len()
			+ self.term_docs.standalone_residuals.len()
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
	/// Each `!tx` entry in `tx_keys`, in the same order, for an index that
	/// keeps its postings in blocks: an addition carries its documents'
	/// postings, and a removal tells which documents a transaction also removed
	/// ([`FullTextIndex::changed_postings`]).
	batches: Vec<ChangeBatch>,
	/// The first term either scan stopped inside, before which every term's
	/// deltas were captured whole. `None` where both scans reached the end of
	/// their range, so every term this snapshot names was captured whole.
	///
	/// Both families order their entries by term, and each is bounded
	/// separately, so a term is captured whole only when both scans passed it —
	/// which is why one cutoff covers a split within either family and a split
	/// between them.
	captured_below: Option<String>,
	/// The terms holding a residual (`!tr`) and no delta this snapshot saw,
	/// every one of them captured whole. Only the term is kept: the write phase
	/// reads each residual once, under the generation it advances, as it does
	/// for a term it folds deltas for.
	standalone_residuals: Vec<String>,
	has_more: bool,
}

impl TermDocsCompactionPlan {
	/// Whether the plan holds anything to fold: a delta of either family, or a
	/// residual no delta sits beside.
	fn has_logs(&self) -> bool {
		!self.tt_keys.is_empty()
			|| !self.tx_keys.is_empty()
			|| !self.standalone_residuals.is_empty()
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
	/// Creates a new full-text index for the stored on-disk format.
	///
	/// Retrieves the analyzer from the database and selects shared or legacy
	/// doc-ID mappings based on `format_version`.
	pub async fn new(
		ixs: &IndexStores,
		tx: &Transaction,
		ikb: IndexKeyBase,
		p: &FullTextParams,
		allow_list: &[PathBuf],
		format_version: u16,
	) -> Result<Self> {
		let az = tx.get_db_analyzer(ikb.0.ns, ikb.0.db, &p.analyzer, None).await?;
		Self::with_analyzer(ixs, az, ikb, p, allow_list, format_version).await
	}

	/// Creates a new full-text index that analyses with `az`, the analyzer `p`
	/// names as the caller resolved it, rather than as the database holds it.
	pub async fn with_analyzer(
		ixs: &IndexStores,
		az: Arc<AnalyzerDefinition>,
		ikb: IndexKeyBase,
		p: &FullTextParams,
		allow_list: &[PathBuf],
		format_version: u16,
	) -> Result<Self> {
		ixs.mappers().check(&az, allow_list).await?;
		let analyzer = Analyzer::new(ixs, az).await?;
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
			doc_ids: FullTextDocIds::new(format_version),
			ikb,
			bm25,
			blocks: surrealdb_catalog::keeps_posting_blocks(format_version),
			cache: Arc::clone(ixs.fulltext_cache()),
		})
	}

	/// Removes a document's postings, length and term memberships from the
	/// index, returning its doc id, or `None` for a record with none.
	///
	/// A record with no doc-ID has nothing to remove, and that absence is read
	/// under a commit-time conflict registration: an index build replaying the
	/// write that created the record can map and index it after this
	/// transaction's snapshot, and a removal that registered nothing would commit
	/// beside that content — leaving the record matched by terms it no longer
	/// holds. Registered, the commit fails as a retryable conflict.
	///
	/// A removal is recorded only for a term the index holds for the document,
	/// because a term's changes within one transaction sum: a removal of a term
	/// the document never carried would cancel the re-addition an `UPDATE`
	/// records next, and the document would drop out of a term it keeps. So the
	/// terms come from what the index stored, not from `content`:
	///
	/// - the document's `!dt` entry, written for every document indexed since it existed, lists
	///   them exactly;
	/// - a document indexed before then has no `!dt` but has a length (`!dl`), and its terms are
	///   `content` analysed again, which is what was indexed while the analyzer is unchanged;
	/// - a document with no length was never indexed here — its doc id was assigned for another
	///   index on the table — so it has nothing to remove.
	///
	/// `!dl` is read first because it decides all three: [`Self::index_content`]
	/// writes `!dt` only beside `!dl`, and this method deletes both together, so
	/// a document without `!dl` has no `!dt` either.
	pub(crate) async fn remove_content(
		&self,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		rid: &RecordId,
		content: Vec<Value>,
		require_compaction: &mut bool,
	) -> Result<Option<DocId>> {
		let tx = env.tx();
		let nid = env.node_id();
		let Some(doc_id) = self.get_doc_id(&tx, rid).await? else {
			self.doc_ids.lock_unmapped(&self.ikb, &tx, &rid.key).await?;
			return Ok(None);
		};
		let dl_key = self.ikb.new_dl(doc_id);
		if let Some(dl) = tx.get_key(&dl_key, None).await? {
			let dt_key = self.ikb.new_dt(doc_id);
			if let Some(doc) = tx.get_key(&dt_key, None).await? {
				tx.del_key(&dt_key).await?;
				for term in doc.terms.keys() {
					self.ikb.buffer_tt(
						&tx,
						term,
						doc_id,
						nid,
						false,
						self.blocks.then_some((0, 0)),
					)?;
				}
			} else {
				let tokens =
					self.analyzer.analyze_content(az_fn, content, FilteringStage::Indexing).await?;
				let mut set = HashSet::new();
				for tks in &tokens {
					for t in tks.list() {
						let s = tks.get_token_string(t)?;
						if set.insert(s) {
							tx.del_key(&self.ikb.new_td(s, doc_id)).await?;
							self.ikb.buffer_tt(
								&tx,
								s,
								doc_id,
								nid,
								false,
								self.blocks.then_some((0, 0)),
							)?;
						}
					}
				}
			}
			tx.del_key(&dl_key).await?;
			// Decrease the doc count and total doc length
			let dcl = DocLengthAndCount {
				total_docs_length: -(dl as i128),
				doc_count: -1,
			};
			self.ikb.buffer_dx(&tx, dcl, nid);
			*require_compaction = true;
		}
		if !self.doc_ids.uses_shared() {
			self.doc_ids.remove_legacy_doc_id(&self.ikb, &tx, doc_id).await?;
		}
		Ok(Some(doc_id))
	}

	/// Indexes content in the full-text index
	///
	/// This method analyzes and indexes the specified content for a document.
	/// It resolves the document ID, tokenizes the content, and stores term
	/// frequencies and offsets.
	pub(crate) async fn index_content(
		&self,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		rid: &RecordId,
		content: Vec<Value>,
		require_compaction: &mut bool,
	) -> Result<()> {
		let tx = env.tx();
		let nid = env.node_id();
		// Resolve (or assign) the record's doc id in the table's shared space
		let doc_id = self.doc_ids.resolve_or_assign(&self.ikb, env, &rid.key).await?;
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(az_fn, content, FilteringStage::Indexing).await?;
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
			let f = o.len() as TermFrequency;
			postings.push((
				t.into(),
				TermDocument {
					f,
					o,
				},
			));
			self.ikb.buffer_tt(tx, t, id, *nid, true, self.blocks.then_some((f, dl)))?;
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
			self.ikb.buffer_tt(tx, t, id, *nid, true, self.blocks.then_some((f, dl)))?;
		}
		let doc = DocumentTerms {
			terms: postings.into_iter().collect(),
		};
		tx.set_key(&self.ikb.new_dt(id), &doc).await?;
		Ok(dl)
	}

	/// A query's terms: `query_string` analyzed as a query, each distinct
	/// term once, in the order it first appears. Reads nothing of the index;
	/// [`QueryPlan::reads`] names what each term reads.
	pub async fn plan_query(
		&self,
		az_fn: &dyn AnalyzerFunction,
		query_string: String,
	) -> Result<QueryPlan> {
		let tokens = self
			.analyzer
			.generate_tokens(az_fn, FilteringStage::Querying, query_string.into())
			.await?;
		let mut terms: Vec<String> = Vec::new();
		let mut term_index: HashMap<String, usize> = HashMap::new();
		let mut word_index: HashMap<Position, usize> = HashMap::new();
		let mut groups: Vec<Vec<usize>> = Vec::new();
		for token in tokens.list() {
			let text = tokens.get_token_string(token)?;
			let term = match term_index.get(text) {
				Some(&i) => i,
				None => {
					terms.push(text.to_owned());
					term_index.insert(text.to_owned(), terms.len() - 1);
					terms.len() - 1
				}
			};
			let word = *word_index.entry(token.word()).or_insert_with(|| {
				groups.push(Vec::new());
				groups.len() - 1
			});
			if !groups[word].contains(&term) {
				groups[word].push(term);
			}
		}
		let reads = terms
			.iter()
			.map(|term| kernels::fulltext::term_reads(&self.ikb, term, self.blocks))
			.collect::<Result<_>>()?;
		Ok(QueryPlan {
			tokens,
			terms,
			groups,
			reads,
		})
	}

	/// The scope `tx` reads this index's compacted terms at, when the node
	/// caches them and `tx` may read the cache ([`crate::ft::cache`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::idx::fulltext", skip_all)
	)]
	pub async fn term_scope(
		&self,
		tx: &Transaction,
	) -> Result<Option<crate::ft::cache::TermScope>> {
		if !self.cache.enabled() {
			return Ok(None);
		}
		crate::ft::cache::TermScope::of(tx, &self.ikb).await
	}

	/// The compacted state of each of `plan`'s terms, its documents and its
	/// residual, as the node's cache holds it at `scope`, in order; `None` for a
	/// term the caller reads and hands to [`Self::settle_states`], with the
	/// fill the caller completes there where it is the one reader of the term
	/// the others wait for ([`crate::ft::cache::TermFill`]).
	///
	/// Where `docs_only`, the caller scores no document from the terms'
	/// postings, and a term whose postings the cache does not hold may be
	/// served as its documents alone ([`Compacted::Bitmap`]); terms resolved
	/// from those score every document from its stored postings and bound no
	/// score, as [`QueryTerms::scored_as_stored`] does.
	///
	/// The fills are taken in the order of the terms' text, so two readers of
	/// overlapping terms never each wait for a term the other fills.
	#[allow(clippy::type_complexity)]
	pub async fn load_states(
		&self,
		scope: Option<&crate::ft::cache::TermScope>,
		plan: &QueryPlan,
		docs_only: bool,
	) -> (
		Vec<Option<(Compacted, Option<TermDocsResidual>)>>,
		Vec<Option<crate::ft::cache::TermFill<'_>>>,
	) {
		let mut cached: Vec<_> = plan.terms.iter().map(|_| None).collect();
		let mut fills: Vec<_> = plan.terms.iter().map(|_| None).collect();
		let Some(scope) = scope else {
			return (cached, fills);
		};
		let mut order: Vec<usize> = (0..plan.terms.len()).collect();
		order.sort_by(|a, b| plan.terms[*a].cmp(&plan.terms[*b]));
		for at in order {
			match self.cache.term_or_fill(scope, &plan.terms[at], docs_only).await {
				Ok(term) => cached[at] = Some((term.compacted.clone(), term.residual.clone())),
				Err(fill) => fills[at] = fill,
			}
		}
		(cached, fills)
	}

	/// Gives each of `states`, one per term of `plan`, its compacted documents
	/// and residual: the cached state where `cached` ([`Self::load_states`])
	/// holds one, and otherwise the next of `read`, the uncached terms' states
	/// in order as the caller read them, which is cached at `scope` and handed
	/// to the readers waiting on the term's fill in `fills`.
	pub fn settle_states(
		&self,
		scope: Option<&crate::ft::cache::TermScope>,
		plan: &QueryPlan,
		states: &mut [TermState],
		cached: Vec<Option<(Compacted, Option<TermDocsResidual>)>>,
		fills: Vec<Option<crate::ft::cache::TermFill<'_>>>,
		read: impl IntoIterator<Item = (Compacted, Option<TermDocsResidual>)>,
	) -> Result<()> {
		let mut read = read.into_iter();
		for (((state, cached), fill), term) in
			states.iter_mut().zip(cached).zip(fills).zip(&plan.terms)
		{
			let (compacted, residual) = match cached {
				Some(cached) => cached,
				None => {
					let (compacted, residual) = read
						.next()
						.ok_or_else(|| anyhow::anyhow!("a term's state the read did not answer"))?;
					match (fill, scope) {
						(Some(fill), Some(scope)) => fill.finish(
							&self.cache,
							scope,
							term,
							compacted.clone(),
							residual.clone(),
						),
						(None, Some(scope)) => {
							self.cache.insert_term(scope, term, compacted.clone(), residual.clone())
						}
						(_, None) => {}
					}
					(compacted, residual)
				}
			};
			state.compacted = compacted;
			state.residual = residual;
		}
		Ok(())
	}

	/// Reports the uncompacted term-change keys (`!tt`, `!tx`) read for
	/// `states` to `tx`'s execution observer. A term's residual (`!tr`) is not
	/// counted: it is at most one key per term, bounded rather than a log, and
	/// may have been served from the term cache rather than read. A read that
	/// met none reports nothing.
	pub fn record_pending_read(tx: &Transaction, states: &[TermState]) {
		let keys_read: u64 =
			states.iter().map(|s| (s.change_keys.len() + s.batches.len()) as u64).sum();
		Self::report_pending_read(tx, keys_read);
	}

	/// Reports `keys` uncompacted keys a query read of this index merged to
	/// `tx`'s execution observer, unless there were none.
	fn report_pending_read(tx: &Transaction, keys: u64) {
		if keys == 0 {
			return;
		}
		tx.observer().on_index_pending_read(&surrealdb_observe::IndexPendingReadEvent {
			safe: surrealdb_observe::IndexPendingReadEventSafe {
				kind: surrealdb_observe::IndexCompactionKind::FullText,
				keys,
			},
			ctx: surrealdb_observe::IndexPendingReadEventCtx::default(),
		});
	}

	/// The changes to `term` the transaction `tx` buffered and has not yet
	/// written: the documents it added and those it removed.
	pub fn pending_term_changes(
		&self,
		tx: &Transaction,
		term: &str,
	) -> (RoaringTreemap, RoaringTreemap) {
		self.ikb.pending_tt(tx, term)
	}

	/// Extracts query terms from a search string: [`Self::plan_query`], the
	/// reads it names made through `env`'s transaction, and
	/// [`QueryPlan::resolve`].
	///
	/// The residual and the bitmap are read for every term, not only for
	/// terms that have changes: see [`kernels::fulltext`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::idx::fulltext", skip_all)
	)]
	pub async fn extract_querying_terms(
		&self,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		query_string: String,
	) -> Result<QueryTerms> {
		self.read_terms(env, az_fn, query_string, false, false).await
	}

	/// [`Self::extract_querying_terms`] for a reader that judges documents one
	/// at a time while `env`'s transaction may write to them: where it may, the
	/// terms keep what each set was resolved from, so that a document it writes
	/// after the read is judged as it then stands ([`Self::contains_doc_now`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::idx::fulltext", skip_all)
	)]
	pub async fn extract_judging_terms(
		&self,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		query_string: String,
	) -> Result<QueryTerms> {
		let judging = env.tx().writeable();
		self.read_terms(env, az_fn, query_string, judging, false).await
	}

	/// The terms of `query_string` for a reader that only asks which
	/// documents carry them, scoring none: a term's documents may be served
	/// from the node's cache where its postings are not
	/// ([`crate::ft::cache::FullTextCache::term_docs`]). Read as
	/// [`Self::extract_judging_terms`] reads them where `judging`, and as
	/// [`Self::extract_querying_terms`] does otherwise.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::idx::fulltext", skip_all)
	)]
	pub async fn extract_membership_terms(
		&self,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		query_string: String,
		judging: bool,
	) -> Result<QueryTerms> {
		let judging = judging && env.tx().writeable();
		self.read_terms(env, az_fn, query_string, judging, true).await
	}

	/// The terms of `query_string`, read through `env`'s transaction and
	/// resolved as [`QueryPlan::resolve_judging`] resolves them; their
	/// documents alone where `docs_only` ([`Self::cached_states`]).
	async fn read_terms(
		&self,
		env: &dyn IndexEnv,
		az_fn: &dyn AnalyzerFunction,
		query_string: String,
		judging: bool,
		docs_only: bool,
	) -> Result<QueryTerms> {
		let plan = self.plan_query(az_fn, query_string).await?;
		let tx = env.tx();
		let mut states = Vec::with_capacity(plan.reads.len());
		for (term, reads) in plan.terms.iter().zip(&plan.reads) {
			let change_keys = tx.keys(reads.changes.clone(), u32::MAX, 0, None).await?;
			let batches = tx.getr(reads.batches.clone(), None).await?;
			let (pending_added, pending_removed) = self.pending_term_changes(&tx, term);
			states.push(TermState {
				change_keys,
				batches,
				pending_added,
				pending_removed,
				residual: None,
				compacted: Compacted::Absent,
			});
		}
		// The compacted state of a term the node has cached for this snapshot's
		// generation is taken from memory; the rest is read, and cached.
		let scope = self.term_scope(&tx).await?;
		let (cached, fills) = self.load_states(scope.as_ref(), &plan, docs_only).await;
		let missing: Vec<&str> = plan
			.terms
			.iter()
			.zip(&cached)
			.filter(|(_, cached)| cached.is_none())
			.map(|(term, _)| term.as_str())
			.collect();
		let residual_keys: Vec<_> = missing.iter().map(|term| self.ikb.new_tr_root(term)).collect();
		let residuals = tx.get_many_key(residual_keys, None).await?;
		let mut compacted = Vec::with_capacity(missing.len());
		if self.blocks {
			for term in &missing {
				let blocks: Vec<PostingBlock> = tx
					.getr(self.ikb.new_tp_term_range(term)?, None)
					.await?
					.into_iter()
					.map(|(_, block)| block)
					.collect();
				compacted.push(Compacted::from_blocks(&blocks)?);
			}
		} else {
			let bitmap_keys: Vec<_> =
				missing.iter().map(|term| self.ikb.new_td_root(term)).collect();
			compacted.extend(
				tx.get_many_key(bitmap_keys, None).await?.into_iter().map(Compacted::from_bitmap),
			);
		}
		self.settle_states(
			scope.as_ref(),
			&plan,
			&mut states,
			cached,
			fills,
			compacted.into_iter().zip(residuals),
		)?;
		Self::record_pending_read(&tx, &states);
		Ok(plan.resolve_judging(states, judging)?.with_scope(scope))
	}

	pub async fn matches_value(
		&self,
		az_fn: &dyn AnalyzerFunction,
		qt: &QueryTerms,
		bo: BooleanOperator,
		val: Value,
	) -> Result<bool> {
		let mut tks = vec![];
		self.analyzer.analyze_value(az_fn, val, FilteringStage::Indexing, &mut tks).await?;
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
		mut docs: RoaringTreemap,
		mut totals: HashMap<DocId, i64>,
		residual: Option<TermDocsResidual>,
		captured_whole: bool,
	) -> Result<()> {
		let td = self.ikb.new_td_root(term);

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
		self.write_residual(tx, term, !carried.is_empty(), false, left_over).await
	}

	/// Writes what a round left of `term`'s totals as its residual, or removes
	/// the residual it `carried` where nothing is left. Where `split`, the round
	/// stopped inside the term, and the residual is written even where it holds
	/// no total.
	async fn write_residual(
		&self,
		tx: &Transaction,
		term: &str,
		carried: bool,
		split: bool,
		left_over: Vec<(DocId, i64)>,
	) -> Result<()> {
		let tr = self.ikb.new_tr_root(term);
		if left_over.is_empty() && !split {
			// Only where one is there to remove: the residual is absent for all
			// but the terms a round stopped inside, and a delete of a key that was
			// never written is a write nonetheless.
			if carried {
				tx.del_key(&tr).await?;
			}
		} else {
			tx.set_key(&tr, &TermDocsResidual::new(left_over)).await?;
		}
		Ok(())
	}

	/// [`Self::fold_term_docs`] for an index that keeps its postings in
	/// blocks: the same totals decide which documents the term's blocks hold,
	/// and each document they hold takes its frequency of the term and its
	/// length from `changed`, the posting of the one addition that is its only
	/// change in the round ([`Self::changed_postings`]), and otherwise from
	/// `stored`, its postings (`!dt`) and length (`!dl`) as the round read them.
	/// A round that stops inside the term leaves it a residual, even an empty
	/// one, after which an addition's posting places only a document the blocks
	/// do not hold yet.
	/// The two agree: an addition's posting is what it wrote to `!dt` and `!dl`,
	/// and a later write to either is a change of its own.
	///
	/// A document folded present has its entry put afresh, whether or not the
	/// blocks held it already: a rewrite of a document records a change for
	/// every term it carries, so the round that folds that change is the one
	/// that brings the entry up to date. A document whose stored postings no
	/// longer carry the term — removed by a change the round did not capture —
	/// is placed with a frequency of zero, and stays uncompacted until a later
	/// round folds that change.
	#[expect(clippy::too_many_arguments, reason = "the round's view of one term")]
	async fn fold_term_blocks(
		&self,
		tx: &Transaction,
		term: &str,
		listed: Option<TermBlocks>,
		mut totals: HashMap<DocId, i64>,
		residual: Option<TermDocsResidual>,
		captured_whole: bool,
		changed: Option<&HashMap<DocId, Posting>>,
		stored: &HashMap<DocId, StoredDocument>,
	) -> Result<()> {
		let tk = self.ikb.new_tk(term);
		let had_blocks = listed.is_some();
		let mut window = BlockWindow::new(listed);

		let had_residual = residual.is_some();
		let carried = residual.unwrap_or_default().counts;
		for (doc_id, count) in carried.iter() {
			*totals.entry(*doc_id).or_default() += *count;
		}
		let starts = window.starts_for(totals.keys().copied());
		self.load_blocks(tx, term, &mut window, starts).await?;
		// After a round that stopped inside the term, a document the blocks hold
		// is read rather than placed from a change (below), and the round read
		// none a change answered.
		let mut held_stored = HashMap::new();
		if had_residual && let Some(changed) = changed {
			let held: Vec<DocId> =
				changed.keys().copied().filter(|doc_id| window.contains(*doc_id)).collect();
			if !held.is_empty() {
				let term_only = HashSet::from([term.to_owned()]);
				self.read_stored(tx, held, &term_only, &mut held_stored).await?;
			}
		}
		let mut folded: Vec<(DocId, i64)> = totals.into_iter().collect();
		folded.sort_unstable();
		let mut left_over = Vec::new();
		for (doc_id, total) in folded {
			let held = window.contains(doc_id);
			let total = i64::from(held) + total;
			match 0.cmp(&total) {
				Ordering::Less => {
					// From the change that placed the document where that is its
					// only change, and otherwise from what it stores. After a round
					// that stopped inside the term, an addition may be older than one
					// that placed the document, so only one placing it first counts.
					let changed = changed
						.and_then(|changed| changed.get(&doc_id))
						.filter(|_| !(had_residual && held) && carried.get(&doc_id).is_none());
					let (frequency, length) = match changed {
						Some(posting) => *posting,
						None => {
							let (frequencies, length) = stored
								.get(&doc_id)
								.or_else(|| held_stored.get(&doc_id))
								.map_or((&[][..], 0), |(f, l)| (f.as_slice(), *l));
							let frequency = frequencies
								.binary_search_by(|(t, _)| t.as_str().cmp(term))
								.map_or(0, |at| frequencies[at].1);
							(frequency, length)
						}
					};
					window.put(doc_id, frequency, length);
					if !captured_whole && total > 1 {
						left_over.push((doc_id, total - 1));
					}
				}
				Ordering::Greater => {
					window.remove(doc_id);
					if !captured_whole {
						left_over.push((doc_id, total));
					}
				}
				Ordering::Equal => {
					window.remove(doc_id);
					// Changes newer than an addition a later round folds may net to
					// nothing here; the residual names the document so that round
					// reads it rather than taking the older addition's posting.
					if !captured_whole {
						left_over.push((doc_id, 0));
					}
				}
			}
		}
		let partners = window.merge_partners();
		self.load_blocks(tx, term, &mut window, partners).await?;

		let (writes, blocks) = window.finish();
		for (start, block) in writes {
			let key = self.ikb.new_tp(term, start);
			match block {
				Some(block) => tx.set_key(&key, &block).await?,
				None => tx.del_key(&key).await?,
			}
		}
		match blocks {
			Some(blocks) => tx.set_key(&tk, &blocks).await?,
			None if had_blocks => tx.del_key(&tk).await?,
			None => {}
		}
		// A round that stops inside the term leaves a residual even where no total
		// is left over, so the next round knows an addition it folds may be older
		// than one this round placed ([`Self::changed_postings`]).
		self.write_residual(tx, term, had_residual, !captured_whole, left_over).await
	}

	/// Loads `term`'s blocks starting at `starts` into `window`, in one batched
	/// read. A block its list names that is not stored loads as an empty one.
	async fn load_blocks(
		&self,
		tx: &Transaction,
		term: &str,
		window: &mut BlockWindow,
		starts: std::collections::BTreeSet<DocId>,
	) -> Result<()> {
		if starts.is_empty() {
			return Ok(());
		}
		let keys = starts.iter().map(|start| self.ikb.new_tp(term, *start)).collect();
		let blocks: Vec<Option<PostingBlock>> = tx.get_many_key(keys, None).await?;
		for (start, block) in starts.into_iter().zip(blocks) {
			window.load(start, &block.unwrap_or_default())?;
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
		let mut batches = Vec::new();
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
			for (index, (k, batch)) in page.into_iter().enumerate() {
				let docs = &batch.docs;
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
				for doc_id in docs {
					*by_doc.entry(doc_id).or_default() += step;
				}
				if self.blocks {
					batches.push(batch);
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
		let mut has_more = tt_cut_short || batched_has_more;
		// A residual with no delta beside it is reached by no delta scan, so the
		// round looks for one itself. Only below the cutoff, where both delta scans
		// passed: a term there that holds no delta in this snapshot has none at
		// all, so its residual is its whole remaining total and folds as a term
		// captured whole. A term at or past the cutoff is left for the round that
		// reaches its deltas, and the round already reports more work.
		//
		// The round captures at most what the delta scans left of its budget,
		// counted in entries as the `!tt` scan counts them, and looks one term
		// further to learn whether a later round is owed. A residual of a term
		// whose deltas this round folds is read and folded with those deltas, so
		// the scan steps over it: it counts toward neither the capture nor the
		// look ahead. With nothing left of the budget, the scan only looks.
		let spent = (tt_keys.len() as u64).saturating_add(folded_docs.max(tx_keys.len() as u64));
		let residual_budget = (limit as u64).saturating_sub(spent) as usize;
		let mut standalone_residuals = Vec::new();
		if residual_budget > 0 || !has_more {
			let mut range = self.ikb.new_tr_terms_range(captured_below.as_deref())?;
			loop {
				let wanted = residual_budget + 1 - standalone_residuals.len();
				let rows = (wanted as u64).clamp(1, TERM_DELTA_SCAN_PAGE as u64) as u32;
				let keys = tx.keys(range.clone(), rows, 0, None).await?;
				let page_full = keys.len() as u32 == rows;
				let resume_after = keys.last().cloned();
				for k in keys {
					let term = TermDocsResidualKey::decode_key(&k)?.term.into_owned();
					// A term counts as folded with its deltas only where it has some:
					// an empty `!tx` batch leaves an empty entry, which the write
					// skips, so its residual is folded on its own.
					if deltas_by_term.get(&term).is_none_or(HashMap::is_empty) {
						standalone_residuals.push(term);
					}
				}
				if standalone_residuals.len() > residual_budget {
					standalone_residuals.truncate(residual_budget);
					has_more = true;
					break;
				}
				match resume_after {
					Some(k) if page_full => range = range.resume_after(&k, Direction::Forward),
					_ => break,
				}
			}
		}
		Ok(TermDocsCompactionPlan {
			generation,
			deltas_by_term,
			tt_keys,
			tx_keys,
			batches,
			captured_below,
			standalone_residuals,
			has_more,
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
		let mut folded: Vec<(String, HashMap<DocId, i64>, bool)> =
			std::mem::take(&mut plan.deltas_by_term)
				.into_iter()
				.filter(|(_, deltas)| !deltas.is_empty())
				.map(|(term, deltas)| {
					let captured_whole = plan.captured_whole(&term);
					(term, deltas, captured_whole)
				})
				.collect();
		// A standalone residual is folded as a term with no deltas, captured
		// whole: its total lands in the bitmap and the residual is removed.
		folded.extend(
			std::mem::take(&mut plan.standalone_residuals)
				.into_iter()
				.map(|term| (term, HashMap::new(), true)),
		);
		// One round trip for every term's residual rather than one per term. A
		// round folds as many terms as its budget allows, and all but the term it
		// stopped inside and the standalone residuals have no residual to read.
		let keys: Vec<_> = folded.iter().map(|(term, ..)| self.ikb.new_tr_root(term)).collect();
		let residuals: Vec<Option<TermDocsResidual>> = tx.get_many_key(keys, None).await?;
		if self.blocks {
			let changed = self.changed_postings(&plan, &folded);
			let mut pending = folded.into_iter().zip(residuals).peekable();
			while pending.peek().is_some() {
				let (chunk, chunk_residuals): (Vec<_>, Vec<_>) =
					pending.by_ref().take(TERM_FOLD_PREFETCH).unzip();
				// The documents the chunk's terms fold, with their frequencies of
				// those terms only: a document several chunks name is read once
				// per chunk.
				let terms: HashSet<String> = chunk.iter().map(|(term, ..)| term.clone()).collect();
				let stored =
					self.stored_postings(tx, &chunk, &chunk_residuals, &changed, &terms).await?;
				let tk_keys: Vec<_> =
					chunk.iter().map(|(term, ..)| self.ikb.new_tk(term)).collect();
				let listed_blocks: Vec<Option<TermBlocks>> = tx.get_many_key(tk_keys, None).await?;
				for (((term, deltas, captured_whole), residual), listed) in
					chunk.into_iter().zip(chunk_residuals).zip(listed_blocks)
				{
					let changed = changed.get(&term);
					self.fold_term_blocks(
						tx,
						&term,
						listed,
						deltas,
						residual,
						captured_whole,
						changed,
						&stored,
					)
					.await?;
				}
			}
		} else {
			let mut pending = folded.into_iter().zip(residuals).peekable();
			while pending.peek().is_some() {
				let chunk: Vec<_> = pending.by_ref().take(TERM_FOLD_PREFETCH).collect();
				let td_keys: Vec<_> =
					chunk.iter().map(|((term, ..), _)| self.ikb.new_td_root(term)).collect();
				let term_docs: Vec<Option<RoaringTreemap>> = tx.get_many_key(td_keys, None).await?;
				for (((term, deltas, captured_whole), residual), docs) in
					chunk.into_iter().zip(term_docs)
				{
					self.fold_term_docs(
						tx,
						&term,
						docs.unwrap_or_default(),
						deltas,
						residual,
						captured_whole,
					)
					.await?;
				}
			}
		}
		for key in &plan.tt_keys {
			tx.del_key(key).await?;
		}
		for key in &plan.tx_keys {
			tx.del_key(key).await?;
		}
		Ok(())
	}

	/// The postings the round's additions carry (`!tx`), by term, for each
	/// document whose only changes to the term in the round are one such
	/// addition and, at most, a removal by the same transaction: its total is
	/// one, or zero for a transaction that kept the term (removed it and gave it
	/// back), and no other change or per-document change (`!tt`) names it.
	/// Every addition to an index that keeps its postings in blocks carries its
	/// postings, so such a document's current posting is that addition's.
	///
	/// Additions to a term are ordered by node rather than by time, but a
	/// snapshot that sees an addition sees every earlier one, so an addition is
	/// folded after a later one only where a round stopped inside the term
	/// between them, and such a round leaves the term a residual. In a term
	/// holding none, an addition folded alone is the document's newest. In one
	/// holding a residual, it is only where the term's blocks do not hold the
	/// document yet, so no later addition has placed it
	/// ([`Self::fold_term_blocks`]).
	fn changed_postings(
		&self,
		plan: &TermDocsCompactionPlan,
		folded: &[(String, HashMap<DocId, i64>, bool)],
	) -> HashMap<String, HashMap<DocId, Posting>> {
		let batches = || plan.tx_keys.iter().zip(&plan.batches);
		// The posting and transaction of each document's one addition, `None`
		// for a document more than one addition names.
		let mut found: HashMap<&str, OneChange<(Posting, ChangeTags)>> = HashMap::new();
		for (key, batch) in batches().filter(|(key, _)| key.add) {
			let docs = &batch.docs;
			let by_doc = found.entry(key.term.as_ref()).or_default();
			// An addition that carries no postings, or postings that do not pair one
			// for one with its documents, tells nothing of them, and it is still one
			// of the additions that name them: they are read from what they store.
			let postings = match &batch.postings {
				Some(carried) => {
					let postings = carried.postings();
					if postings.as_ref().is_none_or(|postings| postings.len() as u64 != docs.len())
					{
						tracing::warn!(
							target: "surrealdb::idx::fulltext",
							"Full-text index {} on table {} holds change postings that do not pair \
							 with their change ({} documents, {} postings); reading the documents",
							self.ikb.index().0,
							self.ikb.table(),
							docs.len(),
							carried.len(),
						);
						None
					} else {
						postings
					}
				}
				None => None,
			};
			let Some(postings) = postings else {
				for doc_id in docs {
					by_doc.insert(doc_id, None);
				}
				continue;
			};
			for (doc_id, posting) in docs.iter().zip(postings) {
				// A frequency of zero: the transaction's last change to the
				// document carried no posting.
				let posting = (posting.0 > 0).then_some((posting, (key.nid, key.uid)));
				by_doc.entry(doc_id).and_modify(|added| *added = None).or_insert(posting);
			}
		}
		// The transaction of each document's one removal, `None` for a document
		// more than one removal names.
		let mut removed: HashMap<&str, OneChange<ChangeTags>> = HashMap::new();
		for (key, batch) in batches().filter(|(key, _)| !key.add) {
			let by_doc = removed.entry(key.term.as_ref()).or_default();
			for doc_id in &batch.docs {
				by_doc
					.entry(doc_id)
					.and_modify(|tags| *tags = None)
					.or_insert(Some((key.nid, key.uid)));
			}
		}
		for key in &plan.tt_keys {
			if let Some(added) =
				found.get_mut(key.term.as_ref()).and_then(|by_doc| by_doc.get_mut(&key.doc_id))
			{
				*added = None;
			}
		}
		let totals_by_term: HashMap<&str, &HashMap<DocId, i64>> =
			folded.iter().map(|(term, totals, _)| (term.as_str(), totals)).collect();
		found
			.into_iter()
			.map(|(term, by_doc)| {
				let totals = totals_by_term.get(term);
				let removed = removed.get(term);
				let by_doc = by_doc
					.into_iter()
					.filter_map(|(doc_id, added)| {
						let (posting, tags) = added?;
						let total = totals.and_then(|totals| totals.get(&doc_id));
						let removal = removed.and_then(|removed| removed.get(&doc_id));
						let alone = match (total, removal) {
							(Some(1), None) => true,
							(Some(0), Some(Some(removed_by))) => *removed_by == tags,
							_ => false,
						};
						alone.then_some((doc_id, posting))
					})
					.collect();
				(term.to_owned(), by_doc)
			})
			.collect()
	}

	/// The stored postings and length of every document `folded` or
	/// `residuals` name that `changed` does not answer for the term, read in
	/// one batch of each, for the entries a round puts in posting blocks. A
	/// document with no stored postings or length reads as having none.
	async fn stored_postings(
		&self,
		tx: &Transaction,
		folded: &[(String, HashMap<DocId, i64>, bool)],
		residuals: &[Option<TermDocsResidual>],
		changed: &HashMap<String, HashMap<DocId, Posting>>,
		terms: &HashSet<String>,
	) -> Result<HashMap<DocId, StoredDocument>> {
		let mut doc_ids: Vec<DocId> = folded
			.iter()
			.flat_map(|(term, deltas, _)| {
				let changed = changed.get(term);
				deltas.keys().copied().filter(move |doc_id| {
					changed.is_none_or(|changed| !changed.contains_key(doc_id))
				})
			})
			.chain(residuals.iter().flatten().flat_map(|r| r.counts.iter().map(|(id, _)| *id)))
			.collect();
		doc_ids.sort_unstable();
		doc_ids.dedup();
		let mut stored = HashMap::with_capacity(doc_ids.len());
		self.read_stored(tx, doc_ids, terms, &mut stored).await?;
		Ok(stored)
	}

	/// Reads the postings and length of each of `doc_ids` into `stored`,
	/// keeping each document's frequency of the round's `terms` only, in one
	/// batched read of each. A document with no stored postings or length
	/// reads as having none.
	async fn read_stored(
		&self,
		tx: &Transaction,
		doc_ids: Vec<DocId>,
		terms: &HashSet<String>,
		stored: &mut HashMap<DocId, StoredDocument>,
	) -> Result<()> {
		let lengths: Vec<Option<DocLength>> =
			tx.get_many_key(doc_ids.iter().map(|id| self.ikb.new_dl(*id)).collect(), None).await?;
		let postings: Vec<Option<DocumentTerms>> =
			tx.get_many_key(doc_ids.iter().map(|id| self.ikb.new_dt(*id)).collect(), None).await?;
		for ((doc_id, length), postings) in doc_ids.into_iter().zip(lengths).zip(postings) {
			// In term order, as the entry keeps them, so a lookup can search.
			let frequencies = postings
				.map(|doc| {
					doc.terms
						.iter()
						.filter(|(term, _)| terms.contains(term.as_str()))
						.map(|(term, posting)| (term.as_str().to_owned(), posting.f))
						.collect()
				})
				.unwrap_or_default();
			stored.insert(doc_id, (frequencies, length.unwrap_or_default()));
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
		let hits = kernels::fulltext::combine(&qt.docs, bo);

		// Create and return an iterator if we have matching documents
		if let Some(hits) = hits
			&& !hits.is_empty()
		{
			return Some(FullTextHitsIterator::new(
				self.ikb.clone(),
				self.doc_ids,
				hits,
				matches!(bo, BooleanOperator::Or),
			));
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
		kernels::fulltext::combine(&qt.docs, bo)
	}

	pub async fn get_doc_id(&self, tx: &Transaction, rid: &RecordId) -> Result<Option<DocId>> {
		if rid.table != *self.ikb.table() {
			return Ok(None);
		}
		self.doc_ids.get_doc_id(&self.ikb, tx, &rid.key).await
	}

	/// Whether document `doc_id` matches `qt` under `op` as `tx` holds this
	/// index now, the changes `tx` has buffered since reading the terms
	/// included ([`QueryTerms::contains_doc_now`]).
	pub fn contains_doc_now(
		&self,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
		op: BooleanOperator,
	) -> bool {
		qt.contains_doc_now(doc_id, op, |term| self.ikb.pending_tt_of(tx, term, doc_id))
	}

	pub async fn new_scorer(&self, env: &dyn IndexEnv) -> Result<Option<Scorer>> {
		if let Some(bm25) = &self.bm25 {
			let tx = env.tx();
			let (committed, deltas) =
				collect_committed_doc_length_and_count_for(&tx, &self.ikb).await?;
			// The document statistics' uncompacted deltas (`!dc`, `!dx`) the
			// scorer summed, reported apart from the terms' changes.
			Self::report_pending_read(&tx, (deltas.legacy.len() + deltas.batched.len()) as u64);
			let sc = Scorer::new(committed, self.ikb.pending_dx(&tx), *bm25);
			return Ok(Some(sc));
		}
		Ok(None)
	}

	/// Collects compacted root stats plus all visible `!dc` deltas.
	///
	/// The root and deltas share a range, so this read gives the complete
	/// statistic. Scoring reads the keyspace's part and the transaction's
	/// buffered part apart ([`Self::new_scorer`]).
	#[cfg(test)]
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

	#[cfg(test)]
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
	/// transaction.
	///
	/// A plan whose document-statistics or term-postings generation no longer
	/// matches reports [`CompactionApply::LostRace`], and the caller must discard
	/// the transaction: it can already hold the other generation's advance.
	pub(crate) async fn apply_compaction(
		&self,
		tx: &Transaction,
		plan: FullTextCompactionPlan,
	) -> Result<CompactionApply> {
		if !plan.has_work() {
			return Ok(CompactionApply::Empty);
		}
		if !self.reserve_doc_length_and_count_compaction_generation(tx, &plan.doc_lengths).await? {
			return Ok(CompactionApply::LostRace);
		}
		if !self.reserve_term_docs_compaction_generation(tx, &plan.term_docs).await? {
			return Ok(CompactionApply::LostRace);
		}
		if plan.doc_lengths.has_logs() {
			self.write_doc_length_and_count_compaction(tx, plan.doc_lengths).await?;
		}
		if plan.term_docs.has_logs() {
			self.write_term_docs_compaction(tx, plan.term_docs).await?;
		}
		Ok(CompactionApply::Applied)
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

	/// Each of the query's terms that document `doc_id` carries, in query
	/// order, with its offsets in the document.
	///
	/// The hits are those of the union of the terms of `qts`: each term once,
	/// however many of the queries name it, including a term of a word the
	/// document carries only part of. A document carrying none of the terms is
	/// answered without reading its postings: as `tx` holds the index now for
	/// terms read to judge documents ([`QueryTerms::carries_any_term_now`]), so a
	/// document `tx` wrote after reading them is located as it was written, and
	/// by the terms' document sets for any other terms.
	async fn term_hits(
		&self,
		tx: &Transaction,
		doc_id: DocId,
		qts: &[&QueryTerms],
	) -> Result<Vec<kernels::fulltext::TermHit>> {
		if !qts.iter().any(|qt| {
			qt.carries_any_term_now(doc_id, |term| self.ikb.pending_tt_of(tx, term, doc_id))
		}) {
			return Ok(Vec::new());
		}
		let mut hits = Vec::new();
		let mut loaded = self.get_document_terms(tx, doc_id).await?;
		let mut seen = HashSet::new();
		for qt in qts {
			for tk in qt.tokens.list() {
				let term = qt.tokens.get_token_string(tk)?;
				if !seen.insert(term) {
					continue;
				}
				if let Some(td) = self.take_term_document(tx, doc_id, &mut loaded, term).await? {
					hits.push(kernels::fulltext::TermHit {
						char_len: tk.get_char_len(),
						offsets: td.o,
					});
				}
			}
		}
		Ok(hits)
	}

	/// Highlights the occurrences of the terms of `qts` in the document `doc_id`,
	/// as this index numbers it, whose value is `doc`, formatted as `hlp` asks.
	/// `qts` are the queries whose terms are highlighted together; a term that
	/// several of them name is highlighted once.
	pub async fn highlight_doc(
		&self,
		tx: &Transaction,
		doc_id: DocId,
		qts: &[&QueryTerms],
		hlp: HighlightParams,
		idiom: &Idiom,
		doc: &Value,
	) -> Result<Value> {
		let hits = self.term_hits(tx, doc_id, qts).await?;
		kernels::fulltext::highlight(&hlp, idiom, doc, &hits)
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

	/// The offsets of the terms of `qts` in the document `doc_id`, as this index
	/// numbers it, unioned as [`Self::highlight_doc`] does.
	pub async fn read_offsets_doc(
		&self,
		tx: &Transaction,
		doc_id: DocId,
		qts: &[&QueryTerms],
		partial: bool,
	) -> Result<Value> {
		let hits = self.term_hits(tx, doc_id, qts).await?;
		Ok(kernels::fulltext::offsets(partial, &hits))
	}
}

/// Iterator for full-text search hits that implements the MatchesHitsIterator
/// trait
pub struct FullTextHitsIterator {
	/// The index key base used for key generation
	ikb: IndexKeyBase,
	/// The doc-ID mapping this index was created with
	doc_ids: FullTextDocIds,
	/// The document IDs in the search results.
	hits: RoaringTreemap,
	/// The lowest doc id not yet taken or passed over; `None` once the hits
	/// have run out.
	next: Option<DocId>,
	/// Where [`Self::take_doc_ids_within`] stands in the ranges of the bounds
	/// it is given: the range the last hit it looked at falls in, and the end
	/// of the ranges from there on known to hold no bound that beats the
	/// threshold.
	ranges: BoundsCursor,
	/// Whether the hits are those of an `OR` of the query's terms, any of which
	/// a hit may carry alone.
	or: bool,
	/// How many of the terms the hits are no longer listed for alone
	/// ([`BlockBounds::passed_terms`]).
	passed_terms: usize,
}

/// A position in a [`BlockBounds`]'s ranges, moved forward by hits taken in
/// ascending doc-id order.
#[derive(Default)]
struct BoundsCursor {
	/// The range the last hit looked at falls in.
	at: usize,
	/// The first range at or after `at` whose bound may beat the threshold, as
	/// far as known: the ranges from `at` up to it beat none, and as the
	/// threshold only tightens, never will.
	beats_from: usize,
}

impl FullTextHitsIterator {
	/// Creates a new iterator for full-text search hits
	///
	/// This method initializes an iterator with the index key base and a bitmap
	/// of matching document IDs.
	fn new(ikb: IndexKeyBase, doc_ids: FullTextDocIds, hits: RoaringTreemap, or: bool) -> Self {
		Self {
			ikb,
			doc_ids,
			next: hits.min(),
			hits,
			ranges: BoundsCursor::default(),
			or,
			passed_terms: 0,
		}
	}

	/// The hits not yet taken, from the lowest.
	fn remaining(&self) -> Option<roaring::treemap::Iter<'_>> {
		let next = self.next?;
		let mut iter = self.hits.iter();
		iter.advance_to(next);
		Some(iter)
	}
}

impl FullTextHitsIterator {
	/// The doc ids of up to `limit` hits not yet taken, in doc-id order; empty
	/// once the hits have run out.
	pub fn take_doc_ids(&mut self, limit: usize) -> Vec<DocId> {
		if limit == 0 {
			return Vec::new();
		}
		let Some(iter) = self.remaining() else {
			return Vec::new();
		};
		let taken: Vec<DocId> = iter.take(limit).collect();
		self.next = if taken.len() < limit {
			None
		} else {
			taken.last().and_then(|last| last.checked_add(1))
		};
		taken
	}

	/// The doc ids of up to `limit` hits not yet taken whose score under
	/// `terms` may beat a threshold, in doc-id order, with how many hits were
	/// passed over on the way; no doc id once the hits have run out.
	///
	/// A hit `terms` hold uncompacted state for ([`QueryTerms::is_uncompacted`])
	/// is always taken, and so is one before every range of `bounds`, the
	/// bounds of `terms` ([`QueryTerms::block_bounds`]). Any other hit falls in
	/// a range whose bound is at or above its score, so a hit whose range's
	/// bound `beats` does not accept cannot beat the threshold. Such a range,
	/// and every range after it that `beats` does not accept either, is passed
	/// over whole, but for the uncompacted hits in it, which are taken in
	/// their place in doc-id order.
	///
	/// The hits of an `OR` of terms are also cut, once and for good, to those
	/// carrying a term other than those whose ceilings together cannot beat
	/// the threshold ([`BlockBounds::passed_terms`]), and those `terms` hold
	/// uncompacted state for; the hits cut count as passed over.
	///
	/// The threshold `beats` tests a bound against must only tighten from one
	/// call to the next: a range passed over, or known to beat nothing, is not
	/// looked at again.
	pub fn take_doc_ids_within(
		&mut self,
		limit: usize,
		terms: &QueryTerms,
		bounds: &BlockBounds,
		beats: impl Fn(f32) -> bool,
	) -> (Vec<DocId>, u64) {
		let mut taken = Vec::with_capacity(limit.min(1024));
		let mut passed = 0;
		if limit == 0 {
			return (taken, passed);
		}
		// Of an `OR` of terms, a hit carrying only terms whose ceilings together
		// cannot beat the threshold cannot either, unless the terms hold
		// uncompacted state for it: the hits are cut to those carrying another
		// term, or uncompacted.
		if self.or && bounds.terms.len() > 1 {
			let passed_terms = bounds.passed_terms(&beats);
			let count = passed_terms.iter().filter(|passed| **passed).count();
			if count > self.passed_terms {
				let mut kept = terms.uncompacted.clone();
				for ((at, _), passed) in bounds.terms.iter().zip(&passed_terms) {
					if let (false, Some(docs)) = (passed, terms.docs_of(&terms.scored[*at])) {
						kept |= docs;
					}
				}
				passed += self.retain(&kept);
				self.passed_terms = count;
			}
		}
		let Some(next) = self.next else {
			return (taken, passed);
		};
		let mut iter = self.hits.iter();
		iter.advance_to(next);
		while taken.len() < limit {
			let Some(doc_id) = iter.next() else {
				self.next = None;
				return (taken, passed);
			};
			if terms.is_uncompacted(doc_id) {
				taken.push(doc_id);
				continue;
			}
			let Some(at) = bounds.range_at(&mut self.ranges, doc_id) else {
				taken.push(doc_id);
				continue;
			};
			let beats_from = bounds.beats_from(&mut self.ranges, at, &beats);
			if beats_from == at {
				taken.push(doc_id);
				continue;
			}
			// No hit from here up to the start of the next range that may beat the
			// threshold can, but for an uncompacted one: the scan resumes at the
			// first of either.
			let end = bounds.starts.get(beats_from).copied();
			let uncompacted = doc_id.checked_add(1).and_then(|after| {
				let mut uncompacted = terms.uncompacted.iter();
				uncompacted.advance_to(after);
				uncompacted.next()
			});
			let resume = match (end, uncompacted) {
				(Some(end), Some(uncompacted)) => Some(end.min(uncompacted)),
				(end, uncompacted) => end.or(uncompacted),
			};
			match resume {
				Some(resume) => {
					passed += self.hits.range_cardinality(doc_id..resume);
					iter.advance_to(resume);
				}
				None => {
					passed += self.hits.range_cardinality(doc_id..);
					self.next = None;
					return (taken, passed);
				}
			}
		}
		self.next = taken.last().and_then(|last| last.checked_add(1));
		(taken, passed)
	}

	/// Keeps of the hits not yet taken only those `kept` holds, and answers
	/// how many were dropped.
	pub fn retain(&mut self, kept: &RoaringTreemap) -> u64 {
		let Some(next) = self.next else {
			return 0;
		};
		let before = self.hits.range_cardinality(next..);
		self.hits &= kept;
		let after = self.hits.range_cardinality(next..);
		if after == 0 {
			self.next = None;
		}
		before - after
	}

	/// The record each of `doc_ids` maps to, in order, in one batched read;
	/// `None` for a doc id whose mapping is gone.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::idx::fulltext", skip_all, fields(docs = doc_ids.len()))
	)]
	pub async fn records(
		&self,
		tx: &Transaction,
		doc_ids: &[DocId],
	) -> Result<Vec<Option<RecordId>>> {
		let keys = self.doc_ids.get_record_ids(&self.ikb, tx, doc_ids).await?;
		Ok(keys
			.into_iter()
			.map(|key| {
				key.map(|key| RecordId {
					table: self.ikb.table().clone(),
					key,
				})
			})
			.collect())
	}
}

impl MatchesHitsIterator for FullTextHitsIterator {
	fn len(&self) -> usize {
		self.next.map_or(0, |next| {
			usize::try_from(self.hits.range_cardinality(next..)).unwrap_or(usize::MAX)
		})
	}

	/// Returns the next search hit in the iterator
	///
	/// This method retrieves the next document ID from the bitmap and resolves
	/// it to a Thing. It returns None when there are no more hits.
	async fn next(&mut self, tx: &Transaction) -> Result<Option<(RecordId, DocId)>> {
		while let Some(doc_id) = self.take_doc_ids(1).first().copied() {
			if let Some(key) = self.doc_ids.get_record_id(&self.ikb, tx, doc_id).await? {
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

/// Upper bounds on the score of a query's terms over ranges of doc ids
/// ([`QueryTerms::block_bounds`]).
pub struct BlockBounds {
	/// Where each range starts, in order.
	starts: Vec<DocId>,
	/// Each range's bound.
	bounds: Vec<f32>,
	/// Each distinct term whose blocks bound its score, in the order a score
	/// sums them: its index in [`QueryTerms::scored`], and its ceiling, the
	/// best score among the impacts of all of its blocks.
	terms: Vec<(usize, f64)>,
}

impl BlockBounds {
	/// The bound on the score of a document at `doc_id` the terms hold no
	/// uncompacted state for; `None` before every range, where no such
	/// document carries a term.
	pub fn bound(&self, doc_id: DocId) -> Option<f32> {
		let at = self.starts.partition_point(|start| *start <= doc_id);
		at.checked_sub(1).map(|at| self.bounds[at])
	}

	/// The range `doc_id` falls in, found from `cursor`, which it leaves on
	/// that range; `None` before every range.
	fn range_at(&self, cursor: &mut BoundsCursor, doc_id: DocId) -> Option<usize> {
		if self.starts.get(cursor.at).is_none_or(|start| *start > doc_id) {
			// Behind the cursor, or no range at all: searched for from the start.
			let at = self.starts.partition_point(|start| *start <= doc_id).checked_sub(1)?;
			*cursor = BoundsCursor {
				at,
				beats_from: at,
			};
			return Some(at);
		}
		while self.starts.get(cursor.at + 1).is_some_and(|start| *start <= doc_id) {
			cursor.at += 1;
		}
		cursor.beats_from = cursor.beats_from.max(cursor.at);
		Some(cursor.at)
	}

	/// The highest bound of any range, at or above the score of every
	/// document the terms hold no uncompacted state for; `None` where no range
	/// is bounded, as no such document carries a term.
	pub fn ceiling(&self) -> Option<f32> {
		self.bounds.iter().copied().reduce(f32::max)
	}

	/// Which of [`Self::terms`] a document may carry alone, in their order, and
	/// still not score above a threshold `beats` tests a bound against: the
	/// most terms, taken lowest ceiling first, whose ceilings sum, as a score
	/// sums its terms in `f64` and rounded up to the `f32` it is compared in
	/// ([`round_up`]), to a bound `beats` does not accept.
	///
	/// A document the query's terms hold no uncompacted state for scores each
	/// term it carries at most that term's ceiling, and a sum in one order of
	/// fewer non-negative values, each no larger, is no larger: carrying none
	/// but these terms, it cannot beat the threshold.
	fn passed_terms(&self, beats: impl Fn(f32) -> bool) -> Vec<bool> {
		let mut order: Vec<usize> = (0..self.terms.len()).collect();
		order.sort_by(|a, b| self.terms[*a].1.total_cmp(&self.terms[*b].1));
		let mut passed = vec![false; self.terms.len()];
		for at in order {
			passed[at] = true;
			let sum = self
				.terms
				.iter()
				.zip(&passed)
				.filter(|(_, passed)| **passed)
				.fold(0.0f64, |sum, ((_, ceiling), _)| sum + ceiling);
			if beats(round_up(sum)) {
				passed[at] = false;
				break;
			}
		}
		passed
	}

	/// The first range at or after range `at`, the one `cursor` stands on,
	/// whose bound `beats` accepts; the number of ranges where none does.
	fn beats_from(
		&self,
		cursor: &mut BoundsCursor,
		at: usize,
		beats: impl Fn(f32) -> bool,
	) -> usize {
		let mut from = cursor.beats_from.max(at);
		while self.bounds.get(from).is_some_and(|bound| !beats(*bound)) {
			from += 1;
		}
		cursor.beats_from = from;
		from
	}
}

/// `bound` as the `f32` a score is compared in, at or above every score it
/// bounds.
///
/// A score is summed in `f64` and narrowed to the nearest `f32`, and narrowing
/// to the nearest never orders two values the other way, so a bound at or
/// above a score in `f64` stays at or above it once both are narrowed. The
/// bound is first raised by a few units of `f64` rounding, so a logarithm the
/// platform rounds differently for two near inputs cannot leave a dominated
/// document's score above it. A document whose own frequency and length set
/// the bound computes the same `f64` and so mostly narrows to the same `f32`,
/// which a single-key order's tie then passes over.
fn round_up(bound: f64) -> f32 {
	(bound * (1.0 + 8.0 * f64::EPSILON)) as f32
}

/// Implements BM25 scoring for relevance ranking of search results
pub struct Scorer {
	/// The statistics the scorer was built with: `committed` and `pending`
	/// together.
	bm25: kernels::fulltext::Bm25,
	params: Bm25Params,
	/// The index's document statistics as the keyspace held them, which the
	/// reading transaction does not change before it commits.
	committed: DocLengthAndCount,
	/// The reading transaction's own buffered contribution to the statistics
	/// when the scorer was built.
	pending: DocLengthAndCount,
}

impl Scorer {
	/// A scorer with the BM25 parameters `params`, weighing documents against
	/// the statistics `committed` with the reading transaction's buffered
	/// contribution `pending` added.
	fn new(committed: DocLengthAndCount, pending: DocLengthAndCount, params: Bm25Params) -> Self {
		Self {
			bm25: Self::bm25_of(params, committed, pending),
			params,
			committed,
			pending,
		}
	}

	/// The BM25 statistics of the index whose keyspace holds `committed` and
	/// to which the reading transaction has buffered `pending`.
	fn bm25_of(
		params: Bm25Params,
		committed: DocLengthAndCount,
		pending: DocLengthAndCount,
	) -> kernels::fulltext::Bm25 {
		kernels::fulltext::Bm25::new(
			params.k1,
			params.b,
			committed.doc_count + pending.doc_count,
			committed.total_docs_length + pending.total_docs_length,
		)
	}

	/// The statistics as `tx` holds the index now: the ones the scorer was
	/// built with, with `tx`'s buffered contribution as it stands now in place
	/// of the one it held then. The keyspace's part is the scorer's own, as `tx`
	/// writes none of it before it commits.
	fn bm25_now(&self, fti: &FullTextIndex, tx: &Transaction) -> kernels::fulltext::Bm25 {
		let pending = fti.ikb.pending_dx(tx);
		if pending == self.pending {
			self.bm25
		} else {
			Self::bm25_of(self.params, self.committed, pending)
		}
	}

	/// Calculates the overall score for a document based on query terms
	///
	/// This method computes the sum of BM25 scores for all matching terms in
	/// the document. The score represents the relevance of the document to the
	/// query.
	///
	/// A document carrying none of the terms scores zero, answered without
	/// reading its length or postings: a row another clause of an `OR`
	/// produced reaches this clause's score that way.
	///
	/// Terms read to judge documents while `tx` writes
	/// ([`FullTextIndex::extract_judging_terms`]) score the document as `tx`
	/// holds the index now ([`Self::score_now`]); any other terms, as the index
	/// stood when they and the scorer were read.
	pub async fn score(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Score> {
		if qt.judging() {
			return self.score_now(fti, tx, qt, doc_id).await;
		}
		if !qt.carries_any(doc_id) {
			return Ok(0.0);
		}
		let idfs = self.idfs(qt);
		let mut cursors = vec![0; qt.scored.len()];
		if let Some(score) = self.score_from_postings(qt, &idfs, &mut cursors, doc_id) {
			return Ok(score);
		}
		let documents = Self::stored_documents(fti, tx, qt, &[doc_id]).await?;
		let document = documents
			.first()
			.ok_or_else(|| anyhow::anyhow!("a document the read did not answer"))?;
		self.score_loaded(fti, tx, qt, &idfs, doc_id, document.length, document.terms.as_ref())
			.await
	}

	/// [`Self::score`] of document `doc_id` against terms read to judge
	/// documents, as `tx` holds the index now, its own writes since the terms
	/// and the scorer were read included: the terms the document carries
	/// ([`QueryTerms::carried_now`]), how many documents carry each of them
	/// ([`QueryTerms::docs_now`]), the index's document count and total length
	/// ([`Self::bm25_now`]), and the document's stored length and postings. The
	/// score is the one terms and a scorer read afresh give the document, bit
	/// for bit, without reading the terms again.
	///
	/// The document is scored from what it stores, never from a posting block,
	/// which `tx` may have rewritten it since.
	async fn score_now(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Score> {
		let carried = qt.carried_now(doc_id, |term| fti.ikb.pending_tt_of(tx, term, doc_id));
		if !qt.words_carried(&carried).any(|carried| carried) {
			return Ok(0.0);
		}
		let bm25 = self.bm25_now(fti, tx);
		let docs = qt.docs_now(&carried, |term| fti.pending_term_changes(tx, term));
		let idfs: Vec<f64> =
			docs.iter().map(|docs| docs.map_or(0.0, |docs| bm25.idf(docs as f64))).collect();
		let documents = Self::stored_documents(fti, tx, qt, &[doc_id]).await?;
		let document = documents
			.first()
			.ok_or_else(|| anyhow::anyhow!("a document the read did not answer"))?;
		Self::score_stored(
			&bm25,
			fti,
			tx,
			qt,
			&idfs,
			|at| docs[at],
			doc_id,
			document.length,
			document.terms.as_ref(),
		)
		.await
	}

	/// The inverse document frequency of each distinct term of the query, in
	/// the order of [`QueryTerms::scored`]; zero for a term no document
	/// carries, which no score takes.
	fn idfs(&self, qt: &QueryTerms) -> Vec<f64> {
		qt.scored
			.iter()
			.map(|term| qt.docs_of(term).map_or(0.0, |docs| self.bm25.idf(docs.len() as f64)))
			.collect()
	}

	/// [`Self::score`] of each of `doc_ids`, in input order, reading the
	/// lengths and postings of the documents that carry a term in two batched
	/// reads rather than two point reads per document, or from the node's
	/// cache where it may serve them.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::idx::fulltext", skip_all, fields(docs = doc_ids.len()))
	)]
	pub async fn score_batch(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_ids: &[DocId],
	) -> Result<Vec<Score>> {
		// Terms read to judge documents score each as `tx` holds it now, one at
		// a time.
		if qt.judging() {
			let mut scores = Vec::with_capacity(doc_ids.len());
			for doc_id in doc_ids {
				scores.push(self.score_now(fti, tx, qt, *doc_id).await?);
			}
			return Ok(scores);
		}
		let idfs = self.idfs(qt);
		// A cursor per term into its postings, moved forward by doc ids that
		// ascend, as a scan lists them.
		let mut cursors = vec![0; qt.scored.len()];
		let mut scores = vec![0.0; doc_ids.len()];
		// The positions in `doc_ids` of the documents scored from what they store.
		let mut stored = Vec::new();
		// A document carrying none of the query's words scores zero, as
		// [`Self::score`] answers it, whatever terms of a word it carries alone.
		for (at, doc_id) in doc_ids.iter().enumerate().filter(|(_, d)| qt.carries_any(**d)) {
			match self.score_from_postings(qt, &idfs, &mut cursors, *doc_id) {
				Some(score) => scores[at] = score,
				None => stored.push(at),
			}
		}
		if stored.is_empty() {
			return Ok(scores);
		}
		let carriers: Vec<DocId> = stored.iter().map(|at| doc_ids[*at]).collect();
		let documents = Self::stored_documents(fti, tx, qt, &carriers).await?;
		for ((at, doc_id), document) in stored.into_iter().zip(carriers).zip(documents) {
			scores[at] = self
				.score_loaded(fti, tx, qt, &idfs, doc_id, document.length, document.terms.as_ref())
				.await?;
		}
		Ok(scores)
	}

	/// The length and postings of each of `doc_ids`, in order: from the node's
	/// cache where the terms' scope lets it serve the document
	/// ([`QueryTerms::document_scope`]), and otherwise read, the lengths and
	/// the postings of the misses in one batched read each, and cached where
	/// the scope lets them be.
	async fn stored_documents(
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_ids: &[DocId],
	) -> Result<Vec<Arc<crate::ft::cache::StoredDocument>>> {
		let mut documents: Vec<Option<Arc<crate::ft::cache::StoredDocument>>> = doc_ids
			.iter()
			.map(|doc_id| {
				qt.document_scope(*doc_id).and_then(|scope| fti.cache.document(scope, *doc_id))
			})
			.collect();
		let missing: Vec<usize> =
			(0..doc_ids.len()).filter(|at| documents[*at].is_none()).collect();
		if !missing.is_empty() {
			let lengths: Vec<Option<DocLength>> = tx
				.get_many_key(missing.iter().map(|at| fti.ikb.new_dl(doc_ids[*at])).collect(), None)
				.await?;
			let postings: Vec<Option<DocumentTerms>> = tx
				.get_many_key(missing.iter().map(|at| fti.ikb.new_dt(doc_ids[*at])).collect(), None)
				.await?;
			for ((at, length), terms) in missing.into_iter().zip(lengths).zip(postings) {
				let document = Arc::new(crate::ft::cache::StoredDocument {
					length,
					terms,
				});
				if let Some(scope) = qt.document_scope(doc_ids[at]) {
					fti.cache.insert_document(scope, doc_ids[at], Arc::clone(&document));
				}
				documents[at] = Some(document);
			}
		}
		Ok(documents.into_iter().flatten().collect())
	}

	/// The score of document `doc_id` from the posting blocks of the terms
	/// that carry it, or `None` where they cannot answer it alone: the terms
	/// hold uncompacted state for the document, or one of the terms carrying it
	/// keeps no block entry for it with a frequency.
	///
	/// A block entry holds the frequency and length the document's postings
	/// (`!dt`) and length (`!dl`) held when compaction placed it, and a
	/// document rewritten since holds uncompacted state for every term it
	/// carries, so the score equals [`Self::score_loaded`]'s, term for term and
	/// in the same order.
	///
	/// A document the terms hold no uncompacted state for carries a term
	/// exactly where the term's blocks hold an entry for it, so the entry is
	/// all a term is asked for: through `cursors`, one per term of
	/// [`QueryTerms::scored`], which calls for doc ids in ascending order move
	/// forward. `idfs` holds each such term's idf ([`Self::idfs`]). A document
	/// of an index whose terms keep no blocks, or that a statement scores from
	/// what it stores ([`QueryTerms::scored_as_stored`]), must carry a term.
	fn score_from_postings(
		&self,
		qt: &QueryTerms,
		idfs: &[f64],
		cursors: &mut [usize],
		doc_id: DocId,
	) -> Option<Score> {
		if qt.bitmaps || qt.uncompacted.contains(doc_id) {
			return None;
		}
		let mut sc = 0.0;
		for ((term, idf), cursor) in qt.scored.iter().zip(idfs).zip(cursors) {
			let Some((frequency, length)) =
				term.postings.as_ref().and_then(|postings| postings.entry_at(cursor, doc_id))
			else {
				continue;
			};
			if frequency == 0 {
				return None;
			}
			sc += self.bm25.score_with_idf(*idf, frequency, length as f64);
		}
		Some(sc as f32)
	}

	/// The score of document `doc_id`, of length `doc_length`, from its
	/// postings entry `loaded`; `None` for a document whose postings predate
	/// the per-document entry, which are read per term. `idfs` holds the idf
	/// of each distinct term ([`Self::idfs`]).
	///
	/// Each distinct term of the query is read once, however many of its words
	/// name it.
	#[allow(clippy::too_many_arguments)]
	async fn score_loaded(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		idfs: &[f64],
		doc_id: DocId,
		doc_length: Option<DocLength>,
		loaded: Option<&DocumentTerms>,
	) -> Result<Score> {
		let carried = |at: usize| {
			qt.docs_of(&qt.scored[at]).filter(|docs| docs.contains(doc_id)).map(RoaringTreemap::len)
		};
		Self::score_stored(&self.bm25, fti, tx, qt, idfs, carried, doc_id, doc_length, loaded).await
	}

	/// [`Self::score_loaded`] against the statistics `bm25`, where `carried`
	/// says of each term of [`QueryTerms::scored`], by position, how many
	/// documents carry it when the document carries it, and `None` when it does
	/// not. `idfs` holds the idf of each term the document carries.
	#[allow(clippy::too_many_arguments)]
	async fn score_stored(
		bm25: &kernels::fulltext::Bm25,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		idfs: &[f64],
		carried: impl Fn(usize) -> Option<u64>,
		doc_id: DocId,
		doc_length: Option<DocLength>,
		loaded: Option<&DocumentTerms>,
	) -> Result<Score> {
		let mut sc = 0.0;
		let doc_length = doc_length.unwrap_or(0) as f64;
		let Some(doc) = loaded else {
			for (at, term) in qt.scored.iter().enumerate() {
				if let Some(docs) = carried(at) {
					let frequency = tx
						.get_key(&fti.ikb.new_td(&term.term, doc_id), None)
						.await?
						.map(|td: TermDocument| td.f);
					if let Some(frequency) = frequency {
						sc += bm25.score(frequency as f64, docs as f64, doc_length)
					}
				}
			}
			return Ok(sc as f32);
		};
		for (at, (term, idf)) in qt.scored.iter().zip(idfs).enumerate() {
			if carried(at).is_none() {
				continue;
			}
			if let Some(td) = doc.terms.get(term.term.as_str()) {
				sc += bm25.score_with_idf(*idf, td.f, doc_length);
			}
		}
		Ok(sc as f32)
	}
}

/// Scoring computed term by term, each distinct term from its own document
/// set and its own posting read, kept to check the scoring above against, bit
/// for bit.
#[cfg(test)]
impl Scorer {
	fn reference_score_from_postings(
		&self,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Option<Score>> {
		if qt.uncompacted.contains(doc_id) {
			return Ok(None);
		}
		let mut sc = 0.0;
		for term in &qt.scored {
			if let Some(docs) = qt.docs_of(term)
				&& docs.contains(doc_id)
			{
				let Some((frequency, length)) =
					term.postings.as_ref().and_then(|p| p.entry(doc_id))
				else {
					return Ok(None);
				};
				if frequency == 0 {
					return Ok(None);
				}
				sc += self.bm25.score(frequency as f64, docs.len() as f64, length as f64);
			}
		}
		Ok(Some(sc as f32))
	}

	async fn reference_score_loaded(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
		doc_length: Option<DocLength>,
		loaded: Option<&DocumentTerms>,
	) -> Result<Score> {
		let mut sc = 0.0;
		let doc_length = doc_length.unwrap_or(0) as f64;
		for term in &qt.scored {
			if let Some(docs) = qt.docs_of(term)
				&& docs.contains(doc_id)
			{
				let term = term.term.as_str();
				let frequency = match loaded {
					Some(doc) => doc.terms.get(term).map(|td| td.f),
					None => tx
						.get_key(&fti.ikb.new_td(term, doc_id), None)
						.await?
						.map(|td: TermDocument| td.f),
				};
				if let Some(frequency) = frequency {
					sc += self.bm25.score(frequency as f64, docs.len() as f64, doc_length)
				}
			}
		}
		Ok(sc as f32)
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
	// The keyspace's statistic, then this transaction's own buffered
	// contribution, which is not written until commit.
	let (mut dlc, deltas) = collect_committed_doc_length_and_count_for(tx, ikb).await?;
	let pending = ikb.pending_dx(tx);
	dlc.doc_count += pending.doc_count;
	dlc.total_docs_length += pending.total_docs_length;
	Ok((dlc, deltas))
}

/// [`collect_doc_length_and_count_for`] without the transaction's own buffered
/// contribution: what the keyspace holds, which a transaction does not change
/// before it commits.
async fn collect_committed_doc_length_and_count_for(
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
	// The batched deltas an up-to-date server writes.
	for (k, st) in tx.getr(ikb.new_dx_range()?, None).await? {
		dlc.doc_count += st.doc_count;
		dlc.total_docs_length += st.total_docs_length;
		deltas.batched.push(DocStatsBatchKey::decode_key(&k)?.into_owned());
	}
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

	use roaring::RoaringTreemap;
	use surrealdb_datastore::Transaction;
	use surrealdb_kvs::TransactionType;
	use surrealdb_strand::Strand;
	use test_log::test;
	use tokio::time::sleep;
	use uuid::Uuid;

	use super::{
		DocId, DocumentTerms, FullTextIndex, Scorer, TERM_FOLD_PREFETCH, TermChangeBatchKey,
		TermChangeKey, TermDocsResidual, TermDocument,
	};
	use crate::catalog::{AnalyzerDefinition, DatabaseId, FullTextParams, IndexId, NamespaceId};
	use crate::expr::Tokenizer;
	use crate::expr::operator::BooleanOperator;
	#[cfg(feature = "cjk-embed-korean")]
	use crate::expr::tokenizer::SegmentLanguage;
	use crate::ft::analyzer::{AnalyzerFunction, BoxAnalyzerFut};
	use crate::ft::offset::Offset;
	use crate::index::IndexOperation;
	use crate::key::schema::DocStatsKey;
	use crate::test_env::{TestIndexEnv, TestIndexStore};
	use crate::val::{Array, RecordId, Value};
	use crate::{CompactionApply, IndexKeyBase};

	/// Stands in for the evaluator behind an analyzer's `FUNCTION` clause. The
	/// analyzer below declares none, so the seam is never reached.
	struct NoAnalyzerFunction;

	impl AnalyzerFunction for NoAnalyzerFunction {
		fn call<'a>(&'a self, name: &'a str, _input: Strand) -> BoxAnalyzerFut<'a> {
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
			Self::with_analyzer(highlight, vec![Tokenizer::Blank]).await
		}

		/// As [`Self::with_highlighting`], with the analyzer's tokenizers chosen
		/// by the caller. The default corpus is still what `index_doc` writes;
		/// a test wanting different text passes it to `index_batch_in_one_tx`.
		async fn with_analyzer(highlight: bool, tokenizers: Vec<Tokenizer>) -> Self {
			Self::build(highlight, tokenizers, crate::catalog::DOC_IDS_FORMAT_VERSION).await
		}

		/// An index stamped with `format_version`, without highlights.
		async fn at_format(format_version: u16) -> Self {
			Self::build(false, vec![Tokenizer::Blank], format_version).await
		}

		async fn build(highlight: bool, tokenizers: Vec<Tokenizer>, format_version: u16) -> Self {
			Self::build_filtered(highlight, tokenizers, None, format_version).await
		}

		/// As [`Self::build`], with the analyzer's filters chosen by the caller.
		async fn build_filtered(
			highlight: bool,
			tokenizers: Vec<Tokenizer>,
			filters: Option<Vec<crate::expr::Filter>>,
			format_version: u16,
		) -> Self {
			let ds = TestIndexStore::new().await;
			let tx = ds.env(TransactionType::Write).await.tx();
			// `DEFINE ANALYZER test TOKENIZERS blank`, as the definition the
			// index consumes: lowering the statement to it is the evaluator's
			// job and nothing here exercises that lowering.
			let az = Arc::new(AnalyzerDefinition {
				name: "test".into(),
				function: None,
				tokenizers: Some(tokenizers),
				filters,
				comment: None,
			});
			let az_key = crate::key::schema::AnalyzerKey {
				ns: NamespaceId(1),
				db: DatabaseId(2),
				az: std::borrow::Cow::Borrowed(az.name.as_str()),
			};
			tx.set_key(&az_key, az.as_ref()).await.unwrap();
			let default_content = Value::from(Array::from(vec![
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
			let content = Arc::new(default_content);
			let ft_params = Arc::new(FullTextParams {
				analyzer: az.name.clone(),
				scoring: Default::default(),
				highlight,
			});
			let nid = Uuid::new_v4();
			let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(3));
			let fti = Arc::new(
				FullTextIndex::new(
					ds.index_stores(),
					tx.as_ref(),
					ikb.clone(),
					&ft_params,
					&[],
					format_version,
				)
				.await
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

		async fn remove_insert_task(&self, rid: &RecordId) {
			let ctx = self.ds.env(TransactionType::Write).await;
			let tx = ctx.tx();

			let mut require_compaction = false;
			let az_fn = NoAnalyzerFunction;
			self.fti
				.remove_content(
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
		/// Resolves a query against the index, returning whether `rid`'s document
		/// is among the hits.
		///
		/// Gated with its only caller: the segmentation test is the one place
		/// that asks whether a query *retrieves* a document rather than what
		/// terms it produced, and without that feature this would be dead code.
		#[cfg(feature = "cjk-embed-korean")]
		async fn query_finds(&self, query: &str, rid: &RecordId) -> bool {
			let (ctx, tx) = self.new_read_env().await;
			let az_fn = NoAnalyzerFunction;
			let qt =
				self.fti.extract_querying_terms(&ctx, &az_fn, query.to_string()).await.unwrap();
			let doc_id = self.fti.get_doc_id(&tx, rid).await.unwrap().unwrap();
			let found = qt.contains_doc(doc_id, crate::expr::operator::BooleanOperator::Or);
			tx.cancel().await.unwrap();
			found
		}

		async fn index_batch_in_one_tx(&self, rids: &[RecordId], content: &Value) {
			let ctx = self.ds.env(TransactionType::Write).await;
			let tx = ctx.tx();
			let az_fn = NoAnalyzerFunction;
			let mut require_compaction = false;
			for rid in rids {
				self.fti
					.index_content(
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
			let batch = super::ChangeBatch {
				docs: docs.clone(),
				postings: None,
			};
			tx.set_key(&key, &batch).await.unwrap();
		}

		/// [`Self::write_tx_delta`] for an addition carrying `postings`, one per
		/// document of `docs` in its order.
		async fn write_tx_addition(
			&self,
			tx: &Transaction,
			term: &str,
			nid: u128,
			docs: &RoaringTreemap,
			postings: impl IntoIterator<Item = (u64, u64)>,
		) {
			let key = TermChangeBatchKey {
				ns: self.ikb.ns(),
				db: self.ikb.db(),
				tb: Cow::Borrowed(self.ikb.table()),
				ix: self.ikb.index(),
				term: Cow::Borrowed(term),
				nid: Uuid::from_u128(nid),
				uid: Uuid::from_u128(9),
				add: true,
			};
			let batch = super::ChangeBatch {
				docs: docs.clone(),
				postings: Some(super::ChangePostings::from_postings(postings)),
			};
			tx.set_key(&key, &batch).await.unwrap();
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
			let az_fn = NoAnalyzerFunction;
			let qt = self.fti.extract_querying_terms(&ctx, &az_fn, term.to_owned()).await.unwrap();
			tx.cancel().await.unwrap();
			qt.docs.into_iter().flatten().fold(RoaringTreemap::new(), |mut all, docs| {
				all |= docs;
				all
			})
		}
	}

	impl TestContext {
		/// Rewrites `rid` from `old` to `new` in one transaction: an index of a
		/// new document where `old` is `None`, a removal where `new` is.
		async fn rewrite(&self, rid: &RecordId, old: Option<&str>, new: Option<&str>) {
			let ctx = self.ds.env(TransactionType::Write).await;
			self.rewrite_within(&ctx, rid, old, new).await;
			ctx.tx().commit().await.unwrap();
		}

		/// Rewrites `rid` from `old` to `new` in the transaction `ctx` runs on,
		/// leaving the change uncommitted.
		async fn rewrite_within(
			&self,
			ctx: &TestIndexEnv,
			rid: &RecordId,
			old: Option<&str>,
			new: Option<&str>,
		) {
			let az_fn = NoAnalyzerFunction;
			let mut require_compaction = false;
			if let Some(old) = old {
				self.fti
					.remove_content(
						ctx,
						&az_fn,
						rid,
						vec![Value::from(old)],
						&mut require_compaction,
					)
					.await
					.unwrap();
			}
			if let Some(new) = new {
				self.fti
					.index_content(
						ctx,
						&az_fn,
						rid,
						vec![Value::from(new)],
						&mut require_compaction,
					)
					.await
					.unwrap();
			}
		}

		/// Every document's score for `query`, by record, as a read sees it.
		async fn scores(&self, query: &str, rids: &[RecordId]) -> Vec<(String, u32)> {
			let (ctx, tx) = self.new_read_env().await;
			let az_fn = NoAnalyzerFunction;
			let qt = self.fti.extract_querying_terms(&ctx, &az_fn, query.to_owned()).await.unwrap();
			let scorer = self.fti.new_scorer(&ctx).await.unwrap().expect("a BM25 index");
			let mut scored = Vec::new();
			for rid in rids {
				if let Some(doc_id) = self.fti.get_doc_id(&tx, rid).await.unwrap() {
					let score = scorer.score(&self.fti, &tx, &qt, doc_id).await.unwrap();
					if score != 0.0 {
						scored.push((format!("{rid:?}"), score.to_bits()));
					}
				}
			}
			tx.cancel().await.unwrap();
			scored
		}
	}

	/// An index keeping its postings in blocks scores every document exactly
	/// as one keeping a bitmap per term, through documents being indexed,
	/// rewritten and removed, and compaction rounds too small to fold a term
	/// whole, which leave residuals and changes behind.
	#[test(tokio::test)]
	async fn posting_blocks_score_as_the_bitmap_index() {
		let bitmaps = TestContext::at_format(crate::catalog::DOC_IDS_FORMAT_VERSION).await;
		let blocks =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let mut seed = 0x9e37_79b9_7f4a_7c15u64;
		let mut next = move |bound: u64| {
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			seed % bound
		};
		let words = ["alpha", "beta", "gamma", "delta", "omega", "zeta", "theta", "kappa"];
		let rids: Vec<RecordId> =
			(0..120).map(|i| RecordId::new("t".into(), format!("d{i}"))).collect();
		let mut held: Vec<Option<String>> = vec![None; rids.len()];
		let queries = ["alpha", "beta gamma", "omega", "kappa theta delta"];
		for step in 0..40 {
			for _ in 0..25 {
				let at = next(rids.len() as u64) as usize;
				let new = (next(5) > 0).then(|| {
					let len = 1 + next(12);
					(0..len)
						.map(|_| words[next(words.len() as u64) as usize])
						.collect::<Vec<_>>()
						.join(" ")
				});
				let old = held[at].clone();
				for test in [&bitmaps, &blocks] {
					test.rewrite(&rids[at], old.as_deref(), new.as_deref()).await;
				}
				held[at] = new;
			}
			// Rounds of a few entries leave terms captured in part.
			if step % 3 != 2 {
				for test in [&bitmaps, &blocks] {
					let tx = test.new_tx(TransactionType::Write).await;
					let plan =
						test.fti.prepare_term_docs_compaction_with_limit(&tx, 7).await.unwrap();
					test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap();
					tx.commit().await.unwrap();
				}
			} else {
				for test in [&bitmaps, &blocks] {
					test.drain_term_docs(13).await;
				}
			}
			for query in queries {
				assert_eq!(
					blocks.scores(query, &rids).await,
					bitmaps.scores(query, &rids).await,
					"step {step}, `{query}`"
				);
			}
		}
	}

	/// A round folding more terms than one read of their stored postings
	/// fetches ([`TERM_FOLD_PREFETCH`]) folds every term to the documents that
	/// hold it, in both the bitmap and the block formats: each chunk's postings
	/// are paired with that chunk's terms, and a term is answered after the round
	/// by its compacted postings alone.
	#[test(tokio::test)]
	async fn a_round_wider_than_one_prefetch_folds_every_term() {
		let words: Vec<String> =
			(0..TERM_FOLD_PREFETCH * 2 + 21).map(|i| format!("w{i}")).collect();
		let queries =
			[words[0].clone(), words[TERM_FOLD_PREFETCH].clone(), words[words.len() - 1].clone()];
		for format in [
			crate::catalog::DOC_IDS_FORMAT_VERSION,
			crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION,
		] {
			let test = TestContext::at_format(format).await;
			let rids: Vec<RecordId> =
				(0..30).map(|i| RecordId::new("t".into(), format!("d{i}"))).collect();
			// Document `d` holds every word whose index is congruent to `d` modulo
			// 3, or to 0 modulo 5, so each term has a distinct set of documents.
			let text = |d: usize, shift: usize| {
				words
					.iter()
					.enumerate()
					.filter(|(i, _)| (i + shift) % 3 == d % 3 || i % 5 == 0)
					.map(|(_, w)| w.as_str())
					.collect::<Vec<_>>()
					.join(" ")
			};
			let mut held: Vec<Option<String>> = vec![None; rids.len()];
			for (step, shift) in [0, 1].into_iter().enumerate() {
				for (d, rid) in rids.iter().enumerate() {
					let new = text(d, shift);
					test.rewrite(rid, held[d].as_deref(), Some(&new)).await;
					held[d] = Some(new);
				}
				// One round takes every term, more than two prefetch chunks of them.
				assert_eq!(test.drain_term_docs(1_000_000).await, 1);
				let tx = test.new_tx(TransactionType::Read).await;
				let mut doc_ids = Vec::new();
				for rid in &rids {
					doc_ids.push(test.fti.get_doc_id(&tx, rid).await.unwrap().unwrap());
				}
				tx.cancel().await.unwrap();
				for (i, word) in words.iter().enumerate() {
					let expected: RoaringTreemap = doc_ids
						.iter()
						.enumerate()
						.filter(|(d, _)| (i + shift) % 3 == d % 3 || i % 5 == 0)
						.map(|(_, doc_id)| *doc_id)
						.collect();
					assert_eq!(
						test.query_docs(word).await,
						expected,
						"format {format}, step {step}, `{word}`"
					);
				}
				for query in &queries {
					test.check_reference_scores(query, &rids, format, step).await;
				}
			}
		}
	}

	/// Every document's score, one at a time and in batches of doc ids ascending
	/// or not, is bit for bit the score of the reference scoring that takes
	/// each distinct term from its own document set. Over the
	/// legacy, bitmap and block formats, queries repeating a term, documents
	/// compacted, partly compacted and rewritten since, documents whose
	/// per-document postings entry is gone, and terms read as a statement that
	/// writes reads them.
	#[test(tokio::test)]
	async fn scores_are_the_reference_scores_bit_for_bit() {
		let words = ["alpha", "beta", "gamma", "delta", "omega", "zeta", "theta", "kappa"];
		let queries = [
			"alpha",
			"beta gamma",
			"alpha beta alpha",
			"kappa theta delta kappa theta",
			"omega unknown zeta",
		];
		for format in [
			0,
			crate::catalog::DOC_IDS_FORMAT_VERSION,
			crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION,
		] {
			let test = TestContext::at_format(format).await;
			let mut seed = 0x2545_f491_4f6c_dd1du64;
			let mut next = move |bound: u64| {
				seed ^= seed << 13;
				seed ^= seed >> 7;
				seed ^= seed << 17;
				seed % bound
			};
			let rids: Vec<RecordId> =
				(0..300).map(|i| RecordId::new("t".into(), format!("d{i}"))).collect();
			let mut held: Vec<Option<String>> = vec![None; rids.len()];
			for step in 0..6 {
				for _ in 0..if step == 0 {
					600
				} else {
					40
				} {
					let at = next(rids.len() as u64) as usize;
					let new = (next(6) > 0).then(|| {
						(0..1 + next(30))
							.map(|_| words[next(words.len() as u64).min(next(9)) as usize])
							.collect::<Vec<_>>()
							.join(" ")
					});
					test.rewrite(&rids[at], held[at].as_deref(), new.as_deref()).await;
					held[at] = new;
				}
				if step % 2 == 0 {
					test.drain_term_docs(1024).await;
				} else {
					let tx = test.new_tx(TransactionType::Write).await;
					let plan =
						test.fti.prepare_term_docs_compaction_with_limit(&tx, 7).await.unwrap();
					test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap();
					tx.commit().await.unwrap();
				}
				for query in queries {
					test.check_reference_scores(query, &rids, format, step).await;
				}
			}
		}
	}

	impl TestContext {
		/// Checks every document's score for `query` against the reference
		/// scoring, in a transaction that drops some documents' per-document
		/// postings entry and is cancelled after.
		async fn check_reference_scores(
			&self,
			query: &str,
			rids: &[RecordId],
			format: u16,
			step: usize,
		) {
			let ctx = self.ds.env(TransactionType::Write).await;
			let tx = ctx.tx();
			let mut doc_ids = Vec::new();
			for rid in rids {
				if let Some(doc_id) = self.fti.get_doc_id(&tx, rid).await.unwrap() {
					doc_ids.push(doc_id);
				}
			}
			doc_ids.sort_unstable();
			doc_ids.dedup();
			for doc_id in doc_ids.iter().step_by(11) {
				tx.del_key(&self.ikb.new_dt(*doc_id)).await.unwrap();
			}
			let az_fn = NoAnalyzerFunction;
			let scorer = self.fti.new_scorer(&ctx).await.unwrap().expect("a BM25 index");
			for mode in 0..3 {
				let mut qt =
					self.fti.extract_querying_terms(&ctx, &az_fn, query.to_owned()).await.unwrap();
				match mode {
					1 => qt = qt.scored_as_stored(),
					// Scored terms that are not the query's words one to one.
					2 if qt.scored.len() > 2 => {
						qt.scored.remove(1);
					}
					2 => continue,
					_ => {}
				}
				let mut expected = Vec::new();
				for doc_id in &doc_ids {
					let reference = if !qt.carries_any(*doc_id) {
						0.0
					} else if let Some(score) =
						scorer.reference_score_from_postings(&qt, *doc_id).unwrap()
					{
						score
					} else {
						let documents = Scorer::stored_documents(&self.fti, &tx, &qt, &[*doc_id])
							.await
							.unwrap();
						scorer
							.reference_score_loaded(
								&self.fti,
								&tx,
								&qt,
								*doc_id,
								documents[0].length,
								documents[0].terms.as_ref(),
							)
							.await
							.unwrap()
					};
					let score = scorer.score(&self.fti, &tx, &qt, *doc_id).await.unwrap();
					assert_eq!(
						score.to_bits(),
						reference.to_bits(),
						"format {format}, step {step}, `{query}`, mode {mode}, doc {doc_id}"
					);
					expected.push(reference.to_bits());
				}
				let batch = scorer.score_batch(&self.fti, &tx, &qt, &doc_ids).await.unwrap();
				assert_eq!(
					batch.iter().map(|score| score.to_bits()).collect::<Vec<_>>(),
					expected,
					"format {format}, step {step}, `{query}`, mode {mode}: ascending batch"
				);
				// Doc ids out of order, and repeated, score as they do in order.
				let shuffled: Vec<DocId> =
					doc_ids.iter().rev().chain(doc_ids.iter().step_by(3)).copied().collect();
				let batch = scorer.score_batch(&self.fti, &tx, &qt, &shuffled).await.unwrap();
				let expected_shuffled: Vec<u32> = shuffled
					.iter()
					.map(|doc_id| expected[doc_ids.binary_search(doc_id).unwrap()])
					.collect();
				assert_eq!(
					batch.iter().map(|score| score.to_bits()).collect::<Vec<_>>(),
					expected_shuffled,
					"format {format}, step {step}, `{query}`, mode {mode}: shuffled batch"
				);
			}
			tx.cancel().await.unwrap();
		}
	}

	/// Compaction fills a block entry from the change that placed the document,
	/// which carries its frequency and length (`!tx`), not by reading the
	/// document: an entry is right with the document's `!dt` and `!dl` gone.
	#[test(tokio::test)]
	async fn a_block_entry_is_filled_from_the_change_that_placed_it() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let rid = RecordId::new("t".into(), "d1".to_owned());
		test.rewrite(&rid, None, Some("hello hello world")).await;
		let tx = test.new_tx(TransactionType::Write).await;
		let doc_id = test.fti.get_doc_id(&tx, &rid).await.unwrap().expect("indexed");
		tx.del_key(&test.ikb.new_dt(doc_id)).await.unwrap();
		tx.del_key(&test.ikb.new_dl(doc_id)).await.unwrap();
		tx.commit().await.unwrap();
		test.drain_term_docs(1024).await;
		let tx = test.new_tx(TransactionType::Read).await;
		let listed: super::TermBlocks =
			tx.get_key(&test.ikb.new_tk("hello"), None).await.unwrap().expect("blocks");
		let start = listed.blocks.first().expect("a block").start;
		let block: super::PostingBlock =
			tx.get_key(&test.ikb.new_tp("hello", start), None).await.unwrap().expect("the block");
		assert_eq!(block.entries(), Some(vec![(doc_id, 2, 3)]));
		tx.cancel().await.unwrap();
	}

	/// A rewrite of a document the blocks already hold takes its entry from the
	/// change too: the rewritten entry is right with the document's `!dt` and
	/// `!dl` gone.
	#[test(tokio::test)]
	async fn a_rewritten_block_entry_is_filled_from_the_change() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let rid = RecordId::new("t".into(), "d1".to_owned());
		test.rewrite(&rid, None, Some("hello world")).await;
		test.drain_term_docs(1024).await;
		test.rewrite(&rid, Some("hello world"), Some("hello hello there")).await;
		let tx = test.new_tx(TransactionType::Write).await;
		let doc_id = test.fti.get_doc_id(&tx, &rid).await.unwrap().expect("indexed");
		tx.del_key(&test.ikb.new_dt(doc_id)).await.unwrap();
		tx.del_key(&test.ikb.new_dl(doc_id)).await.unwrap();
		tx.commit().await.unwrap();
		test.drain_term_docs(1024).await;
		let tx = test.new_tx(TransactionType::Read).await;
		let listed: super::TermBlocks =
			tx.get_key(&test.ikb.new_tk("hello"), None).await.unwrap().expect("blocks");
		let start = listed.blocks.first().expect("a block").start;
		let block: super::PostingBlock =
			tx.get_key(&test.ikb.new_tp("hello", start), None).await.unwrap().expect("the block");
		tx.cancel().await.unwrap();
		assert_eq!(block.entries(), Some(vec![(doc_id, 2, 3)]));
	}

	/// A change's postings pair one for one with the documents of the addition
	/// that carries them. Postings that hold another number pair with none of
	/// them: the round still folds, reading each document's entry from what it
	/// stores, and removes the change, so no later round meets it again.
	#[test(tokio::test)]
	async fn change_postings_that_miss_a_document_are_read_around() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_addition(&tx, "hello", 1, &RoaringTreemap::from_iter([3u64, 7]), [(2, 10)])
			.await;
		for (doc_id, frequency, length) in [(3u64, 4, 40), (7, 5, 50)] {
			let stored = DocumentTerms {
				terms: [(
					"hello".into(),
					TermDocument {
						f: frequency,
						o: Vec::new(),
					},
				)]
				.into_iter()
				.collect(),
			};
			tx.set_key(&test.ikb.new_dt(doc_id), &stored).await.unwrap();
			tx.set_key(&test.ikb.new_dl(doc_id), &length).await.unwrap();
		}
		tx.commit().await.unwrap();
		assert_eq!(test.drain_term_docs(1024).await, 1, "the round folds");
		let tx = test.new_tx(TransactionType::Read).await;
		let listed: super::TermBlocks =
			tx.get_key(&test.ikb.new_tk("hello"), None).await.unwrap().expect("blocks");
		let start = listed.blocks.first().expect("a block").start;
		let block: super::PostingBlock =
			tx.get_key(&test.ikb.new_tp("hello", start), None).await.unwrap().expect("the block");
		assert_eq!(block.entries(), Some(vec![(3, 4, 40), (7, 5, 50)]));
		let changes = tx.count(test.ikb.new_tx_terms_range().unwrap(), None).await.unwrap();
		tx.cancel().await.unwrap();
		assert_eq!(changes, 0, "the change was left");
	}

	#[test(tokio::test)]
	async fn changes_netted_to_nothing_before_a_split_keep_the_current_posting() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let doc_id: DocId = 5;
		let docs = RoaringTreemap::from_iter([doc_id]);
		let tx = test.new_tx(TransactionType::Write).await;
		// The oldest change: an addition on node 2.
		test.write_tx_addition(&tx, "hello", 2, &docs, [(1, 10)]).await;
		// The newer changes, on node 1: a removal, then a re-addition.
		test.write_tx_delta(&tx, "hello", 1, false, &docs).await;
		test.write_tx_addition(&tx, "hello", 1, &docs, [(3, 30)]).await;
		let current = DocumentTerms {
			terms: [(
				"hello".into(),
				TermDocument {
					f: 3,
					o: Vec::new(),
				},
			)]
			.into_iter()
			.collect(),
		};
		tx.set_key(&test.ikb.new_dt(doc_id), &current).await.unwrap();
		tx.set_key(&test.ikb.new_dl(doc_id), &30).await.unwrap();
		tx.commit().await.unwrap();
		// A round of two entries folds node 1's changes; the next folds node 2's.
		for limit in [2, 1024] {
			let tx = test.new_tx(TransactionType::Write).await;
			let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, limit).await.unwrap();
			assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
			tx.commit().await.unwrap();
		}
		let tx = test.new_tx(TransactionType::Read).await;
		let listed: super::TermBlocks =
			tx.get_key(&test.ikb.new_tk("hello"), None).await.unwrap().expect("blocks");
		let start = listed.blocks.first().expect("a block").start;
		let block: super::PostingBlock =
			tx.get_key(&test.ikb.new_tp("hello", start), None).await.unwrap().expect("the block");
		tx.cancel().await.unwrap();
		assert_eq!(block.entries(), Some(vec![(doc_id, 3, 30)]));
	}

	/// A removal and an addition that total nothing take the addition's posting
	/// only when one transaction made both, keeping the term. From two
	/// transactions, nothing orders them, and the document is read instead.
	#[test(tokio::test)]
	async fn a_removal_and_an_addition_from_two_transactions_read_the_document() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let doc_id: DocId = 5;
		let docs = RoaringTreemap::from_iter([doc_id]);
		let current = |f| DocumentTerms {
			terms: [(
				"hello".into(),
				TermDocument {
					f,
					o: Vec::new(),
				},
			)]
			.into_iter()
			.collect(),
		};
		// The document placed in the term's blocks.
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_addition(&tx, "hello", 3, &docs, [(1, 10)]).await;
		tx.commit().await.unwrap();
		test.drain_term_docs(1024).await;
		// A removal on node 1 and an addition on node 2, and the document as it
		// is now.
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_delta(&tx, "hello", 1, false, &docs).await;
		test.write_tx_addition(&tx, "hello", 2, &docs, [(2, 20)]).await;
		tx.set_key(&test.ikb.new_dt(doc_id), &current(3)).await.unwrap();
		tx.set_key(&test.ikb.new_dl(doc_id), &30).await.unwrap();
		tx.commit().await.unwrap();
		test.drain_term_docs(1024).await;
		let tx = test.new_tx(TransactionType::Read).await;
		let listed: super::TermBlocks =
			tx.get_key(&test.ikb.new_tk("hello"), None).await.unwrap().expect("blocks");
		let start = listed.blocks.first().expect("a block").start;
		let block: super::PostingBlock =
			tx.get_key(&test.ikb.new_tp("hello", start), None).await.unwrap().expect("the block");
		tx.cancel().await.unwrap();
		assert_eq!(block.entries(), Some(vec![(doc_id, 3, 30)]));
	}

	/// An addition whose postings hold a frequency of zero for a document, the
	/// transaction's last change to it having carried none, places nothing from
	/// it: the document is read.
	#[test(tokio::test)]
	async fn a_zero_posting_reads_the_document() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let doc_id: DocId = 5;
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_addition(&tx, "hello", 1, &RoaringTreemap::from_iter([doc_id]), [(0, 0)])
			.await;
		let current = DocumentTerms {
			terms: [(
				"hello".into(),
				TermDocument {
					f: 3,
					o: Vec::new(),
				},
			)]
			.into_iter()
			.collect(),
		};
		tx.set_key(&test.ikb.new_dt(doc_id), &current).await.unwrap();
		tx.set_key(&test.ikb.new_dl(doc_id), &30).await.unwrap();
		tx.commit().await.unwrap();
		test.drain_term_docs(1024).await;
		let tx = test.new_tx(TransactionType::Read).await;
		let listed: super::TermBlocks =
			tx.get_key(&test.ikb.new_tk("hello"), None).await.unwrap().expect("blocks");
		let start = listed.blocks.first().expect("a block").start;
		let block: super::PostingBlock =
			tx.get_key(&test.ikb.new_tp("hello", start), None).await.unwrap().expect("the block");
		tx.cancel().await.unwrap();
		assert_eq!(block.entries(), Some(vec![(doc_id, 3, 30)]));
	}

	/// Two additions of a document to a term, made on two nodes, sort by node
	/// rather than by when they were made, so rounds can fold the newer before
	/// the older. The entry the blocks keep is the document's current posting
	/// whatever order they are folded in.
	#[test(tokio::test)]
	async fn an_older_addition_folded_last_leaves_the_current_posting() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let doc_id: DocId = 5;
		let tx = test.new_tx(TransactionType::Write).await;
		let docs = RoaringTreemap::from_iter([doc_id]);
		// The newer addition sorts first (node 1), the older second (node 2).
		for (nid, frequency, length) in [(1u128, 3, 30), (2, 1, 10)] {
			test.write_tx_addition(&tx, "hello", nid, &docs, [(frequency, length)]).await;
		}
		let current = DocumentTerms {
			terms: [(
				"hello".into(),
				TermDocument {
					f: 3,
					o: Vec::new(),
				},
			)]
			.into_iter()
			.collect(),
		};
		tx.set_key(&test.ikb.new_dt(doc_id), &current).await.unwrap();
		tx.set_key(&test.ikb.new_dl(doc_id), &30).await.unwrap();
		tx.commit().await.unwrap();
		// A round of one entry folds the newer addition; the next takes the
		// older one with the rest of the term, leaving nothing for a later
		// round to read the document again for.
		for limit in [1, 1024] {
			let tx = test.new_tx(TransactionType::Write).await;
			let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, limit).await.unwrap();
			assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
			tx.commit().await.unwrap();
		}
		assert_eq!(test.term_residual("hello").await, None, "the term was folded whole");
		let tx = test.new_tx(TransactionType::Read).await;
		let listed: super::TermBlocks =
			tx.get_key(&test.ikb.new_tk("hello"), None).await.unwrap().expect("blocks");
		let start = listed.blocks.first().expect("a block").start;
		let block: super::PostingBlock =
			tx.get_key(&test.ikb.new_tp("hello", start), None).await.unwrap().expect("the block");
		tx.cancel().await.unwrap();
		assert_eq!(block.entries(), Some(vec![(doc_id, 3, 30)]));
	}

	async fn concurrent_doc_update(test: TestContext, rid: Arc<RecordId>, mut count: usize) {
		while count > 0 && test.start.elapsed().as_millis() < 3000 {
			test.remove_insert_task(&rid).await;
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

		test.remove_insert_task(&doc1).await;
		test.remove_insert_task(&doc2).await;

		// Verify `compute_doc_length_and_count` returns valid (non-zero) stats
		// before and after compaction.

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

		test.remove_insert_task(&doc1).await;
		test.remove_insert_task(&doc2).await;
		test.remove_insert_task(&doc3).await;

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
	/// A Korean stem finds a document that only ever contained its inflected
	/// form.
	///
	/// This is the property `SEGMENT` exists for, and the only test that pins
	/// it end to end: the tokenizer tests fix what the analyzer *produces*,
	/// while this one fixes that those tokens are what the index stores and
	/// what a query resolves against. `한국어를` is the noun `한국어` with an
	/// object particle attached, which the character-class tokenizers cannot
	/// separate — under `TOKENIZERS blank` alone the query below finds nothing.
	#[cfg(feature = "cjk-embed-korean")]
	#[test(tokio::test)]
	async fn korean_stem_retrieves_the_inflected_document() {
		let test = TestContext::with_analyzer(
			false,
			vec![Tokenizer::Blank, Tokenizer::Segment(SegmentLanguage::Korean)],
		)
		.await;
		let doc = RecordId::new("t".into(), "doc1".to_owned());
		let content = Value::from(Array::from(vec!["한국어를 배우고 있습니다"]));
		test.index_batch_in_one_tx(std::slice::from_ref(&doc), &content).await;

		let (stem, particle, absent) = (
			test.query_finds("한국어", &doc).await,
			test.query_finds("를", &doc).await,
			test.query_finds("일본어", &doc).await,
		);

		assert!(stem, "the stem `한국어` must find the document holding `한국어를`");
		assert!(particle, "the particle is indexed as its own term, so it matches too");
		assert!(!absent, "an unrelated word must not match");
	}

	/// Indexing a document leaves one term delta per distinct term and one
	/// statistics delta. A remove followed by a re-index of the same record
	/// within one transaction leaves every term's membership where it was, but
	/// still names the document as changed in each of them: it contributes a
	/// removal and an addition per term, which sum to zero. The statistics do not
	/// move at all, so it contributes no statistics entry. The earlier
	/// transaction's deltas are untouched either way.
	#[test(tokio::test)]
	async fn every_term_and_transaction_keeps_its_own_delta() {
		for highlight in [false, true] {
			let test = TestContext::with_highlighting(highlight).await;
			let doc = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));

			// A record with no doc-ID yet: the removal half writes nothing, so
			// this counts exactly what one indexing call emits.
			test.remove_insert_task(&doc).await;
			let tx = test.new_tx(TransactionType::Read).await;
			let terms = test.tt_delta_count(&tx).await;
			assert!(terms > 2, "the fixture content must yield several distinct terms");
			assert_eq!(test.dc_delta_count(&tx).await, 1, "highlight={highlight}");
			tx.cancel().await.unwrap();

			// A second transaction removing and re-indexing the same content:
			// a removal and an addition per term on top of the first
			// transaction's, and no statistics entry, because removing and
			// re-adding one document of unchanged length nets to zero.
			test.remove_insert_task(&doc).await;
			let tx = test.new_tx(TransactionType::Read).await;
			assert_eq!(test.tt_delta_count(&tx).await, terms * 3, "highlight={highlight}");
			assert_eq!(test.dc_delta_count(&tx).await, 1, "highlight={highlight}");
			tx.cancel().await.unwrap();

			// The re-index kept every term, so each still holds the document once.
			let doc_id = {
				let tx = test.new_tx(TransactionType::Read).await;
				let doc_id = test.fti.get_doc_id(&tx, &doc).await.unwrap().unwrap();
				tx.cancel().await.unwrap();
				doc_id
			};
			test.drain_term_docs(1024).await;
			let tx = test.new_tx(TransactionType::Read).await;
			assert_eq!(test.tt_delta_count(&tx).await, 0, "highlight={highlight}");
			tx.cancel().await.unwrap();
			assert!(test.term_docs("blog").await.is_some_and(|docs| docs.contains(doc_id)));
			assert_eq!(test.term_residual("blog").await, None, "highlight={highlight}");
		}
	}

	/// One transaction's removal and re-indexing of a document, as an `UPDATE`
	/// that changes the indexed text from `old` to `new` writes it.
	async fn update_in_one_tx(test: &TestContext, rid: &RecordId, old: &str, new: &str) {
		let ctx = test.ds.env(TransactionType::Write).await;
		let tx = ctx.tx();
		let az_fn = NoAnalyzerFunction;
		let mut require_compaction = false;
		test.fti
			.remove_content(&ctx, &az_fn, rid, vec![Value::from(old)], &mut require_compaction)
			.await
			.unwrap();
		test.fti
			.index_content(&ctx, &az_fn, rid, vec![Value::from(new)], &mut require_compaction)
			.await
			.unwrap();
		tx.commit().await.unwrap();
	}

	/// One transaction's removal of a document, as a `DELETE` writes it.
	async fn delete_in_one_tx(test: &TestContext, rid: &RecordId, old: &str) {
		let ctx = test.ds.env(TransactionType::Write).await;
		let tx = ctx.tx();
		let az_fn = NoAnalyzerFunction;
		let mut require_compaction = false;
		test.fti
			.remove_content(&ctx, &az_fn, rid, vec![Value::from(old)], &mut require_compaction)
			.await
			.unwrap();
		tx.commit().await.unwrap();
	}

	/// An `UPDATE` that keeps a term leaves the document carrying it once, so
	/// a later `DELETE` takes it out of every term, kept or not.
	#[test(tokio::test)]
	async fn a_document_updated_then_deleted_leaves_the_terms_it_kept() {
		let test = TestContext::with_highlighting(false).await;
		let rid = RecordId::new("t".into(), "doc1".to_owned());
		test.index_batch_in_one_tx(std::slice::from_ref(&rid), &Value::from("hello world")).await;
		test.drain_term_docs(1024).await;
		assert_eq!(test.query_docs("hello").await.len(), 1);

		update_in_one_tx(&test, &rid, "hello world", "hello there").await;
		assert_eq!(test.query_docs("hello").await.len(), 1);
		assert!(test.query_docs("world").await.is_empty());

		delete_in_one_tx(&test, &rid, "hello there").await;
		assert!(test.query_docs("hello").await.is_empty());
		assert!(test.query_docs("there").await.is_empty());
	}

	/// The same sequence, compacted: no stale membership is folded into the
	/// kept term's bitmap.
	#[test(tokio::test)]
	async fn compaction_folds_an_update_then_delete_out_of_the_terms_it_kept() {
		let test = TestContext::with_highlighting(false).await;
		let rid = RecordId::new("t".into(), "doc1".to_owned());
		test.index_batch_in_one_tx(std::slice::from_ref(&rid), &Value::from("hello world")).await;
		test.drain_term_docs(1024).await;
		update_in_one_tx(&test, &rid, "hello world", "hello there").await;
		delete_in_one_tx(&test, &rid, "hello there").await;
		test.drain_term_docs(1024).await;
		assert_eq!(test.term_docs("hello").await, None);
		assert_eq!(test.term_residual("hello").await, None);
	}

	/// A removal takes its terms from what the index stored for the document,
	/// not from the old value it is handed: a term the old value lacks is still
	/// removed, and one only the old value has is not.
	#[test(tokio::test)]
	async fn a_removal_takes_its_terms_from_the_stored_document() {
		let test = TestContext::with_highlighting(false).await;
		let rid = RecordId::new("t".into(), "doc1".to_owned());
		test.index_batch_in_one_tx(std::slice::from_ref(&rid), &Value::from("hello world")).await;
		test.drain_term_docs(1024).await;

		update_in_one_tx(&test, &rid, "hello again", "hello again").await;
		assert_eq!(test.query_docs("hello").await.len(), 1);
		assert_eq!(test.query_docs("again").await.len(), 1, "a term only the old value has");
		assert!(test.query_docs("world").await.is_empty(), "a term the old value lacks");

		test.drain_term_docs(1024).await;
		assert_eq!(test.term_docs("hello").await.map(|docs| docs.len()), Some(1));
		assert_eq!(test.term_docs("again").await.map(|docs| docs.len()), Some(1));
		assert_eq!(test.term_docs("world").await, None);
	}

	/// A record whose doc id exists in the table's shared space but which this
	/// index never indexed — another index on the table assigned the id — has
	/// nothing to remove. Its first indexing, even in the same transaction as
	/// the removal an `UPDATE` makes, gives it every term.
	#[test(tokio::test)]
	async fn a_removal_of_a_document_the_index_never_held_records_nothing() {
		let test = TestContext::with_highlighting(false).await;
		let rid = RecordId::new("t".into(), "doc1".to_owned());
		{
			let ctx = test.ds.env(TransactionType::Write).await;
			let tx = ctx.tx();
			test.fti.doc_ids.resolve_or_assign(&test.ikb, &ctx, &rid.key).await.unwrap();
			tx.commit().await.unwrap();
		}

		update_in_one_tx(&test, &rid, "hello world", "hello there").await;
		assert_eq!(test.query_docs("hello").await.len(), 1);
		assert_eq!(test.query_docs("there").await.len(), 1);
		assert!(test.query_docs("world").await.is_empty());
		let tx = test.new_tx(TransactionType::Read).await;
		assert_eq!(test.tt_delta_count(&tx).await, 2, "one addition per term, no removal");
		tx.cancel().await.unwrap();
	}

	/// A document indexed before the per-document entry existed has a length
	/// and per-term postings but no `!dt`, so its removal analyses the old value
	/// again. An update that keeps a term and a later delete leave it carrying
	/// nothing, before and after compaction.
	#[test(tokio::test)]
	async fn a_document_without_its_per_document_entry_is_updated_then_deleted() {
		let test = TestContext::with_highlighting(false).await;
		let rid = RecordId::new("t".into(), "doc1".to_owned());
		test.index_batch_in_one_tx(std::slice::from_ref(&rid), &Value::from("hello world")).await;
		test.drain_term_docs(1024).await;
		// Reshape it as an older server wrote it: per-term postings, no `!dt`.
		{
			let tx = test.new_tx(TransactionType::Write).await;
			let doc_id = test.fti.get_doc_id(&tx, &rid).await.unwrap().unwrap();
			let doc = tx.get_key(&test.ikb.new_dt(doc_id), None).await.unwrap().unwrap();
			for (term, td) in doc.terms.iter() {
				tx.set_key(&test.ikb.new_td(term, doc_id), td).await.unwrap();
			}
			tx.del_key(&test.ikb.new_dt(doc_id)).await.unwrap();
			assert!(tx.get_key(&test.ikb.new_dl(doc_id), None).await.unwrap().is_some());
			tx.commit().await.unwrap();
		}

		update_in_one_tx(&test, &rid, "hello world", "hello there").await;
		assert_eq!(test.query_docs("hello").await.len(), 1, "the kept term");
		assert_eq!(test.query_docs("there").await.len(), 1);
		assert!(test.query_docs("world").await.is_empty());

		delete_in_one_tx(&test, &rid, "hello there").await;
		assert!(test.query_docs("hello").await.is_empty());
		assert!(test.query_docs("there").await.is_empty());
		test.drain_term_docs(1024).await;
		assert_eq!(test.term_docs("hello").await, None);
		assert_eq!(test.term_residual("hello").await, None);
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn term_docs_compaction_batches_deltas() {
		let test = TestContext::new().await;
		let doc = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));

		test.remove_insert_task(&doc).await;

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
		let az_fn = NoAnalyzerFunction;
		let qt = test.fti.extract_querying_terms(&ctx, &az_fn, "Welcome".into()).await.unwrap();
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
		let az_fn = NoAnalyzerFunction;
		let qt = test.fti.extract_querying_terms(&ctx, &az_fn, "Welcome".into()).await.unwrap();
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

		let content = test.content.as_ref().clone();
		test.index_batch_in_one_tx(&docs, &content).await;

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
		test.index_batch_in_one_tx(&docs, &content).await;

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

		test.index_batch_in_one_tx(&ahead, &two_terms).await;
		test.index_batch_in_one_tx(&behind, &one_term).await;

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

		test.index_batch_in_one_tx(&narrow, &first).await;
		test.index_batch_in_one_tx(&wide, &second).await;

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

	/// A residual with no delta beside it is folded into its term's bitmap and
	/// removed, and the term answers the same documents before and after.
	///
	/// A compactor that predates the residual family strands one, so no delta of
	/// the term is left to bring a round to it: the round captures it on its own.
	#[test(tokio::test)]
	async fn a_residual_left_without_deltas_is_folded_into_its_bitmap() {
		let test = TestContext::new().await;
		let kept: DocId = 7;
		let dropped: DocId = 9;

		// One document the residual adds, and one the bitmap holds that the
		// residual takes back out.
		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_td_root("hello"), &RoaringTreemap::from_iter([dropped]))
			.await
			.unwrap();
		tx.set_key(
			&test.ikb.new_tr_root("hello"),
			&TermDocsResidual::new([(kept, 1), (dropped, -1)]),
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
		let before = test.query_docs("hello").await;
		assert_eq!(before, RoaringTreemap::from_iter([kept]));

		assert_eq!(test.drain_term_docs(64).await, 1, "one round folds the standalone residual");
		assert_eq!(test.term_docs("hello").await, Some(RoaringTreemap::from_iter([kept])));
		assert_eq!(test.term_residual("hello").await, None);
		assert_eq!(test.query_docs("hello").await, before);
	}

	/// A residual whose term has only an empty `!tx` batch beside it is folded
	/// in the round that deletes that batch.
	///
	/// The batch adds no delta to the term, so the term is folded as a
	/// standalone residual; were it counted as folded with its deltas, the
	/// round would delete the batch and leave the residual behind.
	#[test(tokio::test)]
	async fn a_residual_beside_only_an_empty_delta_batch_is_folded() {
		let test = TestContext::new().await;
		let kept: DocId = 7;

		let tx = test.new_tx(TransactionType::Write).await;
		tx.set_key(&test.ikb.new_tr_root("hello"), &TermDocsResidual::new([(kept, 1)]))
			.await
			.unwrap();
		test.write_tx_delta(&tx, "hello", 1, true, &RoaringTreemap::new()).await;
		tx.commit().await.unwrap();

		test.drain_term_docs(64).await;
		assert_eq!(test.term_residual("hello").await, None, "the residual is folded");
		assert_eq!(test.term_docs("hello").await, Some(RoaringTreemap::from_iter([kept])));
		assert_eq!(test.query_docs("hello").await, RoaringTreemap::from_iter([kept]));
	}

	/// A residual is folded on its own only once both delta scans have passed
	/// its term, so a term whose deltas a round stopped short of keeps its
	/// residual for the round that reaches them.
	#[test(tokio::test)]
	async fn a_residual_past_the_rounds_cutoff_waits_for_its_deltas() {
		let test = TestContext::new().await;
		let doc: DocId = 7;

		// Two terms of deltas, a budget of one entry, and a residual on the term
		// sorting after the one the round stops on.
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_delta(&tx, "alpha", 1, true, &RoaringTreemap::from_iter([1])).await;
		test.write_tx_delta(&tx, "omega", 2, true, &RoaringTreemap::from_iter([doc])).await;
		tx.set_key(&test.ikb.new_tr_root("omega"), &TermDocsResidual::new([(doc, -1)]))
			.await
			.unwrap();
		tx.commit().await.unwrap();
		let before = test.query_docs("omega").await;

		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 1).await.unwrap();
		assert!(plan.has_more());
		assert!(plan.standalone_residuals.is_empty(), "omega's deltas were not captured");
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();
		assert_eq!(test.term_residual("omega").await, Some(vec![(doc, -1)]));

		test.drain_term_docs(1).await;
		assert_eq!(test.term_residual("omega").await, None);
		assert_eq!(test.term_docs("omega").await, None);
		assert_eq!(test.query_docs("omega").await, before);
	}

	/// The residual scan spends what is left of the round's budget, and a scan
	/// it cuts short leaves the rest for the next round.
	#[test(tokio::test)]
	async fn standalone_residuals_share_the_rounds_budget() {
		let test = TestContext::new().await;
		let tx = test.new_tx(TransactionType::Write).await;
		for (doc, term) in ["a", "b", "c"].into_iter().enumerate() {
			tx.set_key(&test.ikb.new_tr_root(term), &TermDocsResidual::new([(doc as DocId, 1)]))
				.await
				.unwrap();
		}
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 2).await.unwrap();
		assert_eq!(plan.standalone_residuals.len(), 2);
		assert!(plan.has_more(), "the third residual is the next round's");
		tx.cancel().await.unwrap();

		assert_eq!(test.drain_term_docs(2).await, 2);
		for (doc, term) in ["a", "b", "c"].into_iter().enumerate() {
			assert_eq!(test.term_residual(term).await, None);
			assert_eq!(test.term_docs(term).await, Some(RoaringTreemap::from_iter([doc as DocId])));
		}
	}

	/// A residual of a term whose deltas the round folds is folded with them,
	/// so the residual scan steps over it: it takes none of the budget, and a
	/// standalone residual behind it is captured in the same round.
	#[test(tokio::test)]
	async fn the_residual_scan_steps_over_a_term_the_round_folds() {
		let test = TestContext::new().await;
		let tx = test.new_tx(TransactionType::Write).await;
		test.write_tx_delta(&tx, "a", 1, true, &RoaringTreemap::from_iter([1])).await;
		tx.set_key(&test.ikb.new_tr_root("a"), &TermDocsResidual::new([(1, 1)])).await.unwrap();
		tx.set_key(&test.ikb.new_tr_root("b"), &TermDocsResidual::new([(2, 1)])).await.unwrap();
		tx.commit().await.unwrap();

		// One `!tx` entry of one document leaves one entry of a budget of two.
		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 2).await.unwrap();
		assert_eq!(plan.standalone_residuals, vec!["b".to_string()]);
		assert!(!plan.has_more(), "nothing is left beyond the residual captured");
		assert!(test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap());
		tx.commit().await.unwrap();
		assert_eq!(test.term_residual("a").await, None);
		assert_eq!(test.term_residual("b").await, None);
		assert_eq!(test.term_docs("b").await, Some(RoaringTreemap::from_iter([2])));
	}

	/// Exactly as many standalone residuals as the budget allows are captured
	/// in one round that owes no other.
	#[test(tokio::test)]
	async fn standalone_residuals_that_fill_the_budget_owe_no_later_round() {
		let test = TestContext::new().await;
		let tx = test.new_tx(TransactionType::Write).await;
		for (doc, term) in ["a", "b"].into_iter().enumerate() {
			tx.set_key(&test.ikb.new_tr_root(term), &TermDocsResidual::new([(doc as DocId, 1)]))
				.await
				.unwrap();
		}
		tx.commit().await.unwrap();

		let tx = test.new_tx(TransactionType::Write).await;
		let plan = test.fti.prepare_term_docs_compaction_with_limit(&tx, 2).await.unwrap();
		tx.cancel().await.unwrap();
		assert_eq!(plan.standalone_residuals.len(), 2);
		assert!(!plan.has_more(), "no residual is left behind the ones captured");
		assert_eq!(test.drain_term_docs(2).await, 1, "one round folds them both");
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
		test.fti
			.index_content(
				&ctx,
				&az_fn,
				&doc,
				vec![test.content.as_ref().clone()],
				&mut require_compaction,
			)
			.await
			.unwrap();

		// Nothing has reached the keyspace: the deltas are still in the buffer.
		assert_eq!(test.tt_delta_count(&tx).await, 0, "deltas must still be buffered");
		assert_eq!(test.dc_delta_count(&tx).await, 0, "statistics must still be buffered");

		let qt = test.fti.extract_querying_terms(&ctx, &az_fn, "Welcome".into()).await.unwrap();
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

	/// A document the reading transaction writes after reading a query's terms
	/// is judged as it then stands ([`FullTextIndex::contains_doc_now`]):
	/// whatever its committed history, compacted or not, in either format, and
	/// whatever the transaction wrote to it before the read, it matches the
	/// query exactly where terms read afresh say it does.
	#[test(tokio::test)]
	async fn a_document_written_after_the_terms_were_read_is_judged_as_it_stands() {
		let az_fn = NoAnalyzerFunction;
		let query = "hello world";
		let histories: [&[&str]; 5] =
			[&[], &["hello"], &["bye"], &["hello", "bye"], &["bye", "hello world"]];
		let befores = [None, Some("hello world"), Some("bye")];
		let afters = ["hello", "world", "hello world", "bye"];
		for format in [
			crate::catalog::DOC_IDS_FORMAT_VERSION,
			crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION,
		] {
			for compacted in [false, true] {
				let test = TestContext::at_format(format).await;
				let mut cases = Vec::new();
				for history in histories {
					for before in befores {
						for after in afters {
							let rid = RecordId::new("t".into(), format!("d{}", cases.len()));
							cases.push((rid, history, before, after));
						}
					}
				}
				// Each document's committed history: its first content, folded
				// into the compacted sets where the round asks, then its second,
				// left as changes.
				for (rid, history, ..) in &cases {
					if let Some(&first) = history.first() {
						test.rewrite(rid, None, Some(first)).await;
					}
				}
				if compacted {
					loop {
						let tx = test.new_tx(TransactionType::Write).await;
						let more = test.fti.compaction(&tx).await.unwrap();
						tx.commit().await.unwrap();
						if !more {
							break;
						}
					}
				}
				for (rid, history, ..) in &cases {
					if let &[first, second] = *history {
						test.rewrite(rid, Some(first), Some(second)).await;
					}
				}
				// The cases whose answer the write after the read changed.
				let mut moved = 0;
				for (rid, history, before, after) in &cases {
					let case = format!(
						"{history:?} committed, then {before:?} and {after:?} written, compacted \
						 {compacted}, format {format}"
					);
					let ctx = test.ds.env(TransactionType::Write).await;
					let tx = ctx.tx();
					let mut held = history.last().copied();
					if let Some(before) = *before {
						test.rewrite_within(&ctx, rid, held, Some(before)).await;
						held = Some(before);
					}
					let read = test
						.fti
						.extract_judging_terms(&ctx, &az_fn, query.to_owned())
						.await
						.unwrap();
					test.rewrite_within(&ctx, rid, held, Some(*after)).await;
					let doc_id =
						test.fti.get_doc_id(&tx, rid).await.unwrap().expect("an indexed document");
					let fresh = test
						.fti
						.extract_querying_terms(&ctx, &az_fn, query.to_owned())
						.await
						.unwrap();
					for op in [BooleanOperator::And, BooleanOperator::Or] {
						let now = fresh.contains_doc(doc_id, op);
						assert_eq!(
							test.fti.contains_doc_now(&tx, &read, doc_id, op),
							now,
							"{op:?}: {case}"
						);
						moved += usize::from(read.contains_doc(doc_id, op) != now);
					}
					tx.cancel().await.unwrap();
				}
				assert!(
					moved > 0,
					"no write changed an answer, compacted {compacted}, format {format}"
				);
			}
		}
	}

	/// Terms read to judge documents score and locate a document as the reading
	/// transaction holds the index when they are asked, its writes since the
	/// read included ([`Scorer::score`], [`FullTextIndex::read_offsets_doc`]):
	/// through a run of rewrites, removals, additions and save points released
	/// and rolled back, every document gets the score, bit for bit, and the
	/// offsets that terms and a scorer read afresh give it. So does a document
	/// the run never writes, whose idf and length normalisation the run's
	/// writes to others move. In either format, compacted or not, for a query
	/// of one-term words and for one whose word an n-gram analyzer splits into
	/// several terms, a document carrying only some of which scores nothing.
	#[test(tokio::test)]
	async fn terms_read_to_judge_score_a_document_as_terms_read_afresh() {
		#[derive(Clone, Copy, Debug)]
		enum Step {
			Write(usize, Option<&'static str>),
			Push,
			Release,
			Rollback,
		}
		let az_fn = NoAnalyzerFunction;
		// Each document's committed history: its first content, folded into the
		// compacted sets where the round asks, then its second, left as changes.
		let histories: [&[&str]; 6] =
			[&["hello world"], &["hello"], &["bye"], &["hello", "hello world again"], &[], &[]];
		let steps = [
			Step::Write(0, Some("hello")),
			Step::Write(1, None),
			Step::Push,
			Step::Write(4, Some("hello there hello")),
			Step::Write(3, Some("bye")),
			Step::Rollback,
			Step::Push,
			Step::Write(5, Some("help hello world")),
			Step::Write(2, Some("world")),
			Step::Release,
			Step::Write(1, Some("hello world")),
			Step::Write(0, Some("help")),
		];
		let analyzers: [(Option<Vec<crate::expr::Filter>>, &str); 2] =
			[(None, "hello world"), (Some(vec![crate::expr::Filter::Ngram(3, 3)]), "hello")];
		for (filters, query) in analyzers {
			for format in [
				crate::catalog::DOC_IDS_FORMAT_VERSION,
				crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION,
			] {
				for compacted in [false, true] {
					let test = TestContext::build_filtered(
						true,
						vec![Tokenizer::Blank],
						filters.clone(),
						format,
					)
					.await;
					let rids: Vec<RecordId> = (0..histories.len())
						.map(|at| RecordId::new("t".into(), format!("d{at}")))
						.collect();
					let mut held: Vec<Option<&str>> = vec![None; rids.len()];
					for (at, history) in histories.iter().enumerate() {
						if let Some(&first) = history.first() {
							test.rewrite(&rids[at], None, Some(first)).await;
							held[at] = Some(first);
						}
					}
					if compacted {
						loop {
							let tx = test.new_tx(TransactionType::Write).await;
							let more = test.fti.compaction(&tx).await.unwrap();
							tx.commit().await.unwrap();
							if !more {
								break;
							}
						}
					}
					for (at, history) in histories.iter().enumerate() {
						if let &[first, second] = *history {
							test.rewrite(&rids[at], Some(first), Some(second)).await;
							held[at] = Some(second);
						}
					}
					let ctx = test.ds.env(TransactionType::Write).await;
					let tx = ctx.tx();
					// A write before the read, which the terms read.
					test.rewrite_within(&ctx, &rids[2], held[2], Some("hello")).await;
					held[2] = Some("hello");
					let read = test
						.fti
						.extract_judging_terms(&ctx, &az_fn, query.to_owned())
						.await
						.unwrap()
						.scored_as_stored();
					let scorer = test.fti.new_scorer(&ctx).await.unwrap().expect("a BM25 index");
					let mut saved = Vec::new();
					let mut first: Vec<Option<u32>> = vec![None; rids.len()];
					let mut moved = 0;
					for (done, step) in
						std::iter::once(None).chain(steps.iter().map(Some)).enumerate()
					{
						match step {
							None => {}
							Some(Step::Write(at, new)) => {
								test.rewrite_within(&ctx, &rids[*at], held[*at], *new).await;
								held[*at] = *new;
							}
							Some(Step::Push) => {
								tx.new_save_point().await.unwrap();
								saved.push(held.clone());
							}
							Some(Step::Release) => {
								tx.release_last_save_point().await.unwrap();
								saved.pop();
							}
							Some(Step::Rollback) => {
								tx.rollback_to_save_point().await.unwrap();
								held = saved.pop().expect("a save point");
							}
						}
						let fresh = test
							.fti
							.extract_querying_terms(&ctx, &az_fn, query.to_owned())
							.await
							.unwrap()
							.scored_as_stored();
						let fresh_scorer =
							test.fti.new_scorer(&ctx).await.unwrap().expect("a BM25 index");
						for (at, rid) in rids.iter().enumerate() {
							let case = format!(
								"{query:?} filtered by {filters:?}, document {at} holding {:?} \
								 after {:?}, compacted {compacted}, format {format}",
								held[at],
								&steps[..done],
							);
							let Some(doc_id) = test.fti.get_doc_id(&tx, rid).await.unwrap() else {
								continue;
							};
							let live = scorer.score(&test.fti, &tx, &read, doc_id).await.unwrap();
							let now =
								fresh_scorer.score(&test.fti, &tx, &fresh, doc_id).await.unwrap();
							assert_eq!(live.to_bits(), now.to_bits(), "{live} vs {now}: {case}");
							assert_eq!(
								test.fti
									.read_offsets_doc(&tx, doc_id, &[&read], false)
									.await
									.unwrap(),
								test.fti
									.read_offsets_doc(&tx, doc_id, &[&fresh], false)
									.await
									.unwrap(),
								"offsets: {case}"
							);
							match first[at] {
								None => first[at] = Some(live.to_bits()),
								Some(first) => moved += usize::from(first != live.to_bits()),
							}
						}
					}
					assert!(
						moved > 0,
						"no write moved a score: {filters:?}, compacted {compacted}, format {format}"
					);
					tx.cancel().await.unwrap();
				}
			}
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn compaction_preserves_post_snapshot_deltas() {
		let test = TestContext::new().await;
		let doc1 = Arc::new(RecordId::new("t".into(), "doc1".to_owned()));
		let doc2 = Arc::new(RecordId::new("t".into(), "doc2".to_owned()));

		test.remove_insert_task(&doc1).await;

		let read_tx = test.new_tx(TransactionType::Read).await;
		let plan = test.fti.prepare_compaction(&read_tx).await.unwrap();
		read_tx.cancel().await.unwrap();

		test.remove_insert_task(&doc2).await;

		let write_tx = test.new_tx(TransactionType::Write).await;
		assert_eq!(
			test.fti.apply_compaction(&write_tx, plan).await.unwrap(),
			CompactionApply::Applied
		);
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

	/// Hits are taken a batch at a time in doc-id order and resolved to their
	/// records in one read per batch; a hit whose record mapping is gone resolves
	/// to none.
	#[test(tokio::test)]
	async fn hits_resolve_in_batches_and_pass_over_unmapped_documents() {
		let test = TestContext::with_highlighting(false).await;
		let rids: Vec<RecordId> =
			(0..5).map(|n| RecordId::new("t".into(), format!("doc{n}"))).collect();
		test.index_batch_in_one_tx(&rids, &Value::from("hello")).await;
		// Drop the mapping of the second document, as a record purge does.
		let tx = test.new_tx(TransactionType::Write).await;
		crate::docids::TableDocIds::new(test.ikb.ns(), test.ikb.db(), test.ikb.table().clone())
			.remove(&tx, &rids[1].key)
			.await
			.unwrap();
		tx.commit().await.unwrap();

		let (ctx, tx) = test.new_read_env().await;
		let qt = test
			.fti
			.extract_querying_terms(&ctx, &NoAnalyzerFunction, "hello".to_owned())
			.await
			.unwrap();
		let mut hits =
			test.fti.new_hits_iterator(&qt, crate::expr::operator::BooleanOperator::And).unwrap();
		let mut batches = Vec::new();
		loop {
			let doc_ids = hits.take_doc_ids(2);
			if doc_ids.is_empty() {
				break;
			}
			let records = hits.records(&tx, &doc_ids).await.unwrap();
			batches.push((doc_ids, records));
		}
		tx.cancel().await.unwrap();

		let resolved: Vec<Vec<Option<RecordId>>> =
			batches.iter().map(|(_, records)| records.clone()).collect();
		assert_eq!(
			resolved,
			vec![
				vec![Some(rids[0].clone()), None],
				vec![Some(rids[2].clone()), Some(rids[3].clone())],
				vec![Some(rids[4].clone())],
			],
			"the purged document resolves to none"
		);
		let doc_ids: Vec<DocId> = batches.iter().flat_map(|(ids, _)| ids.clone()).collect();
		assert!(doc_ids.windows(2).all(|w| w[0] < w[1]), "in doc-id order: {doc_ids:?}");
	}

	/// A take passing hits over by their block bounds takes exactly the hits a
	/// check of each hit's bound keeps, and of an `OR`, that carry a term
	/// whose ceiling is not passed over, in order, with the others counted as
	/// passed over, for thresholds from none passed over to all, and batch sizes
	/// from one hit up; under a threshold tightening from one take to the next
	/// it takes only hits the threshold of their take keeps and every hit the
	/// last one does. Every hit whose score beats the threshold is taken.
	/// Uncompacted documents are taken wherever their ranges are passed over,
	/// and the hits not looked at are still counted.
	#[test(tokio::test)]
	async fn a_bounded_take_keeps_the_hits_a_check_of_each_bound_keeps() {
		let test =
			TestContext::at_format(crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION).await;
		let mut seed = 0x9e37_79b9_7f4a_7c15u64;
		let mut next = move |bound: u64| {
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			seed % bound
		};
		let words = ["apple", "apple", "berry", "filler", "filler", "filler", "filler", "filler"];
		let rids: Vec<RecordId> =
			(0..3000).map(|i| RecordId::new("t".into(), format!("d{i:05}"))).collect();
		let body = |next: &mut dyn FnMut(u64) -> u64| {
			(0..1 + next(30))
				.map(|_| match next(words.len() as u64 * 25) {
					0 => "cherry",
					n => words[(n / 25) as usize],
				})
				.collect::<Vec<_>>()
				.join(" ")
		};
		for chunk in rids.chunks(250) {
			for rid in chunk {
				test.rewrite(rid, None, Some(&body(&mut next))).await;
			}
		}
		test.drain_term_docs(100_000).await;
		// Documents rewritten and indexed since, scattered over the doc ids.
		for at in (0..rids.len()).step_by(97) {
			test.rewrite(&rids[at], None, Some(&body(&mut next))).await;
		}
		let (ctx, tx) = test.new_read_env().await;
		let scorer = test.fti.new_scorer(&ctx).await.unwrap().expect("a BM25 index");
		let mut cut = 0;
		for (query, op) in [
			("apple", crate::expr::operator::BooleanOperator::And),
			("apple berry", crate::expr::operator::BooleanOperator::And),
			("apple berry", crate::expr::operator::BooleanOperator::Or),
			("apple cherry", crate::expr::operator::BooleanOperator::Or),
			("cherry berry apple cherry", crate::expr::operator::BooleanOperator::Or),
		] {
			let or = matches!(op, crate::expr::operator::BooleanOperator::Or);
			let qt = test
				.fti
				.extract_querying_terms(&ctx, &NoAnalyzerFunction, query.to_owned())
				.await
				.unwrap();
			let bounds = qt.block_bounds(&scorer).expect("posting blocks bound the score");
			assert!(bounds.starts.len() > 3, "`{query}` spans several blocks");
			let all = test.fti.new_hits_iterator(&qt, op).unwrap().take_doc_ids(usize::MAX);
			assert!(all.iter().any(|doc_id| qt.is_uncompacted(*doc_id)));
			let scores = scorer.score_batch(&test.fti, &tx, &qt, &all).await.unwrap();
			let mut levels: Vec<f32> = bounds.bounds.clone();
			levels.extend(bounds.terms.iter().map(|(_, ceiling)| super::round_up(*ceiling)));
			levels.sort_by(f32::total_cmp);
			levels.dedup();
			let keeps = |threshold: f32, doc_id: DocId| {
				if qt.is_uncompacted(doc_id) {
					return true;
				}
				let passed = bounds.passed_terms(|b| b > threshold);
				let essential = !or
					|| bounds.terms.len() < 2
					|| bounds.terms.iter().zip(&passed).any(|((at, _), passed)| {
						!passed
							&& qt.docs_of(&qt.scored[*at]).is_some_and(|docs| docs.contains(doc_id))
					});
				essential && bounds.bound(doc_id).is_none_or(|b| b > threshold)
			};
			for threshold in levels.iter().copied().chain([-1.0, f32::MAX]) {
				let beating: Vec<DocId> = all
					.iter()
					.zip(&scores)
					.filter(|(_, score)| **score > threshold)
					.map(|(doc_id, _)| *doc_id)
					.collect();
				let expected: Vec<DocId> =
					all.iter().copied().filter(|doc_id| keeps(threshold, *doc_id)).collect();
				for limit in [1, 7, 100, usize::MAX] {
					let mut hits = test.fti.new_hits_iterator(&qt, op).unwrap();
					let (mut taken, mut passed) = (Vec::new(), 0);
					loop {
						let (batch, skipped) =
							hits.take_doc_ids_within(limit, &qt, &bounds, |b| b > threshold);
						passed += skipped;
						if batch.is_empty() {
							break;
						}
						assert!(batch.len() <= limit);
						taken.extend(batch);
					}
					assert_eq!(taken, expected, "`{query}` above {threshold}, by {limit}");
					assert_eq!(
						passed as usize,
						all.len() - expected.len(),
						"`{query}` above {threshold}, by {limit}"
					);
					assert!(
						beating.iter().all(|doc_id| taken.contains(doc_id)),
						"`{query}` above {threshold}, by {limit}: a hit that beats it is passed over"
					);
					assert_eq!(crate::ft::MatchesHitsIterator::len(&hits), 0);
				}
				let by_bound = all
					.iter()
					.filter(|doc_id| {
						qt.is_uncompacted(**doc_id)
							|| bounds.bound(**doc_id).is_none_or(|b| b > threshold)
					})
					.count();
				cut += by_bound - expected.len();
			}
			// A threshold rising over the takes.
			let mut hits = test.fti.new_hits_iterator(&qt, op).unwrap();
			let mut taken = Vec::new();
			for threshold in levels.iter().copied() {
				let (batch, _) = hits.take_doc_ids_within(5, &qt, &bounds, |b| b > threshold);
				assert!(batch.iter().all(|doc_id| keeps(threshold, *doc_id)), "`{query}`");
				taken.extend(batch);
			}
			let last = levels.last().copied().unwrap_or_default();
			loop {
				let (batch, _) = hits.take_doc_ids_within(5, &qt, &bounds, |b| b > last);
				if batch.is_empty() {
					break;
				}
				taken.extend(batch);
			}
			let kept_last: Vec<DocId> =
				all.iter().copied().filter(|doc_id| keeps(last, *doc_id)).collect();
			assert!(kept_last.iter().all(|doc_id| taken.contains(doc_id)), "`{query}`");
			assert!(taken.windows(2).all(|w| w[0] < w[1]), "`{query}`: in doc-id order");
		}
		assert!(cut > 0, "no hit of an `OR` was cut by the terms' ceilings alone");
		tx.cancel().await.unwrap();
	}

	/// A document carrying none of the query's terms scores zero and has no
	/// term to highlight; one carrying a term scores above zero.
	#[test(tokio::test)]
	async fn a_document_carrying_no_term_scores_zero() {
		let test = TestContext::new().await;
		let hello = RecordId::new("t".into(), "hello".to_owned());
		let others: Vec<RecordId> =
			(0..3).map(|n| RecordId::new("t".into(), format!("other{n}"))).collect();
		test.index_batch_in_one_tx(std::slice::from_ref(&hello), &Value::from("hello world")).await;
		test.index_batch_in_one_tx(&others, &Value::from("goodbye moon")).await;

		let (ctx, tx) = test.new_read_env().await;
		let qt = test
			.fti
			.extract_querying_terms(&ctx, &NoAnalyzerFunction, "hello".to_owned())
			.await
			.unwrap();
		let scorer = test.fti.new_scorer(&ctx).await.unwrap().expect("BM25 is configured");
		let carrier = test.fti.get_doc_id(&tx, &hello).await.unwrap().unwrap();
		let other = test.fti.get_doc_id(&tx, &others[0]).await.unwrap().unwrap();
		assert!(scorer.score(&test.fti, &tx, &qt, carrier).await.unwrap() > 0.0);
		assert_eq!(scorer.score(&test.fti, &tx, &qt, other).await.unwrap(), 0.0);
		assert!(test.fti.term_hits(&tx, other, &[&qt]).await.unwrap().is_empty());
		assert_eq!(test.fti.term_hits(&tx, carrier, &[&qt]).await.unwrap().len(), 1);
		tx.cancel().await.unwrap();
	}

	/// Scoring a batch answers what scoring each document alone answers, in
	/// input order, for documents that carry the terms and documents that do
	/// not, in either indexing path.
	#[test(tokio::test)]
	async fn a_batch_scores_as_its_documents_do_alone() {
		for highlight in [false, true] {
			let test = TestContext::with_highlighting(highlight).await;
			let texts = ["hello world", "hello hello there", "goodbye", "world peace", "hello"];
			let mut rids = Vec::new();
			for (n, text) in texts.iter().enumerate() {
				let rid = RecordId::new("t".into(), format!("doc{n}"));
				test.index_batch_in_one_tx(std::slice::from_ref(&rid), &Value::from(*text)).await;
				rids.push(rid);
			}
			let (ctx, tx) = test.new_read_env().await;
			let qt = test
				.fti
				.extract_querying_terms(&ctx, &NoAnalyzerFunction, "hello world".to_owned())
				.await
				.unwrap();
			let scorer = test.fti.new_scorer(&ctx).await.unwrap().expect("BM25 is configured");
			let mut doc_ids = Vec::new();
			for rid in rids.iter().rev() {
				doc_ids.push(test.fti.get_doc_id(&tx, rid).await.unwrap().unwrap());
			}
			let batch = scorer.score_batch(&test.fti, &tx, &qt, &doc_ids).await.unwrap();
			let mut alone = Vec::new();
			for doc_id in &doc_ids {
				alone.push(scorer.score(&test.fti, &tx, &qt, *doc_id).await.unwrap());
			}
			assert_eq!(batch, alone, "highlight={highlight}");
			assert!(batch.iter().any(|s| *s > 0.0), "some document scores: {batch:?}");
			assert!(batch.contains(&0.0), "the document carrying no term scores zero");
			tx.cancel().await.unwrap();
		}
	}

	/// Times a full compaction of a large-vocabulary corpus on the bitmap
	/// layout and on posting blocks. Run by hand:
	/// `cargo test --locked --release -p surrealdb-idx --features fulltext -- --ignored --nocapture
	/// compaction_cost_by_format`.
	#[ignore]
	#[test(tokio::test)]
	async fn compaction_cost_by_format() {
		let docs: usize =
			std::env::var("FT_DOCS").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000);
		let words: usize =
			std::env::var("FT_WORDS").ok().and_then(|v| v.parse().ok()).unwrap_or(600);
		let vocabulary: u64 = 3000;
		// Term i is drawn with weight 1/i over `vocabulary` terms.
		let weights: Vec<f64> = (1..=vocabulary).map(|i| 1.0 / i as f64).collect();
		let total: f64 = weights.iter().sum();
		let mut cumulative = Vec::with_capacity(weights.len());
		let mut acc = 0.0;
		for w in &weights {
			acc += w / total;
			cumulative.push(acc);
		}
		let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
		let mut next = move || {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			(state >> 11) as f64 / (1u64 << 53) as f64
		};
		let corpus: Vec<String> = (0..docs)
			.map(|_| {
				(0..words)
					.map(|_| {
						let r = next();
						let i = cumulative.partition_point(|c| *c < r);
						format!("t{}", i + 1)
					})
					.collect::<Vec<_>>()
					.join(" ")
			})
			.collect();
		for format in [
			crate::catalog::LEGACY_LEVEL_INDEX_FORMAT_VERSION,
			crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION,
		] {
			let highlight = std::env::var("FT_HIGHLIGHT").is_ok();
			let test = TestContext::build(highlight, vec![Tokenizer::Blank], format).await;
			let start = Instant::now();
			for (chunk, texts) in corpus.chunks(250).enumerate() {
				let ctx = test.ds.env(TransactionType::Write).await;
				let tx = ctx.tx();
				let mut require_compaction = false;
				for (i, text) in texts.iter().enumerate() {
					let rid = RecordId::new("t".into(), format!("d{}", chunk * 250 + i));
					test.fti
						.index_content(
							&ctx,
							&NoAnalyzerFunction,
							&rid,
							vec![Value::from(text.as_str())],
							&mut require_compaction,
						)
						.await
						.unwrap();
				}
				tx.commit().await.unwrap();
			}
			let indexed = start.elapsed();
			let start = Instant::now();
			let mut rounds = 0;
			loop {
				let tx = test.new_tx(TransactionType::Write).await;
				let plan = test
					.fti
					.prepare_term_docs_compaction_with_limit(&tx, super::COUNT_BATCH_SIZE)
					.await
					.unwrap();
				let has_more = plan.has_more();
				let applied = test.fti.apply_term_docs_compaction(&tx, plan).await.unwrap();
				tx.commit().await.unwrap();
				rounds += 1;
				if !applied || !has_more {
					break;
				}
			}
			eprintln!(
				"format {format}: {docs} docs x {words} words: indexed in {indexed:?}, compacted in {:?} over {rounds} rounds",
				start.elapsed()
			);
		}
	}
}
