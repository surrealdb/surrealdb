//! Full-text kernels: what a query's terms read, how what they read becomes
//! each term's document set, how the sets combine under a boolean operator,
//! and the BM25 score of one term in one document.
//!
//! [`highlight`] and [`offsets`] render the occurrences of a query's terms in
//! one document from the offsets its postings hold.
//!
//! A term's document set is spread over four places, and is their sum:
//!
//! - its compacted documents: one membership bitmap (`!td`), or, on an index that keeps its
//!   postings in blocks, its posting blocks (`!tp`, [`super::postings`]);
//! - its residual (`!tr`), per-document totals the bitmap cannot hold;
//! - its uncompacted changes: per-document `!tt` keys an older server wrote, whose key alone
//!   records an addition or a removal, and batched `!tx` bitmaps;
//! - the changes the reading transaction buffered and has not yet written.
//!
//! [`term_reads`] names the keys and ranges of the first three, which the
//! caller reads; [`term_deltas`] folds the changes into signed per-document
//! counts, [`add_residual`] and [`add_pending`] add the residual and the
//! buffered changes to them, and [`term_docs`] applies them to the compacted
//! documents. A document belongs to the term when its compacted membership plus
//! its total comes to one or more ([`carries`]); only the sign of a change
//! matters.

use ahash::HashMap;
use anyhow::Result;
use roaring::RoaringTreemap;

use crate::IndexKeyBase;
use crate::docids::DocId;
use crate::expr::Idiom;
use crate::expr::operator::BooleanOperator;
use crate::ft::TermFrequency;
use crate::ft::fulltext::{ChangeBatch, PostingBlock, TermDocsResidual};
use crate::ft::highlighter::{HighlightParams, Highlighter, Offseter};
use crate::ft::offset::Offset;
use crate::key::schema::{TermChangeBatchKey, TermChangeKey};
use crate::key::{KVKey, KVKeyDecode, Key, TypedRange};
use crate::val::Value;

/// The stored state of one term a query reads.
#[derive(Debug)]
pub struct TermReads {
	/// The per-document change keys; their values are not read.
	pub changes: TypedRange<String>,
	/// The batched changes, of which a query uses the documents.
	pub batches: TypedRange<ChangeBatch>,
	/// The residual's key, holding a [`TermDocsResidual`].
	pub residual: Key<'static>,
	/// Where the term's compacted documents are kept.
	pub compacted: CompactedReads,
}

/// Where a term's compacted documents are kept, as the index's format keeps
/// them.
#[derive(Debug)]
pub enum CompactedReads {
	/// The key of the term's membership bitmap, holding a [`RoaringTreemap`].
	Bitmap(Key<'static>),
	/// The range of the term's posting blocks, each a [`PostingBlock`], in
	/// order of start.
	Blocks(TypedRange<PostingBlock>),
}

/// What a query reads of `term` in the index `ikb` names; `blocks` where the
/// index keeps its postings in blocks
/// ([`crate::catalog::keeps_posting_blocks`]).
pub fn term_reads(ikb: &IndexKeyBase, term: &str, blocks: bool) -> Result<TermReads> {
	Ok(TermReads {
		changes: ikb.new_tt_term_range(term)?,
		batches: ikb.new_tx_term_range(term)?,
		residual: ikb.new_tr_root(term).encode_key()?,
		compacted: if blocks {
			CompactedReads::Blocks(ikb.new_tp_term_range(term)?)
		} else {
			CompactedReads::Bitmap(ikb.new_td_root(term).encode_key()?)
		},
	})
}

/// One term's uncompacted changes, as its reads answered them.
pub struct TermChanges<'a> {
	/// The keys of [`TermReads::changes`].
	pub change_keys: &'a [Vec<u8>],
	/// The keys and changes of [`TermReads::batches`], of which a query uses
	/// the documents.
	pub batches: &'a [(Vec<u8>, ChangeBatch)],
}

/// The signed per-document count of `changes`.
pub fn term_deltas(changes: &TermChanges<'_>) -> Result<HashMap<DocId, i64>> {
	let mut deltas: HashMap<DocId, i64> = HashMap::default();
	for k in changes.change_keys {
		let tt = TermChangeKey::decode_key(k)?;
		*deltas.entry(tt.doc_id).or_default() += if tt.add {
			1
		} else {
			-1
		};
	}
	for (k, batch) in changes.batches {
		let step = if TermChangeBatchKey::decode_key(k)?.add {
			1
		} else {
			-1
		};
		for doc_id in &batch.docs {
			*deltas.entry(doc_id).or_default() += step;
		}
	}
	Ok(deltas)
}

/// Adds a term's `residual` to the per-document counts `deltas`.
pub fn add_residual(deltas: &mut HashMap<DocId, i64>, residual: &TermDocsResidual) {
	for (doc_id, count) in residual.counts.iter() {
		*deltas.entry(*doc_id).or_default() += *count;
	}
}

/// Adds the changes the reading transaction buffered to a term to the
/// per-document counts `deltas`: one for each document it `added` to the term,
/// minus one for each it `removed`. A document in both sets has no net change.
pub fn add_pending(
	deltas: &mut HashMap<DocId, i64>,
	added: &RoaringTreemap,
	removed: &RoaringTreemap,
) {
	for doc_id in added {
		*deltas.entry(doc_id).or_default() += 1;
	}
	for doc_id in removed {
		*deltas.entry(doc_id).or_default() -= 1;
	}
}

/// Whether a document carries a term: its `compacted` membership plus
/// `change`, the total of its residual and its changes, comes to one or more.
/// [`term_docs`] holds every document a term's changes name to the same rule.
pub fn carries(compacted: bool, change: i64) -> bool {
	i64::from(compacted) + change >= 1
}

/// The documents carrying a term: its compacted `bitmap` with its `residual`
/// and its `deltas` applied, or `None` when no document carries it.
pub fn term_docs(
	bitmap: Option<RoaringTreemap>,
	residual: Option<&TermDocsResidual>,
	mut deltas: HashMap<DocId, i64>,
) -> Option<RoaringTreemap> {
	if let Some(residual) = residual {
		add_residual(&mut deltas, residual);
	}
	// Applied in doc-id order, so the bitmap is walked forward once for the
	// additions and once for the removals rather than in hash order.
	let (mut added, mut removed) = (Vec::new(), Vec::new());
	for (doc_id, delta) in deltas {
		if delta > 0 {
			added.push(doc_id);
		} else if delta < 0 {
			removed.push(doc_id);
		}
	}
	added.sort_unstable();
	removed.sort_unstable();
	let mut docs = bitmap.unwrap_or_default();
	docs.extend(added);
	for doc_id in removed {
		docs.remove(doc_id);
	}
	(!docs.is_empty()).then_some(docs)
}

/// The documents matching a query whose terms carry `docs`, under `op`:
/// those carrying every term for `AND`, any term for `OR`. `None` when no
/// document matches.
pub fn combine(docs: &[Option<RoaringTreemap>], op: BooleanOperator) -> Option<RoaringTreemap> {
	match op {
		BooleanOperator::And => intersection(docs),
		BooleanOperator::Or => union(docs),
	}
}

fn intersection(docs: &[Option<RoaringTreemap>]) -> Option<RoaringTreemap> {
	// A term no document carries empties the intersection.
	let mut sets = docs.iter().map(Option::as_ref).collect::<Option<Vec<_>>>()?;
	// Intersecting the smaller sets first keeps the running result small.
	sets.sort_by_key(|bitmap| bitmap.len());
	let mut sets = sets.into_iter();
	let mut result = sets.next()?.clone();
	for set in sets {
		result &= set;
		if result.is_empty() {
			return None;
		}
	}
	(!result.is_empty()).then_some(result)
}

fn union(docs: &[Option<RoaringTreemap>]) -> Option<RoaringTreemap> {
	let mut sets = docs.iter().flatten();
	let mut result = sets.next()?.clone();
	for set in sets {
		result |= set;
	}
	(!result.is_empty()).then_some(result)
}

/// The statistics BM25 scores a document against: the parameters `k1` and
/// `b`, the number of indexed documents and their total length.
#[derive(Debug, Clone, Copy)]
pub struct Bm25 {
	k1: f64,
	k1_plus_1: f64,
	one_minus_b: f64,
	b_over_avg_len: f64,
	doc_count: f64,
}

impl Bm25 {
	pub fn new(k1: f32, b: f32, doc_count: i64, total_docs_length: i128) -> Self {
		let doc_count = doc_count as f64;
		let k1 = k1 as f64;
		let b = b as f64;
		// An index with no document, or whose documents hold no token, has no
		// average length to normalise against, so the document length drops
		// out of the score rather than `0 / 0` turning it NaN.
		let b_over_avg_len = if doc_count > 0.0 && total_docs_length > 0 {
			b / ((total_docs_length as f64) / doc_count)
		} else {
			0.0
		};
		Self {
			k1,
			k1_plus_1: k1 + 1.0,
			one_minus_b: 1.0 - b,
			b_over_avg_len,
			doc_count,
		}
	}

	/// The Okapi-BM25 score of a term occurring `term_freq` times in a
	/// document of `doc_length` tokens, where `term_doc_count` documents carry
	/// the term.
	///
	/// The inverse document frequency is the smoothed form
	/// `ln(1 + (N − n + 0.5) / (n + 0.5))`, which stays positive for every
	/// `n ≤ N`, so a term most documents carry still scores above zero and
	/// ranks documents by term frequency and length. The term frequency is
	/// lower-bounded as `1 + ln(tf)` (Lv & Zhai, "Lower-Bounding Term
	/// Frequency Normalization", CIKM 2011):
	///
	/// ```text
	/// score = idf · (k1 + 1) · tf′ / (tf′ + k1 · (1 − b + b · doc_len / avg_doc_len))
	/// idf   = ln(1 + (N − n + 0.5) / (n + 0.5))
	/// tf′   = 1 + ln(tf)
	/// ```
	/// Whether the score rises with the term's frequency and does not rise
	/// with the document's length, so that the score of a block's highest
	/// frequency at its shortest length bounds the score of every document in
	/// it: `k1 ≥ 0` and `0 ≤ b ≤ 1`, which keep `k1 + 1`, the length norm and
	/// the denominator non-negative.
	pub fn bounded_by_extremes(&self) -> bool {
		self.k1 >= 0.0 && (0.0..=1.0).contains(&self.one_minus_b) && self.b_over_avg_len >= 0.0
	}

	pub fn score(&self, term_freq: f64, term_doc_count: f64, doc_length: f64) -> f64 {
		if term_freq <= 0.0 {
			return 0.0;
		}
		self.weigh(self.idf(term_doc_count), 1.0 + term_freq.ln(), doc_length)
	}

	/// The inverse document frequency of a term `term_doc_count` documents
	/// carry, as [`Self::score`] weighs it. It depends on the term alone, so a
	/// caller scoring one term in many documents works it out once.
	pub fn idf(&self, term_doc_count: f64) -> f64 {
		// A count outside `0..=N` (more documents carrying the term than the
		// index counts, say) would make the ratio negative or undefined, so
		// the count is clamped into that range.
		let doc_count = self.doc_count.max(0.0);
		let n = term_doc_count.max(0.0).min(doc_count);
		((doc_count - n + 0.5) / (n + 0.5)).ln_1p()
	}

	/// [`Self::score`] of a term of inverse document frequency `idf`
	/// ([`Self::idf`]) occurring `term_freq` times in a document of
	/// `doc_length` tokens: bit for bit the value `score` gives from the
	/// term's document count.
	pub fn score_with_idf(&self, idf: f64, term_freq: TermFrequency, doc_length: f64) -> f64 {
		if term_freq == 0 {
			return 0.0;
		}
		self.weigh(idf, tf_prime(term_freq), doc_length)
	}

	/// The score of a term of inverse document frequency `idf` at the
	/// lower-bounded frequency `tf_prime`, in a document of `doc_length`
	/// tokens.
	fn weigh(&self, idf: f64, tf_prime: f64, doc_length: f64) -> f64 {
		let length_norm = self.one_minus_b + self.b_over_avg_len * doc_length;
		idf * self.k1_plus_1 * tf_prime / (tf_prime + self.k1 * length_norm)
	}
}

/// The highest frequency [`TF_PRIME`] holds `1 + ln(tf)` for.
const TF_PRIME_TABLE: usize = 256;

/// `1 + ln(tf)` for every frequency `tf` up to [`TF_PRIME_TABLE`], computed by
/// the expression [`tf_prime`] evaluates past it, so a lookup gives the same
/// value bit for bit. The entry for zero is never read.
static TF_PRIME: std::sync::LazyLock<[f64; TF_PRIME_TABLE + 1]> = std::sync::LazyLock::new(|| {
	let mut table = [0.0; TF_PRIME_TABLE + 1];
	for (tf, prime) in table.iter_mut().enumerate() {
		*prime = 1.0 + (tf as f64).ln();
	}
	table
});

/// `1 + ln(tf)`, the lower-bounded frequency [`Bm25::score`] weighs a term
/// occurring `tf` times by, as the `f64` expression `score` evaluates.
fn tf_prime(tf: TermFrequency) -> f64 {
	match usize::try_from(tf) {
		Ok(at) if at <= TF_PRIME_TABLE => TF_PRIME[at],
		Ok(_) | Err(_) => 1.0 + (tf as f64).ln(),
	}
}

/// One query term a document carries: its length in characters and where in
/// the document it occurs.
pub struct TermHit {
	pub char_len: u32,
	pub offsets: Vec<Offset>,
}

/// `doc`'s field at `idiom` with every occurrence `hits` names wrapped in the
/// prefix and suffix `params` state.
pub fn highlight(
	params: &HighlightParams,
	idiom: &Idiom,
	doc: &Value,
	hits: &[TermHit],
) -> Result<Value> {
	let mut hl = Highlighter::new(params, idiom, doc);
	for hit in hits {
		hl.highlight(hit.char_len, &hit.offsets);
	}
	hl.try_into()
}

/// The spans of the occurrences `hits` names, by the index of the field value
/// each is in; `NONE` when there are none. With `partial`, an occurrence spans
/// the query term's length from where its token was generated, rather than
/// the whole token.
pub fn offsets(partial: bool, hits: &[TermHit]) -> Value {
	let mut or = Offseter::new(partial);
	for hit in hits {
		or.highlight(hit.char_len, &hit.offsets);
	}
	or.into()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn set(ids: &[u64]) -> RoaringTreemap {
		ids.iter().copied().collect()
	}

	fn deltas<const N: usize>(pairs: [(DocId, i64); N]) -> HashMap<DocId, i64> {
		pairs.into_iter().collect()
	}

	#[test]
	fn a_negative_total_removes_a_document_and_a_positive_one_adds_it() {
		let deltas = deltas([(1, -1), (3, 2), (4, 0)]);
		assert_eq!(term_docs(Some(set(&[1, 2])), None, deltas), Some(set(&[2, 3])));
	}

	#[test]
	fn the_residual_counts_toward_the_total() {
		let residual = TermDocsResidual::new([(2, -1)]);
		let deltas = deltas([(2, 1)]);
		// The bitmap holds 2, the residual takes one away and the delta puts it
		// back: the total is zero, so the bitmap's membership stands.
		assert_eq!(term_docs(Some(set(&[2])), Some(&residual), deltas), Some(set(&[2])));
	}

	#[test]
	fn one_document_is_judged_as_its_terms_set_holds_it() {
		for compacted in [false, true] {
			for change in -3..=3 {
				let bitmap = compacted.then(|| set(&[1]));
				let docs = term_docs(bitmap, None, deltas([(1, change)]));
				assert_eq!(
					carries(compacted, change),
					docs.is_some_and(|docs| docs.contains(1)),
					"compacted {compacted}, change {change}"
				);
			}
		}
	}

	#[test]
	fn a_term_no_document_carries_has_no_set() {
		assert_eq!(term_docs(None, None, deltas([(1, -1)])), None);
		assert_eq!(term_docs(Some(set(&[1])), None, deltas([(1, -1)])), None);
	}

	#[test]
	fn and_needs_every_term_and_or_any() {
		let docs = [Some(set(&[1, 2, 3])), Some(set(&[2, 3, 4])), Some(set(&[3, 9]))];
		assert_eq!(combine(&docs, BooleanOperator::And), Some(set(&[3])));
		assert_eq!(combine(&docs, BooleanOperator::Or), Some(set(&[1, 2, 3, 4, 9])));
		let missing = [Some(set(&[1])), None];
		assert_eq!(combine(&missing, BooleanOperator::And), None);
		assert_eq!(combine(&missing, BooleanOperator::Or), Some(set(&[1])));
		assert_eq!(combine(&[], BooleanOperator::And), None);
		assert_eq!(combine(&[Some(set(&[1])), Some(set(&[2]))], BooleanOperator::And), None);
		// Either operator answers `None`, never an empty set, when no document
		// matches.
		let empty = [Some(RoaringTreemap::new()), Some(RoaringTreemap::new())];
		assert_eq!(combine(&empty, BooleanOperator::And), None);
		assert_eq!(combine(&empty, BooleanOperator::Or), None);
		assert_eq!(combine(&[None, None], BooleanOperator::Or), None);
		assert_eq!(combine(&[Some(RoaringTreemap::new())], BooleanOperator::And), None);
	}

	#[test]
	fn a_term_more_documents_carry_than_the_index_counts_scores_as_one_every_document_carries() {
		// Postings that run ahead of the document count are capped at the
		// index size rather than taking the logarithm of a negative ratio.
		let bm25 = Bm25::new(1.2, 0.75, 3, 30);
		let score = bm25.score(2.0, 5.0, 10.0);
		assert_eq!(score, bm25.score(2.0, 3.0, 10.0));
		assert!(score.is_finite() && score > 0.0);
	}

	#[test]
	fn a_negative_document_count_scores_as_an_empty_index() {
		let bm25 = Bm25::new(1.2, 0.75, -2, 0);
		let score = bm25.score(1.0, 1.0, 3.0);
		assert!(score.is_finite() && score >= 0.0, "{score}");
	}

	#[test]
	fn bm25_over_no_document_or_no_token_scores_finitely() {
		// Documents holding no token leave no average length: a zero-length
		// document would otherwise score `inf * 0`.
		for (doc_count, total_docs_length) in [(0, 0), (3, 0)] {
			let bm25 = Bm25::new(1.2, 0.75, doc_count, total_docs_length);
			for doc_length in [0.0, 4.0] {
				let score = bm25.score(2.0, 0.0, doc_length);
				assert!(
					score.is_finite(),
					"{doc_count} documents of {total_docs_length} tokens, length {doc_length}"
				);
			}
		}
	}

	/// A score from a term's idf worked out once, with the frequency's
	/// `1 + ln(tf)` from the table or past it, is the score [`Bm25::score`]
	/// computes from the term's document count, bit for bit.
	#[test]
	fn a_score_from_a_precomputed_idf_is_the_direct_score() {
		for (k1, b, doc_count, total) in [
			(1.2, 0.75, 100_000, 2_800_000),
			(1.2, 0.0, 7, 40),
			(2.0, 1.0, 1, 1),
			(0.0, 0.5, 3, 0),
			(1.2, 0.75, 0, 0),
		] {
			let bm25 = Bm25::new(k1, b, doc_count, total);
			for n in [0u64, 1, 2, 3, 17, 999, 20_757, 99_999, 100_000, 250_000] {
				let idf = bm25.idf(n as f64);
				for tf in (0..=600u64).chain([1 << 20, u32::MAX as u64, u64::MAX]) {
					for length in [0u64, 1, 8, 28, 48, 1000, 1 << 40] {
						assert_eq!(
							bm25.score_with_idf(idf, tf, length as f64).to_bits(),
							bm25.score(tf as f64, n as f64, length as f64).to_bits(),
							"BM25({k1}, {b}) over {doc_count}: tf {tf}, n {n}, length {length}"
						);
					}
				}
			}
		}
		for tf in 1..=TF_PRIME_TABLE as u64 + 4 {
			assert_eq!(tf_prime(tf).to_bits(), (1.0 + (tf as f64).ln()).to_bits(), "tf {tf}");
		}
	}

	#[test]
	fn bm25_weighs_rarer_terms_and_shorter_documents_higher() {
		let bm25 = Bm25::new(1.2, 0.75, 100, 1000);
		let common = bm25.score(1.0, 50.0, 10.0);
		let rare = bm25.score(1.0, 2.0, 10.0);
		assert!(rare > common, "{rare} vs {common}");
		assert!(bm25.score(1.0, 2.0, 5.0) > bm25.score(1.0, 2.0, 20.0));
		assert_eq!(bm25.score(0.0, 2.0, 5.0), 0.0);
		// A term every document carries still scores above zero, so a small or
		// homogeneous corpus ranks its documents.
		assert!(bm25.score(1.0, 100.0, 5.0) > 0.0);
		assert!(bm25.score(1.0, 99.0, 5.0) > bm25.score(1.0, 100.0, 5.0));
		assert!(bm25.score(3.0, 99.0, 5.0) > bm25.score(1.0, 99.0, 5.0));
	}
}
