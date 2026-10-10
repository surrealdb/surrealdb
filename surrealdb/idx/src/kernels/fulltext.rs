//! Full-text kernels: what a query's terms read, how what they read becomes
//! each term's document set, how the sets combine under a boolean operator,
//! and the BM25 score of one term in one document.
//!
//! [`highlight`] and [`offsets`] render the occurrences of a query's terms in
//! one document from the offsets its postings hold.
//!
//! A term's document set is spread over four places, and is their sum:
//!
//! - its compacted bitmap (`!td`), one document set;
//! - its residual (`!tr`), per-document totals the bitmap cannot hold;
//! - its uncompacted changes: per-document `!tt` keys an older server wrote, whose key alone
//!   records an addition or a removal, and batched `!tx` bitmaps;
//! - the changes the reading transaction buffered and has not yet written.
//!
//! [`term_reads`] names the keys and ranges of the first three, which the
//! caller reads; [`term_deltas`] folds the changes into signed per-document
//! counts and [`term_docs`] applies them, with the residual, to the bitmap. A
//! document belongs to the term when its total is positive; only the sign of a
//! change matters.

use ahash::HashMap;
use anyhow::Result;
use roaring::RoaringTreemap;

use crate::IndexKeyBase;
use crate::docids::DocId;
use crate::expr::Idiom;
use crate::expr::operator::BooleanOperator;
use crate::ft::fulltext::TermDocsResidual;
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
	/// The batched change bitmaps.
	pub batches: TypedRange<RoaringTreemap>,
	/// The residual's key, holding a [`TermDocsResidual`].
	pub residual: Key<'static>,
	/// The compacted bitmap's key, holding a [`RoaringTreemap`].
	pub bitmap: Key<'static>,
}

/// What a query reads of `term` in the index `ikb` names.
pub fn term_reads(ikb: &IndexKeyBase, term: &str) -> Result<TermReads> {
	Ok(TermReads {
		changes: ikb.new_tt_term_range(term)?,
		batches: ikb.new_tx_term_range(term)?,
		residual: ikb.new_tr_root(term).encode_key()?,
		bitmap: ikb.new_td_root(term).encode_key()?,
	})
}

/// One term's uncompacted changes, as its reads answered them and as the
/// reading transaction buffered them.
pub struct TermChanges<'a> {
	/// The keys of [`TermReads::changes`].
	pub change_keys: &'a [Vec<u8>],
	/// The keys and bitmaps of [`TermReads::batches`].
	pub batches: &'a [(Vec<u8>, RoaringTreemap)],
	/// The documents the transaction added to the term.
	pub pending_added: &'a RoaringTreemap,
	/// The documents the transaction removed from the term.
	pub pending_removed: &'a RoaringTreemap,
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
	for (k, docs) in changes.batches {
		let batch = TermChangeBatchKey::decode_key(k)?;
		let step = if batch.add {
			1
		} else {
			-1
		};
		for doc_id in docs {
			*deltas.entry(doc_id).or_default() += step;
		}
	}
	for doc_id in changes.pending_added {
		*deltas.entry(doc_id).or_default() += 1;
	}
	for doc_id in changes.pending_removed {
		*deltas.entry(doc_id).or_default() -= 1;
	}
	Ok(deltas)
}

/// The documents carrying a term: its compacted `bitmap` with its `residual`
/// and its `deltas` applied, or `None` when no document carries it.
pub fn term_docs(
	bitmap: Option<RoaringTreemap>,
	residual: Option<&TermDocsResidual>,
	mut deltas: HashMap<DocId, i64>,
) -> Option<RoaringTreemap> {
	if let Some(residual) = residual {
		for (doc_id, count) in residual.counts.iter() {
			*deltas.entry(*doc_id).or_default() += *count;
		}
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
	/// The inverse document frequency is clamped at zero, so a term most
	/// documents carry weighs nothing rather than counting against a
	/// document, and the term frequency is lower-bounded as `1 + ln(tf)`
	/// (Lv & Zhai, "Lower-Bounding Term Frequency Normalization", CIKM 2011):
	///
	/// ```text
	/// score = idf · (k1 + 1) · tf′ / (tf′ + k1 · (1 − b + b · doc_len / avg_doc_len))
	/// idf   = max(0, ln((N − n + 0.5) / (n + 0.5)))
	/// tf′   = 1 + ln(tf)
	/// ```
	pub fn score(&self, term_freq: f64, term_doc_count: f64, doc_length: f64) -> f64 {
		if term_freq <= 0.0 {
			return 0.0;
		}
		// The inverse document frequency is the logarithm of this ratio, so it
		// is not positive where the ratio is at most one — including where
		// more documents carry the term than the index counts, which makes the
		// numerator negative and its logarithm undefined.
		let numerator = self.doc_count - term_doc_count + 0.5;
		let denominator = term_doc_count + 0.5;
		if numerator <= denominator {
			return 0.0;
		}
		let idf = (numerator / denominator).ln();
		let tf_prime = 1.0 + term_freq.ln();
		let length_norm = self.one_minus_b + self.b_over_avg_len * doc_length;
		idf * self.k1_plus_1 * tf_prime / (tf_prime + self.k1 * length_norm)
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
	fn a_term_more_documents_carry_than_the_index_counts_weighs_nothing() {
		// Postings that run ahead of the document count leave the ratio the
		// logarithm is taken of below zero.
		let bm25 = Bm25::new(1.2, 0.75, 3, 30);
		let score = bm25.score(2.0, 5.0, 10.0);
		assert_eq!(score, 0.0);
		assert!(score.is_finite());
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

	#[test]
	fn bm25_weighs_rarer_terms_and_shorter_documents_higher() {
		let bm25 = Bm25::new(1.2, 0.75, 100, 1000);
		let common = bm25.score(1.0, 50.0, 10.0);
		let rare = bm25.score(1.0, 2.0, 10.0);
		assert!(rare > common, "{rare} vs {common}");
		assert!(bm25.score(1.0, 2.0, 5.0) > bm25.score(1.0, 2.0, 20.0));
		assert_eq!(bm25.score(0.0, 2.0, 5.0), 0.0);
		assert_eq!(bm25.score(1.0, 99.0, 5.0), 0.0, "a term most documents carry weighs nothing");
	}
}
