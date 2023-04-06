mod docids;
mod doclength;
mod postings;
mod terms;

use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::doclength::{DocLength, DocLengths};
use crate::idx::ft::postings::{Postings, PostingsVisitor, TermFrequency};
use crate::idx::ft::terms::Terms;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{BaseStateKey, IndexId, INDEX_DOMAIN};
use crate::kvs::Key;
use crate::sql::error::IResult;
use nom::bytes::complete::take_while;
use nom::character::complete::multispace0;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

struct FtIndex {
	state_key: Key,
	state: State,
	index_id: IndexId,
	bm25: Bm25Params,
	btree_default_order: usize,
}

trait HitVisitor {
	fn visit(&mut self, kv: &mut KVSimulator, doc_key: String, score: Score);
}

#[derive(Clone)]
struct Bm25Params {
	k1: f32,
	b: f32,
}

impl Default for Bm25Params {
	fn default() -> Self {
		Self {
			k1: 1.2,
			b: 0.75,
		}
	}
}

#[derive(Default, Serialize, Deserialize)]
struct State {
	total_docs_lengths: u128,
	doc_count: u64,
}

type Score = f32;

impl FtIndex {
	pub(super) fn new(kv: &mut KVSimulator, index_id: IndexId, btree_default_order: usize) -> Self {
		let state_key = BaseStateKey::new(INDEX_DOMAIN, index_id).into();
		let state = kv.get(&state_key).unwrap_or_default();
		Self {
			state,
			state_key,
			index_id,
			bm25: Bm25Params::default(),
			btree_default_order,
		}
	}

	fn doc_ids(&self, kv: &mut KVSimulator) -> DocIds {
		DocIds::new(kv, self.index_id, self.btree_default_order)
	}

	fn terms(&self, kv: &mut KVSimulator) -> Terms {
		Terms::new(kv, self.index_id, self.btree_default_order)
	}

	fn doc_lengths(&self, kv: &mut KVSimulator) -> DocLengths {
		DocLengths::new(kv, self.index_id, self.btree_default_order)
	}

	fn postings(&self, kv: &mut KVSimulator) -> Postings {
		Postings::new(kv, self.index_id, self.btree_default_order)
	}

	fn add_document(&mut self, kv: &mut KVSimulator, doc_key: &str, field_content: &str) {
		// Resolve the doc_id
		let mut d = self.doc_ids(kv);
		let doc_id = d.resolve_doc_id(kv, doc_key);

		// Extract the doc_lengths, terms en frequencies
		let mut t = self.terms(kv);
		let (doc_length, terms_and_frequencies) =
			Self::extract_sorted_terms_with_frequencies(field_content);

		// Set the doc length
		let mut l = self.doc_lengths(kv);
		l.set_doc_length(kv, doc_id, doc_length);

		// Update the index state
		self.state.total_docs_lengths += doc_length as u128;
		self.state.doc_count += 1;

		// Set the terms postings
		let terms = t.resolve_terms(kv, terms_and_frequencies);
		let mut p = self.postings(kv);
		for (term_id, term_freq) in terms {
			p.update_posting(kv, term_id, doc_id, term_freq);
		}

		// Update the states
		kv.set(self.state_key.clone(), &self.state);
		d.finish(kv);
		l.finish(kv);
		p.finish(kv);
		t.finish(kv);
	}

	fn extract_sorted_terms_with_frequencies(
		input: &str,
	) -> (DocLength, HashMap<&str, TermFrequency>) {
		let mut doc_length = 0;
		let mut terms = HashMap::new();
		let mut rest = input;
		loop {
			// Skip whitespace
			let (remaining_input, _) =
				multispace0::<_, ()>(rest).unwrap_or_else(|e| panic!("multispace0 {:?}", e));
			if remaining_input.is_empty() {
				break;
			}
			rest = remaining_input;

			// Tokenize
			let (remaining_input, token) = Self::tokenize(rest).unwrap();
			if !input.is_empty() {
				doc_length += 1;
				match terms.entry(token) {
					Entry::Vacant(e) => {
						e.insert(1);
					}
					Entry::Occupied(mut e) => {
						e.insert(*e.get() + 1);
					}
				}
			}
			rest = remaining_input;
		}
		(doc_length, terms)
	}

	fn tokenize(i: &str) -> IResult<&str, &str> {
		take_while(|c| c != ' ' && c != '\n' && c != '\t')(i)
	}

	fn search<V>(&self, kv: &mut KVSimulator, term: &str, visitor: &mut V)
	where
		V: HitVisitor,
	{
		let terms = self.terms(kv);
		if let Some(term_id) = terms.find_term(kv, term) {
			let postings = self.postings(kv);
			let term_doc_count = postings.get_doc_count(kv, term_id);
			let doc_lengths = self.doc_lengths(kv);
			let doc_ids = self.doc_ids(kv);
			if term_doc_count > 0 {
				let mut scorer = BM25Scorer::new(
					visitor,
					doc_lengths,
					doc_ids,
					self.state.total_docs_lengths,
					self.state.doc_count,
					term_doc_count,
					self.bm25.clone(),
				);
				postings.collect_postings(kv, term_id, &mut scorer);
				terms.debug(kv);
				postings.debug(kv);
			}
		}
	}
}

struct BM25Scorer<'a, V>
where
	V: HitVisitor,
{
	visitor: &'a mut V,
	doc_lengths: DocLengths,
	doc_ids: DocIds,
	average_doc_length: f32,
	doc_count: f32,
	term_doc_count: f32,
	bm25: Bm25Params,
}

impl<'a, V> PostingsVisitor for BM25Scorer<'a, V>
where
	V: HitVisitor,
{
	fn visit(&mut self, kv: &mut KVSimulator, doc_id: DocId, term_frequency: TermFrequency) {
		if let Some(doc_key) = self.doc_ids.get_doc_key(kv, doc_id) {
			let doc_length = self.doc_lengths.get_doc_length(kv, doc_id).unwrap_or(0);
			let bm25_score = self.compute_bm25_score(
				term_frequency as f32,
				self.term_doc_count,
				doc_length as f32,
			);
			self.visitor.visit(kv, doc_key, bm25_score);
		}
	}
}

impl<'a, V> BM25Scorer<'a, V>
where
	V: HitVisitor,
{
	fn new(
		visitor: &'a mut V,
		doc_lengths: DocLengths,
		doc_ids: DocIds,
		total_docs_length: u128,
		doc_count: u64,
		term_doc_count: u64,
		bm25: Bm25Params,
	) -> Self {
		Self {
			visitor,
			doc_lengths,
			doc_ids,
			average_doc_length: (total_docs_length as f32) / (doc_count as f32),
			doc_count: doc_count as f32,
			term_doc_count: term_doc_count as f32,
			bm25,
		}
	}

	// https://en.wikipedia.org/wiki/Okapi_BM25
	// Including the lower-bounding term frequency normalization (2011 CIKM)
	fn compute_bm25_score(&self, term_freq: f32, term_doc_count: f32, doc_length: f32) -> f32 {
		// (n(qi) + 0.5)
		let denominator = term_doc_count + 0.5;
		// (N - n(qi) + 0.5)
		let numerator = self.doc_count - term_doc_count + 0.5;
		let idf = (numerator / denominator).ln();
		if idf.is_nan() || idf <= 0.0 {
			return 0.0;
		}
		let tf_prim = 1.0 + term_freq.ln();
		// idf * (k1 + 1)
		let numerator = idf * (self.bm25.k1 + 1.0) * tf_prim;
		// 1 - b + b * (|D| / avgDL)
		let denominator = 1.0 - self.bm25.b + self.bm25.b * (doc_length / self.average_doc_length);
		// numerator / (k1 * denominator + 1)
		numerator / (self.bm25.k1 * denominator + 1.0)
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::{FtIndex, HitVisitor, Score};
	use crate::idx::kvsim::KVSimulator;
	use std::collections::HashMap;
	use test_log::test;

	#[test]
	fn test_ft_index() {
		let mut kv = KVSimulator::default();
		let default_btree_order = 200;

		{
			// Add one document
			let mut fti = FtIndex::new(&mut kv, 0, default_btree_order);
			fti.add_document(&mut kv, "doc1", "hello the world");
		}

		{
			// Add two documents
			let mut fti = FtIndex::new(&mut kv, 0, default_btree_order);
			fti.add_document(&mut kv, "doc2", "a yellow hello");
			fti.add_document(&mut kv, "doc3", "foo bar");
		}

		{
			// Search & score
			let fti = FtIndex::new(&mut kv, 0, default_btree_order);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut kv, "hello", &mut visitor);
			visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut kv, "world", &mut visitor);
			visitor.check(vec![("doc1".to_string(), 0.4859746)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut kv, "yellow", &mut visitor);
			visitor.check(vec![("doc2".to_string(), 0.4859746)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut kv, "foo", &mut visitor);
			visitor.check(vec![("doc3".to_string(), 0.56902087)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut kv, "bar", &mut visitor);
			visitor.check(vec![("doc3".to_string(), 0.56902087)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut kv, "dummy", &mut visitor);
			visitor.check(Vec::<(String, f32)>::new());
		}
	}

	#[test]
	fn test_ft_index_bm_25() {
		// The function `extract_sorted_terms_with_frequencies` is non-deterministic.
		// the inner structures (BTrees) are built with the same terms and frequencies,
		// but the insertion order is different, ending up in different BTree structures.
		// Therefore it makes sense to do multiple runs.
		for _ in 0..10 {
			let mut kv = KVSimulator::default();
			let default_btree_order = 75;
			{
				let mut fti = FtIndex::new(&mut kv, 0, default_btree_order);
				fti.add_document(&mut kv, "doc1", "the quick brown fox jumped over the lazy dog");
				fti.add_document(&mut kv, "doc2", "the fast fox jumped over the lazy dog");
				fti.add_document(&mut kv, "doc3", "the dog sat there and did nothing");
				fti.add_document(&mut kv, "doc4", "the other animals sat there watching");
			}
			{
				let fti = FtIndex::new(&mut kv, 0, default_btree_order);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "the", &mut visitor);
				visitor.check(vec![
					("doc1".to_string(), 0.0),
					("doc2".to_string(), 0.0),
					("doc3".to_string(), 0.0),
					("doc4".to_string(), 0.0),
				]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "dog", &mut visitor);
				visitor.check(vec![
					("doc1".to_string(), 0.0),
					("doc2".to_string(), 0.0),
					("doc3".to_string(), 0.0),
				]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "fox", &mut visitor);
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "over", &mut visitor);
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "lazy", &mut visitor);
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "jumped", &mut visitor);
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "nothing", &mut visitor);
				visitor.check(vec![("doc3".to_string(), 0.87105393)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "animals", &mut visitor);
				visitor.check(vec![("doc4".to_string(), 0.92279965)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut kv, "dummy", &mut visitor);
				visitor.check(Vec::<(String, f32)>::new());
			}
		}
	}

	#[derive(Default)]
	pub(super) struct HashHitVisitor {
		map: HashMap<String, Score>,
	}

	impl HitVisitor for HashHitVisitor {
		fn visit(&mut self, _kv: &mut KVSimulator, doc_key: String, score: Score) {
			self.map.insert(doc_key, score);
		}
	}

	impl HashHitVisitor {
		pub(super) fn check(&self, res: Vec<(String, Score)>) {
			assert_eq!(res.len(), self.map.len());
			for (k, p) in res {
				assert_eq!(self.map.get(&k), Some(&p));
			}
		}
	}
}
