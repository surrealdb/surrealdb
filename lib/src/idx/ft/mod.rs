mod docids;
mod doclength;
mod postings;
mod terms;

use crate::idx::ft::docids::DocIds;
use crate::idx::ft::doclength::{DocLength, DocLengths};
use crate::idx::ft::postings::{Postings, TermFrequency};
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
	average_doc_length: f32,
	bm25: Bm25Params,
}

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
	pub(super) fn new(kv: &mut KVSimulator, index_id: IndexId) -> Self {
		let state_key = BaseStateKey::new(INDEX_DOMAIN, index_id).into();
		let state = kv.get(&state_key).unwrap_or_else(State::default);
		let mut index = Self {
			state,
			state_key,
			index_id,
			average_doc_length: 0.0,
			bm25: Bm25Params::default(),
		};
		index.update_average_doc_length();
		index
	}

	fn update_average_doc_length(&mut self) {
		self.average_doc_length = if self.state.doc_count == 0 || self.state.total_docs_lengths == 0
		{
			0.0
		} else {
			self.state.total_docs_lengths as f32 / self.state.doc_count as f32
		}
	}

	fn add_document(&mut self, kv: &mut KVSimulator, doc_key: &str, field_content: &str) {
		// Resolve the doc_id
		let mut d = DocIds::new(kv, self.index_id, 100);
		let doc_id = d.resolve_doc_id(kv, doc_key);

		// Extract the doc_lengths, terms en frequencies
		let mut t: Terms = Terms::new(kv, self.index_id, 100);
		let (doc_length, terms_and_frequencies) =
			Self::extract_sorted_terms_with_frequencies(field_content);

		// Set the doc length
		let mut l = DocLengths::new(kv, self.index_id, 100);
		l.set_doc_length(kv, doc_id, doc_length);

		// Update the index state
		self.state.total_docs_lengths += doc_length as u128;
		self.state.doc_count += 1;
		self.update_average_doc_length();

		// Set the terms postings
		let terms = t.resolve_terms(kv, terms_and_frequencies);
		let mut p = Postings::new(kv, self.index_id, 100);
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

	// https://en.wikipedia.org/wiki/Okapi_BM25
	// Including the lower-bounding term frequency normalization (2011 CIKM)
	fn compute_bm25_score(&self, term_freq: f32, term_doc_count: f32, doc_length: f32) -> f32 {
		// (n(qi) + 0.5)
		let denominator = term_doc_count + 0.5;
		// (N - n(qi) + 0.5)
		let numerator = (self.state.doc_count as f32) - term_doc_count + 0.5;
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

	fn search(&self, kv: &mut KVSimulator, term: &str) -> Vec<(String, Score)> {
		let mut res = Vec::new();
		let t: Terms = Terms::new(kv, 0, 100);
		if let Some(term_id) = t.find_term(kv, term) {
			let p = Postings::new(kv, self.index_id, 100);
			let postings = p.get_postings(kv, term_id);
			if !postings.is_empty() {
				let term_doc_count = postings.len() as f32;
				let l = DocLengths::new(kv, self.index_id, 100);
				let d = DocIds::new(kv, self.index_id, 100);
				for (doc_id, term_freq) in postings {
					if let Some(doc_key) = d.get_doc_key(kv, doc_id) {
						let doc_length = l.get_doc_length(kv, doc_id).unwrap_or(0);
						let bm25_score = self.compute_bm25_score(
							term_freq as f32,
							term_doc_count,
							doc_length as f32,
						);
						res.push((doc_key, bm25_score));
					}
				}
			}
		}
		res
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::FtIndex;
	use crate::idx::kvsim::KVSimulator;

	#[test]
	fn test_ft_index() {
		let mut kv = KVSimulator::default();

		{
			// Add one document
			let mut fti = FtIndex::new(&mut kv, 0);
			fti.add_document(&mut kv, "doc1", "hello the world");
			assert_eq!(fti.average_doc_length, 3.0);
		}

		{
			// Add two documents
			let mut fti = FtIndex::new(&mut kv, 0);
			fti.add_document(&mut kv, "doc2", "a yellow hello");
			assert_eq!(fti.average_doc_length, 3.0);
			fti.add_document(&mut kv, "doc3", "foo bar");
			assert_eq!(fti.average_doc_length, 2.6666667);
		}

		{
			// Search & score
			let fti = FtIndex::new(&mut kv, 0);
			assert_eq!(
				fti.search(&mut kv, "hello"),
				vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]
			);
			assert_eq!(fti.search(&mut kv, "world"), vec![("doc1".to_string(), 0.4859746)]);
			assert_eq!(fti.search(&mut kv, "yellow"), vec![("doc2".to_string(), 0.4859746)]);
			assert_eq!(fti.search(&mut kv, "foo"), vec![("doc3".to_string(), 0.56902087)]);
			assert_eq!(fti.search(&mut kv, "bar"), vec![("doc3".to_string(), 0.56902087)]);
			assert_eq!(fti.search(&mut kv, "dummy"), Vec::<(String, f32)>::new());
		}
	}

	#[test]
	fn test_ft_index_bm_25() {
		let mut kv = KVSimulator::default();
		{
			let mut fti = FtIndex::new(&mut kv, 0);
			fti.add_document(&mut kv, "doc1", "the quick brown fox jumped over the lazy dog");
			fti.add_document(&mut kv, "doc2", "the fast fox jumped over the lazy dog");
			fti.add_document(&mut kv, "doc3", "the dog sat there and did nothing");
			fti.add_document(&mut kv, "doc4", "the other animals sat there watching");
		}

		{
			let fti = FtIndex::new(&mut kv, 0);
			assert_eq!(
				fti.search(&mut kv, "the"),
				vec![
					("doc1".to_string(), 0.0),
					("doc2".to_string(), 0.0),
					("doc3".to_string(), 0.0),
					("doc4".to_string(), 0.0)
				]
			);
			assert_eq!(
				fti.search(&mut kv, "dog"),
				vec![
					("doc1".to_string(), 0.0),
					("doc2".to_string(), 0.0),
					("doc3".to_string(), 0.0)
				]
			);
			assert_eq!(
				fti.search(&mut kv, "jumped"),
				vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]
			);
			assert_eq!(fti.search(&mut kv, "nothing"), vec![("doc3".to_string(), 0.87105393)]);
			assert_eq!(fti.search(&mut kv, "animals"), vec![("doc4".to_string(), 0.92279965)]);
			assert_eq!(fti.search(&mut kv, "dummy"), Vec::<(String, f32)>::new());
		}
	}
}
