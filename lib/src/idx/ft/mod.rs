pub(crate) mod docids;
mod doclength;
mod postings;
pub(crate) mod terms;

use crate::err::Error;
use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::doclength::{DocLength, DocLengths};
use crate::idx::ft::postings::{Postings, PostingsVisitor, TermFrequency};
use crate::idx::ft::terms::Terms;
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use crate::sql::error::IResult;
use async_trait::async_trait;
use nom::bytes::complete::take_while;
use nom::character::complete::multispace0;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub(crate) struct FtIndex {
	state_key: Key,
	index_key_base: IndexKeyBase,
	state: State,
	bm25: Bm25Params,
	btree_default_order: usize,
}

pub(crate) trait HitVisitor {
	fn visit(&mut self, tx: &mut Transaction, doc_key: String, score: Score);
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

pub(super) struct Statistics {
	doc_ids: btree::Statistics,
	terms: btree::Statistics,
	doc_lengths: btree::Statistics,
	postings: btree::Statistics,
}

#[derive(Default, Serialize, Deserialize)]
struct State {
	total_docs_lengths: u128,
	doc_count: u64,
}

impl SerdeState for State {}

type Score = f32;

impl FtIndex {
	pub(crate) async fn new(
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		btree_default_order: usize,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bs_key();
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::default()
		};
		Ok(Self {
			state,
			state_key,
			index_key_base,
			bm25: Bm25Params::default(),
			btree_default_order,
		})
	}

	async fn doc_ids(&self, tx: &mut Transaction) -> Result<DocIds, Error> {
		DocIds::new(tx, self.index_key_base.clone(), self.btree_default_order).await
	}

	async fn terms(&self, tx: &mut Transaction) -> Result<Terms, Error> {
		Terms::new(tx, self.index_key_base.clone(), self.btree_default_order).await
	}

	async fn doc_lengths(&self, tx: &mut Transaction) -> Result<DocLengths, Error> {
		DocLengths::new(tx, self.index_key_base.clone(), self.btree_default_order).await
	}

	async fn postings(&self, tx: &mut Transaction) -> Result<Postings, Error> {
		Postings::new(tx, self.index_key_base.clone(), self.btree_default_order).await
	}

	pub(crate) async fn remove_document(
		&mut self,
		_tx: &mut Transaction,
		_doc_key: &str,
	) -> Result<(), Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "FT - Remove document",
		})
	}

	pub(crate) async fn index_document(
		&mut self,
		tx: &mut Transaction,
		doc_key: &str,
		field_content: &str,
	) -> Result<(), Error> {
		// Resolve the doc_id
		let mut d = self.doc_ids(tx).await?;
		let doc_id = d.resolve_doc_id(tx, doc_key).await?;

		// Extract the doc_lengths, terms en frequencies
		let mut t = self.terms(tx).await?;
		let (doc_length, terms_and_frequencies) =
			Self::extract_sorted_terms_with_frequencies(field_content);

		// Set the doc length
		let mut l = self.doc_lengths(tx).await?;
		l.set_doc_length(tx, doc_id, doc_length).await?;

		// Update the index state
		self.state.total_docs_lengths += doc_length as u128;
		self.state.doc_count += 1;

		// Set the terms postings
		let terms = t.resolve_terms(tx, terms_and_frequencies).await?;
		let mut p = self.postings(tx).await?;
		for (term_id, term_freq) in terms {
			p.update_posting(tx, term_id, doc_id, term_freq).await?;
		}

		// Update the states
		tx.set(self.state_key.clone(), self.state.try_to_val()?).await?;
		d.finish(tx).await?;
		l.finish(tx).await?;
		p.finish(tx).await?;
		t.finish(tx).await?;
		Ok(())
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

	pub(super) async fn search<V>(
		&self,
		tx: &mut Transaction,
		term: &str,
		visitor: &mut V,
	) -> Result<(), Error>
	where
		V: HitVisitor + Send,
	{
		let terms = self.terms(tx).await?;
		if let Some(term_id) = terms.find_term(tx, term).await? {
			let postings = self.postings(tx).await?;
			let term_doc_count = postings.get_doc_count(tx, term_id).await?;
			let doc_lengths = self.doc_lengths(tx).await?;
			let doc_ids = self.doc_ids(tx).await?;
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
				postings.collect_postings(tx, term_id, &mut scorer).await?;
			}
		}
		Ok(())
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		// TODO do parallel execution
		Ok(Statistics {
			doc_ids: self.doc_ids(tx).await?.statistics(tx).await?,
			terms: self.terms(tx).await?.statistics(tx).await?,
			doc_lengths: self.doc_lengths(tx).await?.statistics(tx).await?,
			postings: self.postings(tx).await?.statistics(tx).await?,
		})
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

#[async_trait]
impl<'a, V> PostingsVisitor for BM25Scorer<'a, V>
where
	V: HitVisitor + Send,
{
	async fn visit(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_frequency: TermFrequency,
	) -> Result<(), Error> {
		if let Some(doc_key) = self.doc_ids.get_doc_key(tx, doc_id).await? {
			let doc_length = self.doc_lengths.get_doc_length(tx, doc_id).await?.unwrap_or(0);
			let bm25_score = self.compute_bm25_score(
				term_frequency as f32,
				self.term_doc_count,
				doc_length as f32,
			);
			self.visitor.visit(tx, doc_key, bm25_score);
		}
		Ok(())
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
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};
	use std::collections::HashMap;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_ft_index() {
		let ds = Datastore::new("memory").await.unwrap();

		let default_btree_order = 5;

		{
			// Add one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, "doc1", "hello the world").await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			// Add two documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, "doc2", "a yellow hello").await.unwrap();
			fti.index_document(&mut tx, "doc3", "foo bar").await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = ds.transaction(true, false).await.unwrap();
			let fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();

			// Check the statistics
			let statistics = fti.statistics(&mut tx).await.unwrap();
			assert_eq!(statistics.terms.keys_count, 7);
			assert_eq!(statistics.postings.keys_count, 8);
			assert_eq!(statistics.doc_ids.keys_count, 3);
			assert_eq!(statistics.doc_lengths.keys_count, 3);

			// Search & score
			let mut visitor = HashHitVisitor::default();
			fti.search(&mut tx, "hello", &mut visitor).await.unwrap();
			visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut tx, "world", &mut visitor).await.unwrap();
			visitor.check(vec![("doc1".to_string(), 0.4859746)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut tx, "yellow", &mut visitor).await.unwrap();
			visitor.check(vec![("doc2".to_string(), 0.4859746)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut tx, "foo", &mut visitor).await.unwrap();
			visitor.check(vec![("doc3".to_string(), 0.56902087)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut tx, "bar", &mut visitor).await.unwrap();
			visitor.check(vec![("doc3".to_string(), 0.56902087)]);

			let mut visitor = HashHitVisitor::default();
			fti.search(&mut tx, "dummy", &mut visitor).await.unwrap();
			visitor.check(Vec::<(String, f32)>::new());
		}
	}

	#[test(tokio::test)]
	async fn test_ft_index_bm_25() {
		// The function `extract_sorted_terms_with_frequencies` is non-deterministic.
		// the inner structures (BTrees) are built with the same terms and frequencies,
		// but the insertion order is different, ending up in different BTree structures.
		// Therefore it makes sense to do multiple runs.
		for _ in 0..10 {
			let ds = Datastore::new("memory").await.unwrap();

			let default_btree_order = 5;
			{
				let mut tx = ds.transaction(true, false).await.unwrap();
				let mut fti = FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();
				fti.index_document(&mut tx, "doc1", "the quick brown fox jumped over the lazy dog")
					.await
					.unwrap();
				fti.index_document(&mut tx, "doc2", "the fast fox jumped over the lazy dog")
					.await
					.unwrap();
				fti.index_document(&mut tx, "doc3", "the dog sat there and did nothing")
					.await
					.unwrap();
				fti.index_document(&mut tx, "doc4", "the other animals sat there watching")
					.await
					.unwrap();
				tx.commit().await.unwrap();
			}

			{
				let mut tx = ds.transaction(true, false).await.unwrap();
				let fti = FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();

				let statistics = fti.statistics(&mut tx).await.unwrap();
				assert_eq!(statistics.terms.keys_count, 17);
				assert_eq!(statistics.postings.keys_count, 28);
				assert_eq!(statistics.doc_ids.keys_count, 4);
				assert_eq!(statistics.doc_lengths.keys_count, 4);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "the", &mut visitor).await.unwrap();
				visitor.check(vec![
					("doc1".to_string(), 0.0),
					("doc2".to_string(), 0.0),
					("doc3".to_string(), 0.0),
					("doc4".to_string(), 0.0),
				]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "dog", &mut visitor).await.unwrap();
				visitor.check(vec![
					("doc1".to_string(), 0.0),
					("doc2".to_string(), 0.0),
					("doc3".to_string(), 0.0),
				]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "fox", &mut visitor).await.unwrap();
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "over", &mut visitor).await.unwrap();
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "lazy", &mut visitor).await.unwrap();
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "jumped", &mut visitor).await.unwrap();
				visitor.check(vec![("doc1".to_string(), 0.0), ("doc2".to_string(), 0.0)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "nothing", &mut visitor).await.unwrap();
				visitor.check(vec![("doc3".to_string(), 0.87105393)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "animals", &mut visitor).await.unwrap();
				visitor.check(vec![("doc4".to_string(), 0.92279965)]);

				let mut visitor = HashHitVisitor::default();
				fti.search(&mut tx, "dummy", &mut visitor).await.unwrap();
				visitor.check(Vec::<(String, f32)>::new());
			}
		}
	}

	#[derive(Default)]
	pub(super) struct HashHitVisitor {
		map: HashMap<String, Score>,
	}

	impl HitVisitor for HashHitVisitor {
		fn visit(&mut self, _tx: &mut Transaction, doc_key: String, score: Score) {
			self.map.insert(doc_key, score);
		}
	}

	impl HashHitVisitor {
		pub(super) fn check(&self, res: Vec<(String, Score)>) {
			assert_eq!(res.len(), self.map.len(), "{:?}", self.map);
			for (k, p) in res {
				assert_eq!(self.map.get(&k), Some(&p));
			}
		}
	}
}
