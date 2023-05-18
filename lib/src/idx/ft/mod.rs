pub(crate) mod docids;
mod doclength;
mod postings;
pub(crate) mod terms;

use crate::err::Error;
use crate::error::Db::AnalyzerError;
use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::doclength::{DocLength, DocLengths};
use crate::idx::ft::postings::{Postings, PostingsIterator, TermFrequency};
use crate::idx::ft::terms::{TermId, Terms};
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use crate::sql::error::IResult;
use nom::bytes::complete::take_while;
use nom::character::complete::multispace0;
use roaring::RoaringTreemap;
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
		tx: &mut Transaction,
		doc_key: Key,
	) -> Result<(), Error> {
		// Extract and remove the doc_id (if any)
		let mut d = self.doc_ids(tx).await?;
		if let Some(doc_id) = d.remove_doc(tx, doc_key).await? {
			self.state.doc_count -= 1;

			// Remove the doc length
			let mut l = self.doc_lengths(tx).await?;
			if let Some(doc_lengths) = l.remove_doc_length(tx, doc_id).await? {
				self.state.total_docs_lengths -= doc_lengths as u128;
				l.finish(tx).await?;
			}

			// Get the term list
			if let Some(term_list_vec) = tx.get(self.index_key_base.new_bk_key(doc_id)).await? {
				let term_list = RoaringTreemap::try_from_val(term_list_vec)?;
				// Remove the postings
				let mut p = self.postings(tx).await?;
				let mut t = self.terms(tx).await?;
				for term_id in term_list {
					p.remove_posting(tx, term_id, doc_id).await?;
					// if the term is not present in any document in the index, we can remove it
					if p.count_postings(tx, term_id).await? == 0 {
						t.remove_term_id(tx, term_id).await?;
					}
				}
				t.finish(tx).await?;
				p.finish(tx).await?;
			}

			d.finish(tx).await?;
		}
		Ok(())
	}

	pub(crate) async fn index_document(
		&mut self,
		tx: &mut Transaction,
		doc_key: Key,
		field_content: &str,
	) -> Result<(), Error> {
		// Resolve the doc_id
		let mut d = self.doc_ids(tx).await?;
		let resolved = d.resolve_doc_id(tx, doc_key).await?;
		let doc_id = *resolved.doc_id();

		// Extract the doc_lengths, terms en frequencies
		let mut t = self.terms(tx).await?;
		let (doc_length, terms_and_frequencies) =
			Self::extract_sorted_terms_with_frequencies(field_content)?;

		// Set the doc length
		let mut l = self.doc_lengths(tx).await?;
		if resolved.was_existing() {
			if let Some(old_doc_length) = l.get_doc_length(tx, doc_id).await? {
				self.state.total_docs_lengths -= old_doc_length as u128;
			}
		}
		l.set_doc_length(tx, doc_id, doc_length).await?;

		// Retrieve the existing terms for this document (if any)
		let term_ids_key = self.index_key_base.new_bk_key(doc_id);
		let mut old_term_ids = if let Some(val) = tx.get(term_ids_key.clone()).await? {
			Some(RoaringTreemap::try_from_val(val)?)
		} else {
			None
		};

		// Set the terms postings
		let terms = t.resolve_term_ids(tx, terms_and_frequencies).await?;
		let mut terms_ids = RoaringTreemap::default();
		let mut p = self.postings(tx).await?;
		for (term_id, term_freq) in terms {
			p.update_posting(tx, term_id, doc_id, term_freq).await?;
			if let Some(old_term_ids) = &mut old_term_ids {
				old_term_ids.remove(term_id);
			}
			terms_ids.insert(term_id);
		}

		// Remove any remaining postings
		if let Some(old_term_ids) = old_term_ids {
			for old_term_id in old_term_ids {
				p.remove_posting(tx, old_term_id, doc_id).await?;
				// if the term does not have anymore postings, we can remove the term
				if p.count_postings(tx, old_term_id).await? == 0 {
					t.remove_term_id(tx, old_term_id).await?;
				}
			}
		}

		// Stores the term list for this doc_id
		tx.set(term_ids_key, terms_ids.try_to_val()?).await?;

		// Update the index state
		self.state.total_docs_lengths += doc_length as u128;
		if !resolved.was_existing() {
			self.state.doc_count += 1;
		}

		// Update the states
		tx.set(self.state_key.clone(), self.state.try_to_val()?).await?;
		d.finish(tx).await?;
		l.finish(tx).await?;
		p.finish(tx).await?;
		t.finish(tx).await?;
		Ok(())
	}

	// TODO: This is currently a place holder. It has to be replaced by the analyzer/token/filter logic.
	fn extract_sorted_terms_with_frequencies(
		input: &str,
	) -> Result<(DocLength, HashMap<&str, TermFrequency>), Error> {
		let mut doc_length = 0;
		let mut terms = HashMap::new();
		let mut rest = input;
		while !rest.is_empty() {
			// Extract the next token
			match Self::next_token(rest) {
				Ok((remaining_input, token)) => {
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
				Err(e) => return Err(AnalyzerError(e.to_string())),
			}
		}
		Ok((doc_length, terms))
	}

	/// Extracting the next token. The string is left trimmed first.
	fn next_token(i: &str) -> IResult<&str, &str> {
		let (i, _) = multispace0(i)?;
		take_while(|c| c != ' ' && c != '\n' && c != '\t')(i)
	}

	pub(super) async fn search(
		&self,
		tx: &mut Transaction,
		term: &str,
	) -> Result<HitsIterator, Error> {
		let terms = self.terms(tx).await?;
		if let Some(term_id) = terms.get_term_id(tx, term).await? {
			let postings = self.postings(tx).await?;
			let term_doc_count = postings.count_postings(tx, term_id).await?;
			let doc_lengths = self.doc_lengths(tx).await?;
			if term_doc_count > 0 {
				// TODO: Score can be optional
				let scorer = BM25Scorer::new(
					doc_lengths,
					self.state.total_docs_lengths,
					self.state.doc_count,
					term_doc_count as u64,
					self.bm25.clone(),
				);
				let doc_ids = self.doc_ids(tx).await?;
				return Ok(HitsIterator::new(Some((doc_ids, postings, term_id)), Some(scorer)));
			}
		}
		Ok(HitsIterator::new(None, None))
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

pub(crate) struct HitsIterator {
	docs: Option<(DocIds, PostingsIterator)>,
	scorer: Option<BM25Scorer>,
}

impl HitsIterator {
	fn new(ft: Option<(DocIds, Postings, TermId)>, scorer: Option<BM25Scorer>) -> Self {
		if let Some((doc_ids, postings, term_id)) = ft {
			let postings = postings.collect_postings(term_id);
			Self {
				docs: Some((doc_ids, postings)),
				scorer,
			}
		} else {
			Self {
				docs: None,
				scorer,
			}
		}
	}

	pub(crate) async fn next(
		&mut self,
		tx: &mut Transaction,
	) -> Result<Option<(Key, Option<Score>)>, Error> {
		if let Some((doc_ids, postings)) = &mut self.docs {
			if let Some((doc_id, term_freq)) = postings.next(tx).await? {
				if let Some(doc_key) = doc_ids.get_doc_key(tx, doc_id).await? {
					let score = if let Some(scorer) = &self.scorer {
						Some(scorer.score(tx, doc_id, term_freq).await?)
					} else {
						None
					};
					return Ok(Some((doc_key, score)));
				}
			}
		}
		Ok(None)
	}
}

struct BM25Scorer {
	doc_lengths: DocLengths,
	average_doc_length: f32,
	doc_count: f32,
	term_doc_count: f32,
	bm25: Bm25Params,
}

impl BM25Scorer {
	fn new(
		doc_lengths: DocLengths,
		total_docs_length: u128,
		doc_count: u64,
		term_doc_count: u64,
		bm25: Bm25Params,
	) -> Self {
		Self {
			doc_lengths,
			average_doc_length: (total_docs_length as f32) / (doc_count as f32),
			doc_count: doc_count as f32,
			term_doc_count: term_doc_count as f32,
			bm25,
		}
	}

	async fn score(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_frequency: TermFrequency,
	) -> Result<Score, Error> {
		let doc_length = self.doc_lengths.get_doc_length(tx, doc_id).await?.unwrap_or(0);
		Ok(self.compute_bm25_score(term_frequency as f32, self.term_doc_count, doc_length as f32))
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
	use crate::idx::ft::{FtIndex, HitsIterator, Score};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Key, Transaction};
	use std::collections::HashMap;
	use test_log::test;

	async fn check_hits(mut i: HitsIterator, tx: &mut Transaction, e: Vec<(Key, Option<Score>)>) {
		let mut map = HashMap::new();
		while let Some((k, s)) = i.next(tx).await.unwrap() {
			map.insert(k, s);
		}
		assert_eq!(map.len(), e.len());
		for (k, p) in e {
			assert_eq!(map.get(&k), Some(&p));
		}
	}

	#[test(tokio::test)]
	async fn test_ft_index() {
		let ds = Datastore::new("memory").await.unwrap();

		let default_btree_order = 5;

		{
			// Add one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, "doc1".into(), "hello the world").await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			// Add two documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, "doc2".into(), "a yellow hello").await.unwrap();
			fti.index_document(&mut tx, "doc3".into(), "foo bar").await.unwrap();
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
			let i = fti.search(&mut tx, "hello").await.unwrap();
			check_hits(i, &mut tx, vec![("doc1".into(), Some(0.0)), ("doc2".into(), Some(0.0))])
				.await;

			let i = fti.search(&mut tx, "world").await.unwrap();
			check_hits(i, &mut tx, vec![("doc1".into(), Some(0.4859746))]).await;

			let i = fti.search(&mut tx, "yellow").await.unwrap();
			check_hits(i, &mut tx, vec![("doc2".into(), Some(0.4859746))]).await;

			let i = fti.search(&mut tx, "foo").await.unwrap();
			check_hits(i, &mut tx, vec![("doc3".into(), Some(0.56902087))]).await;

			let i = fti.search(&mut tx, "bar").await.unwrap();
			check_hits(i, &mut tx, vec![("doc3".into(), Some(0.56902087))]).await;

			let i = fti.search(&mut tx, "dummy").await.unwrap();
			check_hits(i, &mut tx, vec![]).await;
		}

		{
			// Reindex one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, "doc3".into(), "nobar foo").await.unwrap();
			tx.commit().await.unwrap();

			// We can still find 'foo'
			let mut tx = ds.transaction(false, false).await.unwrap();
			let i = fti.search(&mut tx, "foo").await.unwrap();
			check_hits(i, &mut tx, vec![("doc3".into(), Some(0.56902087))]).await;

			// We can't anymore find 'bar'
			let i = fti.search(&mut tx, "bar").await.unwrap();
			check_hits(i, &mut tx, vec![]).await;

			// We can now find 'nobar'
			let i = fti.search(&mut tx, "nobar").await.unwrap();
			check_hits(i, &mut tx, vec![("doc3".into(), Some(0.56902087))]).await;
		}

		{
			// Remove documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.remove_document(&mut tx, "doc1".into()).await.unwrap();
			fti.remove_document(&mut tx, "doc2".into()).await.unwrap();
			fti.remove_document(&mut tx, "doc3".into()).await.unwrap();
			tx.commit().await.unwrap();

			let mut tx = ds.transaction(false, false).await.unwrap();
			let i = fti.search(&mut tx, "hello").await.unwrap();
			check_hits(i, &mut tx, vec![]).await;

			let i = fti.search(&mut tx, "foo").await.unwrap();
			check_hits(i, &mut tx, vec![]).await;
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
				fti.index_document(
					&mut tx,
					"doc1".into(),
					"the quick brown fox jumped over the lazy dog",
				)
				.await
				.unwrap();
				fti.index_document(&mut tx, "doc2".into(), "the fast fox jumped over the lazy dog")
					.await
					.unwrap();
				fti.index_document(&mut tx, "doc3".into(), "the dog sat there and did nothing")
					.await
					.unwrap();
				fti.index_document(&mut tx, "doc4".into(), "the other animals sat there watching")
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

				let i = fti.search(&mut tx, "the").await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![
						("doc1".into(), Some(0.0)),
						("doc2".into(), Some(0.0)),
						("doc3".into(), Some(0.0)),
						("doc4".into(), Some(0.0)),
					],
				)
				.await;

				let i = fti.search(&mut tx, "dog").await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![
						("doc1".into(), Some(0.0)),
						("doc2".into(), Some(0.0)),
						("doc3".into(), Some(0.0)),
					],
				)
				.await;

				let i = fti.search(&mut tx, "fox").await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![("doc1".into(), Some(0.0)), ("doc2".into(), Some(0.0))],
				)
				.await;

				let i = fti.search(&mut tx, "over").await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![("doc1".into(), Some(0.0)), ("doc2".into(), Some(0.0))],
				)
				.await;

				let i = fti.search(&mut tx, "lazy").await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![("doc1".into(), Some(0.0)), ("doc2".into(), Some(0.0))],
				)
				.await;

				let i = fti.search(&mut tx, "jumped").await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![("doc1".into(), Some(0.0)), ("doc2".into(), Some(0.0))],
				)
				.await;

				let i = fti.search(&mut tx, "nothing").await.unwrap();
				check_hits(i, &mut tx, vec![("doc3".into(), Some(0.87105393))]).await;

				let i = fti.search(&mut tx, "animals").await.unwrap();
				check_hits(i, &mut tx, vec![("doc4".into(), Some(0.92279965))]).await;

				let i = fti.search(&mut tx, "dummy").await.unwrap();
				check_hits(i, &mut tx, vec![]).await;
			}
		}
	}
}
