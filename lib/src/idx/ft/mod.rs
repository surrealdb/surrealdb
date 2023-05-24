mod analyzer;
pub(crate) mod docids;
mod doclength;
mod postings;
mod scorer;
pub(crate) mod terms;

use crate::err::Error;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::docids::DocIds;
use crate::idx::ft::doclength::DocLengths;
use crate::idx::ft::postings::{Postings, PostingsIterator};
use crate::idx::ft::scorer::{BM25Scorer, Score};
use crate::idx::ft::terms::{TermId, Terms};
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use crate::sql::{Array, Thing};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

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
		rid: &Thing,
	) -> Result<(), Error> {
		// Extract and remove the doc_id (if any)
		let mut d = self.doc_ids(tx).await?;
		if let Some(doc_id) = d.remove_doc(tx, rid.into()).await? {
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
		rid: &Thing,
		field_content: &Array,
	) -> Result<(), Error> {
		// Resolve the doc_id
		let mut d = self.doc_ids(tx).await?;
		let resolved = d.resolve_doc_id(tx, rid.into()).await?;
		let doc_id = *resolved.doc_id();

		// Extract the doc_lengths, terms en frequencies
		let mut t = self.terms(tx).await?;
		let (doc_length, terms_and_frequencies) =
			Analyzer::extract_terms_with_frequencies(&mut t, tx, field_content).await?;

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
		let mut terms_ids = RoaringTreemap::default();
		let mut p = self.postings(tx).await?;
		for (term_id, term_freq) in terms_and_frequencies {
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
				// TODO: Scoring should be optional
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

	pub(super) async fn match_id_value(
		&self,
		tx: &mut Transaction,
		rid: &Thing,
		term: &str,
	) -> Result<bool, Error> {
		let doc_key: Key = rid.into();
		let doc_ids = self.doc_ids(tx).await?;
		if let Some(doc_id) = doc_ids.get_doc_id(tx, doc_key).await? {
			let terms = self.terms(tx).await?;
			if let Some(term_id) = terms.get_term_id(tx, term).await? {
				let postings = self.postings(tx).await?;
				if let Some(term_freq) = postings.get_term_frequency(tx, term_id, doc_id).await? {
					if term_freq > 0 {
						return Ok(true);
					}
				}
			}
		}
		Ok(false)
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
			let postings = postings.new_postings_iterator(term_id);
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
	) -> Result<Option<(Thing, Option<Score>)>, Error> {
		if let Some((doc_ids, postings)) = &mut self.docs {
			if let Some((doc_id, term_freq)) = postings.next(tx).await? {
				if let Some(doc_key) = doc_ids.get_doc_key(tx, doc_id).await? {
					let score = if let Some(scorer) = &self.scorer {
						Some(scorer.score(tx, doc_id, term_freq).await?)
					} else {
						None
					};
					return Ok(Some((doc_key.into(), score)));
				}
			}
		}
		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::{FtIndex, HitsIterator, Score};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};
	use crate::sql::{Array, Thing};
	use std::collections::HashMap;
	use test_log::test;

	async fn check_hits(
		mut i: HitsIterator,
		tx: &mut Transaction,
		e: Vec<(&Thing, Option<Score>)>,
	) {
		let mut map = HashMap::new();
		while let Some((k, s)) = i.next(tx).await.unwrap() {
			map.insert(k, s);
		}
		assert_eq!(map.len(), e.len());
		for (k, p) in e {
			assert_eq!(map.get(k), Some(&p));
		}
	}

	#[test(tokio::test)]
	async fn test_ft_index() {
		let ds = Datastore::new("memory").await.unwrap();

		let default_btree_order = 5;

		let doc1: Thing = ("t", "doc1").into();
		let doc2: Thing = ("t", "doc2").into();
		let doc3: Thing = ("t", "doc3").into();

		{
			// Add one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, &doc1, &Array::from(vec!["hello the world"]))
				.await
				.unwrap();
			tx.commit().await.unwrap();
		}

		{
			// Add two documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, &doc2, &Array::from(vec!["a yellow hello"])).await.unwrap();
			fti.index_document(&mut tx, &doc3, &Array::from(vec!["foo bar"])).await.unwrap();
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
			check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

			let i = fti.search(&mut tx, "world").await.unwrap();
			check_hits(i, &mut tx, vec![(&doc1, Some(0.4859746))]).await;

			let i = fti.search(&mut tx, "yellow").await.unwrap();
			check_hits(i, &mut tx, vec![(&doc2, Some(0.4859746))]).await;

			let i = fti.search(&mut tx, "foo").await.unwrap();
			check_hits(i, &mut tx, vec![(&doc3, Some(0.56902087))]).await;

			let i = fti.search(&mut tx, "bar").await.unwrap();
			check_hits(i, &mut tx, vec![(&doc3, Some(0.56902087))]).await;

			let i = fti.search(&mut tx, "dummy").await.unwrap();
			check_hits(i, &mut tx, vec![]).await;
		}

		{
			// Reindex one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.index_document(&mut tx, &doc3, &Array::from(vec!["nobar foo"])).await.unwrap();
			tx.commit().await.unwrap();

			// We can still find 'foo'
			let mut tx = ds.transaction(false, false).await.unwrap();
			let i = fti.search(&mut tx, "foo").await.unwrap();
			check_hits(i, &mut tx, vec![((&doc3).into(), Some(0.56902087))]).await;

			// We can't anymore find 'bar'
			let i = fti.search(&mut tx, "bar").await.unwrap();
			check_hits(i, &mut tx, vec![]).await;

			// We can now find 'nobar'
			let i = fti.search(&mut tx, "nobar").await.unwrap();
			check_hits(i, &mut tx, vec![((&doc3).into(), Some(0.56902087))]).await;
		}

		{
			// Remove documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order).await.unwrap();
			fti.remove_document(&mut tx, &doc1).await.unwrap();
			fti.remove_document(&mut tx, &doc2).await.unwrap();
			fti.remove_document(&mut tx, &doc3).await.unwrap();
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

			let doc1: Thing = ("t", "doc1").into();
			let doc2: Thing = ("t", "doc2").into();
			let doc3: Thing = ("t", "doc3").into();
			let doc4: Thing = ("t", "doc4").into();

			let default_btree_order = 5;
			{
				let mut tx = ds.transaction(true, false).await.unwrap();
				let mut fti = FtIndex::new(&mut tx, IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();
				fti.index_document(
					&mut tx,
					&doc1,
					&Array::from(vec!["the quick brown fox jumped over the lazy dog"]),
				)
				.await
				.unwrap();
				fti.index_document(
					&mut tx,
					&doc2,
					&Array::from(vec!["the fast fox jumped over the lazy dog"]),
				)
				.await
				.unwrap();
				fti.index_document(
					&mut tx,
					&doc3,
					&Array::from(vec!["the dog sat there and did nothing"]),
				)
				.await
				.unwrap();
				fti.index_document(
					&mut tx,
					&doc4,
					&Array::from(vec!["the other animals sat there watching"]),
				)
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
						(&doc1, Some(0.0)),
						(&doc2, Some(0.0)),
						(&doc3, Some(0.0)),
						(&doc4, Some(0.0)),
					],
				)
				.await;

				let i = fti.search(&mut tx, "dog").await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![(&doc1, Some(0.0)), (&doc2, Some(0.0)), (&doc3, Some(0.0))],
				)
				.await;

				let i = fti.search(&mut tx, "fox").await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "over").await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "lazy").await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "jumped").await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "nothing").await.unwrap();
				check_hits(i, &mut tx, vec![(&doc3, Some(0.87105393))]).await;

				let i = fti.search(&mut tx, "animals").await.unwrap();
				check_hits(i, &mut tx, vec![(&doc4, Some(0.92279965))]).await;

				let i = fti.search(&mut tx, "dummy").await.unwrap();
				check_hits(i, &mut tx, vec![]).await;
			}
		}
	}
}
