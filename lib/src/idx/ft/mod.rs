pub(crate) mod analyzer;
pub(crate) mod docids;
mod doclength;
mod postings;
mod scorer;
mod termdocs;
pub(crate) mod terms;

use crate::err::Error;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::docids::DocIds;
use crate::idx::ft::doclength::DocLengths;
use crate::idx::ft::postings::Postings;
use crate::idx::ft::scorer::{BM25Scorer, Score};
use crate::idx::ft::termdocs::TermDocs;
use crate::idx::ft::terms::{TermId, Terms};
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::{Array, Object, Thing, Value};
use roaring::treemap::IntoIter;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::ops::BitAnd;

pub(crate) struct FtIndex {
	analyzer: Analyzer,
	state_key: Key,
	index_key_base: IndexKeyBase,
	state: State,
	bm25: Bm25Params,
	btree_default_order: u32,
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

pub(crate) struct Statistics {
	doc_ids: btree::Statistics,
	terms: btree::Statistics,
	doc_lengths: btree::Statistics,
	postings: btree::Statistics,
}

impl From<Statistics> for Value {
	fn from(stats: Statistics) -> Self {
		let mut res = Object::default();
		res.insert("doc_ids".to_owned(), Value::from(stats.doc_ids));
		res.insert("terms".to_owned(), Value::from(stats.terms));
		res.insert("doc_lengths".to_owned(), Value::from(stats.doc_lengths));
		res.insert("postings".to_owned(), Value::from(stats.postings));
		Value::from(res)
	}
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
		az: DefineAnalyzerStatement,
		index_key_base: IndexKeyBase,
		btree_default_order: u32,
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
			analyzer: az.into(),
		})
	}

	async fn doc_ids(&self, tx: &mut Transaction) -> Result<DocIds, Error> {
		DocIds::new(tx, self.index_key_base.clone(), self.btree_default_order).await
	}

	async fn terms(&self, tx: &mut Transaction) -> Result<Terms, Error> {
		Terms::new(tx, self.index_key_base.clone(), self.btree_default_order).await
	}

	fn term_docs(&self) -> TermDocs {
		TermDocs::new(self.index_key_base.clone())
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
				let td = self.term_docs();
				for term_id in term_list {
					p.remove_posting(tx, term_id, doc_id).await?;
					// if the term is not present in any document in the index, we can remove it
					let doc_count = td.remove_doc(tx, term_id, doc_id).await?;
					if doc_count == 0 {
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
			self.analyzer.extract_terms_with_frequencies(&mut t, tx, field_content).await?;

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

		// Set the terms postings and term docs
		let term_docs = self.term_docs();
		let mut terms_ids = RoaringTreemap::default();
		let mut p = self.postings(tx).await?;
		for (term_id, term_freq) in terms_and_frequencies {
			p.update_posting(tx, term_id, doc_id, term_freq).await?;
			if let Some(old_term_ids) = &mut old_term_ids {
				old_term_ids.remove(term_id);
			}
			term_docs.set_doc(tx, term_id, doc_id).await?;
			terms_ids.insert(term_id);
		}

		// Remove any remaining postings
		if let Some(old_term_ids) = old_term_ids {
			for old_term_id in old_term_ids {
				p.remove_posting(tx, old_term_id, doc_id).await?;
				let doc_count = term_docs.remove_doc(tx, old_term_id, doc_id).await?;
				// if the term does not have anymore postings, we can remove the term
				if doc_count == 0 {
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
		query_string: String,
	) -> Result<Option<HitsIterator>, Error> {
		let t = self.terms(tx).await?;
		let td = self.term_docs();
		let terms = self.analyzer.extract_terms(&t, tx, query_string).await?;
		let mut hits: Option<RoaringTreemap> = None;
		let mut terms_docs = Vec::with_capacity(terms.len());
		for term_id in terms {
			if let Some(term_id) = term_id {
				if let Some(term_docs) = td.get_docs(tx, term_id).await? {
					if let Some(h) = hits {
						hits = Some(h.bitand(&term_docs));
					} else {
						hits = Some(term_docs.clone());
					}
					terms_docs.push((term_id, term_docs));
					continue;
				}
			}
			return Ok(None);
		}
		if let Some(hits) = hits {
			if !hits.is_empty() {
				let postings = self.postings(tx).await?;
				let doc_lengths = self.doc_lengths(tx).await?;
				// TODO: Scoring should be optional
				let scorer = BM25Scorer::new(
					doc_lengths,
					self.state.total_docs_lengths,
					self.state.doc_count,
					self.bm25.clone(),
				);
				let doc_ids = self.doc_ids(tx).await?;
				return Ok(Some(HitsIterator::new(
					doc_ids,
					postings,
					hits,
					terms_docs,
					Some(scorer),
				)));
			}
		}
		Ok(None)
	}

	pub(super) async fn match_id_value(
		&self,
		tx: &mut Transaction,
		thg: &Thing,
		term: &str,
	) -> Result<bool, Error> {
		let doc_key: Key = thg.into();
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

	pub(crate) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
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
	doc_ids: DocIds,
	postings: Postings,
	iter: IntoIter,
	terms_docs: Vec<(TermId, RoaringTreemap)>,
	scorer: Option<BM25Scorer>,
}

impl HitsIterator {
	fn new(
		doc_ids: DocIds,
		postings: Postings,
		hits: RoaringTreemap,
		terms_docs: Vec<(TermId, RoaringTreemap)>,
		scorer: Option<BM25Scorer>,
	) -> Self {
		Self {
			doc_ids,
			postings,
			iter: hits.into_iter(),
			terms_docs,
			scorer,
		}
	}

	pub(crate) async fn next(
		&mut self,
		tx: &mut Transaction,
	) -> Result<Option<(Thing, Option<Score>)>, Error> {
		loop {
			if let Some(doc_id) = self.iter.next() {
				if let Some(doc_key) = self.doc_ids.get_doc_key(tx, doc_id).await? {
					let score = if let Some(scorer) = &self.scorer {
						let mut sc = 0.0;
						for (term_id, docs) in &self.terms_docs {
							if docs.contains(doc_id) {
								if let Some(term_freq) =
									self.postings.get_term_frequency(tx, *term_id, doc_id).await?
								{
									sc += scorer.score(tx, doc_id, docs.len(), term_freq).await?;
								}
							}
						}
						Some(sc)
					} else {
						None
					};
					return Ok(Some((doc_key.into(), score)));
				}
			} else {
				break;
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
	use crate::sql::statements::define::analyzer;
	use crate::sql::{Array, Thing};
	use std::collections::HashMap;
	use test_log::test;

	async fn check_hits(
		i: Option<HitsIterator>,
		tx: &mut Transaction,
		e: Vec<(&Thing, Option<Score>)>,
	) {
		if let Some(mut i) = i {
			let mut map = HashMap::new();
			while let Some((k, s)) = i.next(tx).await.unwrap() {
				map.insert(k, s);
			}
			assert_eq!(map.len(), e.len());
			for (k, p) in e {
				assert_eq!(map.get(k), Some(&p));
			}
		} else {
			panic!("hits is none");
		}
	}

	#[test(tokio::test)]
	async fn test_ft_index() {
		let ds = Datastore::new("memory").await.unwrap();
		let (_, az) = analyzer("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();

		let default_btree_order = 5;

		let doc1: Thing = ("t", "doc1").into();
		let doc2: Thing = ("t", "doc2").into();
		let doc3: Thing = ("t", "doc3").into();

		{
			// Add one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, az.clone(), IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();
			fti.index_document(&mut tx, &doc1, &Array::from(vec!["hello the world"]))
				.await
				.unwrap();
			tx.commit().await.unwrap();
		}

		{
			// Add two documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, az.clone(), IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();
			fti.index_document(&mut tx, &doc2, &Array::from(vec!["a yellow hello"])).await.unwrap();
			fti.index_document(&mut tx, &doc3, &Array::from(vec!["foo bar"])).await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = ds.transaction(true, false).await.unwrap();
			let fti =
				FtIndex::new(&mut tx, az.clone(), IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();

			// Check the statistics
			let statistics = fti.statistics(&mut tx).await.unwrap();
			assert_eq!(statistics.terms.keys_count, 7);
			assert_eq!(statistics.postings.keys_count, 8);
			assert_eq!(statistics.doc_ids.keys_count, 3);
			assert_eq!(statistics.doc_lengths.keys_count, 3);

			// Search & score
			let i = fti.search(&mut tx, "hello".to_string()).await.unwrap();
			check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

			let i = fti.search(&mut tx, "world".to_string()).await.unwrap();
			check_hits(i, &mut tx, vec![(&doc1, Some(0.4859746))]).await;

			let i = fti.search(&mut tx, "yellow".to_string()).await.unwrap();
			check_hits(i, &mut tx, vec![(&doc2, Some(0.4859746))]).await;

			let i = fti.search(&mut tx, "foo".to_string()).await.unwrap();
			check_hits(i, &mut tx, vec![(&doc3, Some(0.56902087))]).await;

			let i = fti.search(&mut tx, "bar".to_string()).await.unwrap();
			check_hits(i, &mut tx, vec![(&doc3, Some(0.56902087))]).await;

			let i = fti.search(&mut tx, "dummy".to_string()).await.unwrap();
			assert!(i.is_none());
		}

		{
			// Reindex one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, az.clone(), IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();
			fti.index_document(&mut tx, &doc3, &Array::from(vec!["nobar foo"])).await.unwrap();
			tx.commit().await.unwrap();

			// We can still find 'foo'
			let mut tx = ds.transaction(false, false).await.unwrap();
			let i = fti.search(&mut tx, "foo".to_string()).await.unwrap();
			check_hits(i, &mut tx, vec![(&doc3, Some(0.56902087))]).await;

			// We can't anymore find 'bar'
			let i = fti.search(&mut tx, "bar".to_string()).await.unwrap();
			assert!(i.is_none());

			// We can now find 'nobar'
			let i = fti.search(&mut tx, "nobar".to_string()).await.unwrap();
			check_hits(i, &mut tx, vec![(&doc3, Some(0.56902087))]).await;
		}

		{
			// Remove documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti =
				FtIndex::new(&mut tx, az.clone(), IndexKeyBase::default(), default_btree_order)
					.await
					.unwrap();
			fti.remove_document(&mut tx, &doc1).await.unwrap();
			fti.remove_document(&mut tx, &doc2).await.unwrap();
			fti.remove_document(&mut tx, &doc3).await.unwrap();
			tx.commit().await.unwrap();

			let mut tx = ds.transaction(false, false).await.unwrap();
			let i = fti.search(&mut tx, "hello".to_string()).await.unwrap();
			assert!(i.is_none());

			let i = fti.search(&mut tx, "foo".to_string()).await.unwrap();
			assert!(i.is_none());
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
			let (_, az) = analyzer("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();

			let doc1: Thing = ("t", "doc1").into();
			let doc2: Thing = ("t", "doc2").into();
			let doc3: Thing = ("t", "doc3").into();
			let doc4: Thing = ("t", "doc4").into();

			let default_btree_order = 5;
			{
				let mut tx = ds.transaction(true, false).await.unwrap();
				let mut fti =
					FtIndex::new(&mut tx, az.clone(), IndexKeyBase::default(), default_btree_order)
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
				let fti =
					FtIndex::new(&mut tx, az.clone(), IndexKeyBase::default(), default_btree_order)
						.await
						.unwrap();

				let statistics = fti.statistics(&mut tx).await.unwrap();
				assert_eq!(statistics.terms.keys_count, 17);
				assert_eq!(statistics.postings.keys_count, 28);
				assert_eq!(statistics.doc_ids.keys_count, 4);
				assert_eq!(statistics.doc_lengths.keys_count, 4);

				let i = fti.search(&mut tx, "the".to_string()).await.unwrap();
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

				let i = fti.search(&mut tx, "dog".to_string()).await.unwrap();
				check_hits(
					i,
					&mut tx,
					vec![(&doc1, Some(0.0)), (&doc2, Some(0.0)), (&doc3, Some(0.0))],
				)
				.await;

				let i = fti.search(&mut tx, "fox".to_string()).await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "over".to_string()).await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "lazy".to_string()).await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "jumped".to_string()).await.unwrap();
				check_hits(i, &mut tx, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let i = fti.search(&mut tx, "nothing".to_string()).await.unwrap();
				check_hits(i, &mut tx, vec![(&doc3, Some(0.87105393))]).await;

				let i = fti.search(&mut tx, "animals".to_string()).await.unwrap();
				check_hits(i, &mut tx, vec![(&doc4, Some(0.92279965))]).await;

				let i = fti.search(&mut tx, "dummy".to_string()).await.unwrap();
				assert!(i.is_none());
			}
		}
	}
}
