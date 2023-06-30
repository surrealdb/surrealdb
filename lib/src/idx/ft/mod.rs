pub(crate) mod analyzer;
pub(crate) mod docids;
mod doclength;
mod highlighter;
mod offsets;
mod postings;
pub(super) mod scorer;
pub(super) mod termdocs;
pub(crate) mod terms;

use crate::err::Error;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::doclength::DocLengths;
use crate::idx::ft::highlighter::{Highlighter, Offseter};
use crate::idx::ft::offsets::Offsets;
use crate::idx::ft::postings::Postings;
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::TermDocs;
use crate::idx::ft::terms::{TermId, Terms};
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::{Array, Idiom, Object, Thing, Value};
use roaring::treemap::IntoIter;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::ops::BitAnd;
use std::sync::Arc;

pub(crate) type MatchRef = u8;

pub(crate) struct FtIndex {
	analyzer: Analyzer,
	state_key: Key,
	index_key_base: IndexKeyBase,
	state: State,
	bm25: Option<Bm25Params>,
	order: u32,
	highlighting: bool,
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
		order: u32,
		scoring: &Scoring,
		hl: bool,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bs_key();
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::default()
		};
		let mut bm25 = None;
		if let Scoring::Bm {
			k1,
			b,
		} = scoring
		{
			bm25 = Some(Bm25Params {
				k1: *k1,
				b: *b,
			});
		}
		Ok(Self {
			state,
			state_key,
			index_key_base,
			bm25,
			order,
			highlighting: hl,
			analyzer: az.into(),
		})
	}

	pub(crate) async fn doc_ids(&self, tx: &mut Transaction) -> Result<DocIds, Error> {
		DocIds::new(tx, self.index_key_base.clone(), self.order).await
	}

	async fn terms(&self, tx: &mut Transaction) -> Result<Terms, Error> {
		Terms::new(tx, self.index_key_base.clone(), self.order).await
	}

	fn term_docs(&self) -> TermDocs {
		TermDocs::new(self.index_key_base.clone())
	}

	async fn doc_lengths(&self, tx: &mut Transaction) -> Result<DocLengths, Error> {
		DocLengths::new(tx, self.index_key_base.clone(), self.order).await
	}

	async fn postings(&self, tx: &mut Transaction) -> Result<Postings, Error> {
		Postings::new(tx, self.index_key_base.clone(), self.order).await
	}

	fn offsets(&self) -> Offsets {
		Offsets::new(self.index_key_base.clone())
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
				for term_id in &term_list {
					p.remove_posting(tx, term_id, doc_id).await?;
					// if the term is not present in any document in the index, we can remove it
					let doc_count = td.remove_doc(tx, term_id, doc_id).await?;
					if doc_count == 0 {
						t.remove_term_id(tx, term_id).await?;
					}
				}
				// Remove the offsets if any
				if self.highlighting {
					let o = self.offsets();
					for term_id in term_list {
						// TODO?: Removal can be done with a prefix on doc_id
						o.remove_offsets(tx, doc_id, term_id).await?;
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

		// Extract the doc_lengths, terms en frequencies (and offset)
		let mut t = self.terms(tx).await?;
		let (doc_length, terms_and_frequencies, offsets) = if self.highlighting {
			let (dl, tf, ofs) = self
				.analyzer
				.extract_terms_with_frequencies_with_offsets(&mut t, tx, field_content)
				.await?;
			(dl, tf, Some(ofs))
		} else {
			let (dl, tf) =
				self.analyzer.extract_terms_with_frequencies(&mut t, tx, field_content).await?;
			(dl, tf, None)
		};

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
		if let Some(old_term_ids) = &old_term_ids {
			for old_term_id in old_term_ids {
				p.remove_posting(tx, old_term_id, doc_id).await?;
				let doc_count = term_docs.remove_doc(tx, old_term_id, doc_id).await?;
				// if the term does not have anymore postings, we can remove the term
				if doc_count == 0 {
					t.remove_term_id(tx, old_term_id).await?;
				}
			}
		}

		if self.highlighting {
			let o = self.offsets();
			// Set the offset if any
			if let Some(ofs) = offsets {
				if !ofs.is_empty() {
					for (tid, or) in ofs {
						o.set_offsets(tx, doc_id, tid, or).await?;
					}
				}
			}
			// In case of an update, w remove the offset for the terms that does not exist anymore
			if let Some(old_term_ids) = old_term_ids {
				for old_term_id in old_term_ids {
					o.remove_offsets(tx, doc_id, old_term_id).await?;
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

	pub(super) async fn extract_terms(
		&self,
		tx: &mut Transaction,
		query_string: String,
	) -> Result<Vec<Option<TermId>>, Error> {
		let t = self.terms(tx).await?;
		let terms = self.analyzer.extract_terms(&t, tx, query_string).await?;
		Ok(terms)
	}

	pub(super) async fn get_terms_docs(
		&self,
		tx: &mut Transaction,
		terms: &Vec<Option<TermId>>,
	) -> Result<Vec<Option<(TermId, RoaringTreemap)>>, Error> {
		let mut terms_docs = Vec::with_capacity(terms.len());
		let td = self.term_docs();
		for opt_term_id in terms {
			if let Some(term_id) = opt_term_id {
				let docs = td.get_docs(tx, *term_id).await?;
				if let Some(docs) = docs {
					terms_docs.push(Some((*term_id, docs)));
				} else {
					terms_docs.push(Some((*term_id, RoaringTreemap::new())));
				}
			} else {
				terms_docs.push(None);
			}
		}
		Ok(terms_docs)
	}

	pub(super) async fn new_hits_iterator(
		&self,
		tx: &mut Transaction,
		terms_docs: Arc<Vec<Option<(TermId, RoaringTreemap)>>>,
	) -> Result<Option<HitsIterator>, Error> {
		let mut hits: Option<RoaringTreemap> = None;
		for opt_term_docs in terms_docs.iter() {
			if let Some((_, term_docs)) = opt_term_docs {
				if let Some(h) = hits {
					hits = Some(h.bitand(term_docs));
				} else {
					hits = Some(term_docs.clone());
				}
			} else {
				return Ok(None);
			}
		}
		if let Some(hits) = hits {
			if !hits.is_empty() {
				let doc_ids = self.doc_ids(tx).await?;
				return Ok(Some(HitsIterator::new(doc_ids, hits)));
			}
		}
		Ok(None)
	}

	pub(super) async fn new_scorer(
		&self,
		tx: &mut Transaction,
		terms_docs: Arc<Vec<Option<(TermId, RoaringTreemap)>>>,
	) -> Result<Option<BM25Scorer>, Error> {
		if let Some(bm25) = &self.bm25 {
			return Ok(Some(BM25Scorer::new(
				self.postings(tx).await?,
				terms_docs,
				self.doc_lengths(tx).await?,
				self.state.total_docs_lengths,
				self.state.doc_count,
				bm25.clone(),
			)));
		}
		Ok(None)
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn highlight(
		&self,
		tx: &mut Transaction,
		thg: &Thing,
		terms: &[Option<TermId>],
		prefix: Value,
		suffix: Value,
		idiom: &Idiom,
		doc: &Value,
	) -> Result<Value, Error> {
		let doc_key: Key = thg.into();
		let doc_ids = self.doc_ids(tx).await?;
		if let Some(doc_id) = doc_ids.get_doc_id(tx, doc_key).await? {
			let o = self.offsets();
			let mut hl = Highlighter::new(prefix, suffix, idiom, doc);
			for term_id in terms.iter().flatten() {
				let o = o.get_offsets(tx, doc_id, *term_id).await?;
				if let Some(o) = o {
					hl.highlight(o.0);
				}
			}
			return hl.try_into();
		}
		Ok(Value::None)
	}

	pub(super) async fn extract_offsets(
		&self,
		tx: &mut Transaction,
		thg: &Thing,
		terms: &[Option<TermId>],
	) -> Result<Value, Error> {
		let doc_key: Key = thg.into();
		let doc_ids = self.doc_ids(tx).await?;
		if let Some(doc_id) = doc_ids.get_doc_id(tx, doc_key).await? {
			let o = self.offsets();
			let mut or = Offseter::default();
			for term_id in terms.iter().flatten() {
				let o = o.get_offsets(tx, doc_id, *term_id).await?;
				if let Some(o) = o {
					or.highlight(o.0);
				}
			}
			return or.try_into();
		}
		Ok(Value::None)
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
	iter: IntoIter,
}

impl HitsIterator {
	fn new(doc_ids: DocIds, hits: RoaringTreemap) -> Self {
		Self {
			doc_ids,
			iter: hits.into_iter(),
		}
	}

	pub(crate) async fn next(
		&mut self,
		tx: &mut Transaction,
	) -> Result<Option<(Thing, DocId)>, Error> {
		for doc_id in self.iter.by_ref() {
			if let Some(doc_key) = self.doc_ids.get_doc_key(tx, doc_id).await? {
				return Ok(Some((doc_key.into(), doc_id)));
			}
		}
		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::scorer::{BM25Scorer, Score};
	use crate::idx::ft::{FtIndex, HitsIterator};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};
	use crate::sql::scoring::Scoring;
	use crate::sql::statements::define::analyzer;
	use crate::sql::{Array, Thing};
	use std::collections::HashMap;
	use std::sync::Arc;
	use test_log::test;

	async fn check_hits(
		tx: &mut Transaction,
		hits: Option<HitsIterator>,
		scr: BM25Scorer,
		e: Vec<(&Thing, Option<Score>)>,
	) {
		if let Some(mut hits) = hits {
			let mut map = HashMap::new();
			while let Some((k, d)) = hits.next(tx).await.unwrap() {
				let s = scr.score(tx, d).await.unwrap();
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

	async fn search(
		tx: &mut Transaction,
		fti: &FtIndex,
		qs: &str,
	) -> (Option<HitsIterator>, BM25Scorer) {
		let t = fti.extract_terms(tx, qs.to_string()).await.unwrap();
		let td = Arc::new(fti.get_terms_docs(tx, &t).await.unwrap());
		let scr = fti.new_scorer(tx, td.clone()).await.unwrap().unwrap();
		let hits = fti.new_hits_iterator(tx, td).await.unwrap();
		(hits, scr)
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
			let mut fti = FtIndex::new(
				&mut tx,
				az.clone(),
				IndexKeyBase::default(),
				default_btree_order,
				&Scoring::bm25(),
				false,
			)
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
			let mut fti = FtIndex::new(
				&mut tx,
				az.clone(),
				IndexKeyBase::default(),
				default_btree_order,
				&Scoring::bm25(),
				false,
			)
			.await
			.unwrap();
			fti.index_document(&mut tx, &doc2, &Array::from(vec!["a yellow hello"])).await.unwrap();
			fti.index_document(&mut tx, &doc3, &Array::from(vec!["foo bar"])).await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = ds.transaction(true, false).await.unwrap();
			let fti = FtIndex::new(
				&mut tx,
				az.clone(),
				IndexKeyBase::default(),
				default_btree_order,
				&Scoring::bm25(),
				false,
			)
			.await
			.unwrap();

			// Check the statistics
			let statistics = fti.statistics(&mut tx).await.unwrap();
			assert_eq!(statistics.terms.keys_count, 7);
			assert_eq!(statistics.postings.keys_count, 8);
			assert_eq!(statistics.doc_ids.keys_count, 3);
			assert_eq!(statistics.doc_lengths.keys_count, 3);

			// Search & score
			let (hits, scr) = search(&mut tx, &fti, "hello").await;
			check_hits(&mut tx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

			let (hits, scr) = search(&mut tx, &fti, "world").await;
			check_hits(&mut tx, hits, scr, vec![(&doc1, Some(0.4859746))]).await;

			let (hits, scr) = search(&mut tx, &fti, "yellow").await;
			check_hits(&mut tx, hits, scr, vec![(&doc2, Some(0.4859746))]).await;

			let (hits, scr) = search(&mut tx, &fti, "foo").await;
			check_hits(&mut tx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

			let (hits, scr) = search(&mut tx, &fti, "bar").await;
			check_hits(&mut tx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

			let (hits, _) = search(&mut tx, &fti, "dummy").await;
			assert!(hits.is_none());
		}

		{
			// Reindex one document
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti = FtIndex::new(
				&mut tx,
				az.clone(),
				IndexKeyBase::default(),
				default_btree_order,
				&Scoring::bm25(),
				false,
			)
			.await
			.unwrap();
			fti.index_document(&mut tx, &doc3, &Array::from(vec!["nobar foo"])).await.unwrap();
			tx.commit().await.unwrap();

			// We can still find 'foo'
			let mut tx = ds.transaction(false, false).await.unwrap();
			let (hits, scr) = search(&mut tx, &fti, "foo").await;
			check_hits(&mut tx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

			// We can't anymore find 'bar'
			let (hits, _) = search(&mut tx, &fti, "bar").await;
			assert!(hits.is_none());

			// We can now find 'nobar'
			let (hits, scr) = search(&mut tx, &fti, "nobar").await;
			check_hits(&mut tx, hits, scr, vec![(&doc3, Some(0.56902087))]).await;
		}

		{
			// Remove documents
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut fti = FtIndex::new(
				&mut tx,
				az.clone(),
				IndexKeyBase::default(),
				default_btree_order,
				&Scoring::bm25(),
				false,
			)
			.await
			.unwrap();
			fti.remove_document(&mut tx, &doc1).await.unwrap();
			fti.remove_document(&mut tx, &doc2).await.unwrap();
			fti.remove_document(&mut tx, &doc3).await.unwrap();
			tx.commit().await.unwrap();

			let mut tx = ds.transaction(false, false).await.unwrap();
			let (hits, _) = search(&mut tx, &fti, "hello").await;
			assert!(hits.is_none());

			let (hits, _) = search(&mut tx, &fti, "foo").await;
			assert!(hits.is_none());
		}
	}

	async fn test_ft_index_bm_25(hl: bool) {
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
				let mut fti = FtIndex::new(
					&mut tx,
					az.clone(),
					IndexKeyBase::default(),
					default_btree_order,
					&Scoring::bm25(),
					hl,
				)
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
				let fti = FtIndex::new(
					&mut tx,
					az.clone(),
					IndexKeyBase::default(),
					default_btree_order,
					&Scoring::bm25(),
					hl,
				)
				.await
				.unwrap();

				let statistics = fti.statistics(&mut tx).await.unwrap();
				assert_eq!(statistics.terms.keys_count, 17);
				assert_eq!(statistics.postings.keys_count, 28);
				assert_eq!(statistics.doc_ids.keys_count, 4);
				assert_eq!(statistics.doc_lengths.keys_count, 4);

				let (hits, scr) = search(&mut tx, &fti, "the").await;
				check_hits(
					&mut tx,
					hits,
					scr,
					vec![
						(&doc1, Some(0.0)),
						(&doc2, Some(0.0)),
						(&doc3, Some(0.0)),
						(&doc4, Some(0.0)),
					],
				)
				.await;

				let (hits, scr) = search(&mut tx, &fti, "dog").await;
				check_hits(
					&mut tx,
					hits,
					scr,
					vec![(&doc1, Some(0.0)), (&doc2, Some(0.0)), (&doc3, Some(0.0))],
				)
				.await;

				let (hits, scr) = search(&mut tx, &fti, "fox").await;
				check_hits(&mut tx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let (hits, scr) = search(&mut tx, &fti, "over").await;
				check_hits(&mut tx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let (hits, scr) = search(&mut tx, &fti, "lazy").await;
				check_hits(&mut tx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let (hits, scr) = search(&mut tx, &fti, "jumped").await;
				check_hits(&mut tx, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

				let (hits, scr) = search(&mut tx, &fti, "nothing").await;
				check_hits(&mut tx, hits, scr, vec![(&doc3, Some(0.87105393))]).await;

				let (hits, scr) = search(&mut tx, &fti, "animals").await;
				check_hits(&mut tx, hits, scr, vec![(&doc4, Some(0.92279965))]).await;

				let (hits, _) = search(&mut tx, &fti, "dummy").await;
				assert!(hits.is_none());
			}
		}
	}

	#[test(tokio::test)]
	async fn test_ft_index_bm_25_without_highlighting() {
		test_ft_index_bm_25(false).await;
	}

	#[test(tokio::test)]
	async fn test_ft_index_bm_25_with_highlighting() {
		test_ft_index_bm_25(true).await;
	}
}
