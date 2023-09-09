pub(crate) mod analyzer;
mod doclength;
mod highlighter;
mod offsets;
mod postings;
pub(super) mod scorer;
pub(super) mod termdocs;
pub(crate) mod terms;

use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::doclength::DocLengths;
use crate::idx::ft::highlighter::{Highlighter, Offseter};
use crate::idx::ft::offsets::Offsets;
use crate::idx::ft::postings::Postings;
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::{TermDocs, TermsDocs};
use crate::idx::ft::terms::{TermId, Terms};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::TreeStoreType;
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction};
use crate::sql::index::SearchParams;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::{Idiom, Object, Thing, Value};
use revision::revisioned;
use roaring::treemap::IntoIter;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::ops::BitAnd;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) type MatchRef = u8;

pub(crate) struct FtIndex {
	analyzer: Analyzer,
	state_key: Key,
	index_key_base: IndexKeyBase,
	state: State,
	bm25: Option<Bm25Params>,
	highlighting: bool,
	doc_ids: Arc<RwLock<DocIds>>,
	doc_lengths: Arc<RwLock<DocLengths>>,
	postings: Arc<RwLock<Postings>>,
	terms: Arc<RwLock<Terms>>,
	offsets: Offsets,
	term_docs: TermDocs,
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

pub(crate) struct FtStatistics {
	doc_ids: BStatistics,
	terms: BStatistics,
	doc_lengths: BStatistics,
	postings: BStatistics,
}

impl From<FtStatistics> for Value {
	fn from(stats: FtStatistics) -> Self {
		let mut res = Object::default();
		res.insert("doc_ids".to_owned(), Value::from(stats.doc_ids));
		res.insert("terms".to_owned(), Value::from(stats.terms));
		res.insert("doc_lengths".to_owned(), Value::from(stats.doc_lengths));
		res.insert("postings".to_owned(), Value::from(stats.postings));
		Value::from(res)
	}
}

#[derive(Default, Serialize, Deserialize)]
#[revisioned(revision = 1)]
struct State {
	total_docs_lengths: u128,
	doc_count: u64,
}

impl VersionedSerdeState for State {}

impl FtIndex {
	pub(crate) async fn new(
		tx: &mut Transaction,
		az: DefineAnalyzerStatement,
		index_key_base: IndexKeyBase,
		p: &SearchParams,
		store_type: TreeStoreType,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bs_key();
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::default()
		};
		let doc_ids = Arc::new(RwLock::new(
			DocIds::new(tx, index_key_base.clone(), p.doc_ids_order, store_type).await?,
		));
		let doc_lengths = Arc::new(RwLock::new(
			DocLengths::new(tx, index_key_base.clone(), p.doc_lengths_order, store_type).await?,
		));
		let postings = Arc::new(RwLock::new(
			Postings::new(tx, index_key_base.clone(), p.postings_order, store_type).await?,
		));
		let terms = Arc::new(RwLock::new(
			Terms::new(tx, index_key_base.clone(), p.terms_order, store_type).await?,
		));
		let termdocs = TermDocs::new(index_key_base.clone());
		let offsets = Offsets::new(index_key_base.clone());
		let mut bm25 = None;
		if let Scoring::Bm {
			k1,
			b,
		} = p.sc
		{
			bm25 = Some(Bm25Params {
				k1,
				b,
			});
		}
		Ok(Self {
			state,
			state_key,
			index_key_base,
			bm25,
			highlighting: p.hl,
			analyzer: az.into(),
			doc_ids,
			doc_lengths,
			postings,
			terms,
			term_docs: termdocs,
			offsets,
		})
	}

	pub(super) fn doc_ids(&self) -> Arc<RwLock<DocIds>> {
		self.doc_ids.clone()
	}

	pub(crate) async fn remove_document(
		&mut self,
		tx: &mut Transaction,
		rid: &Thing,
	) -> Result<(), Error> {
		// Extract and remove the doc_id (if any)
		if let Some(doc_id) = self.doc_ids.write().await.remove_doc(tx, rid.into()).await? {
			self.state.doc_count -= 1;

			// Remove the doc length
			if let Some(doc_lengths) =
				self.doc_lengths.write().await.remove_doc_length(tx, doc_id).await?
			{
				self.state.total_docs_lengths -= doc_lengths as u128;
			}

			// Get the term list
			if let Some(term_list_vec) = tx.get(self.index_key_base.new_bk_key(doc_id)).await? {
				let term_list = RoaringTreemap::deserialize_from(&mut term_list_vec.as_slice())?;
				// Remove the postings
				let mut p = self.postings.write().await;
				let mut t = self.terms.write().await;
				for term_id in &term_list {
					p.remove_posting(tx, term_id, doc_id).await?;
					// if the term is not present in any document in the index, we can remove it
					let doc_count = self.term_docs.remove_doc(tx, term_id, doc_id).await?;
					if doc_count == 0 {
						t.remove_term_id(tx, term_id).await?;
					}
				}
				// Remove the offsets if any
				if self.highlighting {
					for term_id in term_list {
						// TODO?: Removal can be done with a prefix on doc_id
						self.offsets.remove_offsets(tx, doc_id, term_id).await?;
					}
				}
			}
		}
		Ok(())
	}

	pub(crate) async fn index_document(
		&mut self,
		tx: &mut Transaction,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<(), Error> {
		// Resolve the doc_id
		let resolved = self.doc_ids.write().await.resolve_doc_id(tx, rid.into()).await?;
		let doc_id = *resolved.doc_id();

		// Extract the doc_lengths, terms en frequencies (and offset)
		let mut t = self.terms.write().await;
		let (doc_length, terms_and_frequencies, offsets) = if self.highlighting {
			let (dl, tf, ofs) = self
				.analyzer
				.extract_terms_with_frequencies_with_offsets(&mut t, tx, content)
				.await?;
			(dl, tf, Some(ofs))
		} else {
			let (dl, tf) =
				self.analyzer.extract_terms_with_frequencies(&mut t, tx, content).await?;
			(dl, tf, None)
		};

		// Set the doc length
		let mut dl = self.doc_lengths.write().await;
		if resolved.was_existing() {
			if let Some(old_doc_length) = dl.get_doc_length(tx, doc_id).await? {
				self.state.total_docs_lengths -= old_doc_length as u128;
			}
		}
		dl.set_doc_length(tx, doc_id, doc_length).await?;

		// Retrieve the existing terms for this document (if any)
		let term_ids_key = self.index_key_base.new_bk_key(doc_id);
		let mut old_term_ids = if let Some(val) = tx.get(term_ids_key.clone()).await? {
			Some(RoaringTreemap::deserialize_from(&mut val.as_slice())?)
		} else {
			None
		};

		// Set the terms postings and term docs
		let mut terms_ids = RoaringTreemap::default();
		let mut p = self.postings.write().await;
		for (term_id, term_freq) in terms_and_frequencies {
			p.update_posting(tx, term_id, doc_id, term_freq).await?;
			if let Some(old_term_ids) = &mut old_term_ids {
				old_term_ids.remove(term_id);
			}
			self.term_docs.set_doc(tx, term_id, doc_id).await?;
			terms_ids.insert(term_id);
		}

		// Remove any remaining postings
		if let Some(old_term_ids) = &old_term_ids {
			for old_term_id in old_term_ids {
				p.remove_posting(tx, old_term_id, doc_id).await?;
				let doc_count = self.term_docs.remove_doc(tx, old_term_id, doc_id).await?;
				// if the term does not have anymore postings, we can remove the term
				if doc_count == 0 {
					t.remove_term_id(tx, old_term_id).await?;
				}
			}
		}

		if self.highlighting {
			// Set the offset if any
			if let Some(ofs) = offsets {
				if !ofs.is_empty() {
					for (tid, or) in ofs {
						self.offsets.set_offsets(tx, doc_id, tid, or).await?;
					}
				}
			}
			// In case of an update, w remove the offset for the terms that does not exist anymore
			if let Some(old_term_ids) = old_term_ids {
				for old_term_id in old_term_ids {
					self.offsets.remove_offsets(tx, doc_id, old_term_id).await?;
				}
			}
		}

		// Stores the term list for this doc_id
		let mut val = Vec::new();
		terms_ids.serialize_into(&mut val)?;
		tx.set(term_ids_key, val).await?;

		// Update the index state
		self.state.total_docs_lengths += doc_length as u128;
		if !resolved.was_existing() {
			self.state.doc_count += 1;
		}

		// Update the states
		tx.set(self.state_key.clone(), self.state.try_to_val()?).await?;
		Ok(())
	}

	pub(super) async fn extract_terms(
		&self,
		tx: &mut Transaction,
		query_string: String,
	) -> Result<Vec<Option<TermId>>, Error> {
		let t = self.terms.read().await;
		let terms = self.analyzer.extract_terms(&t, tx, query_string).await?;
		Ok(terms)
	}

	pub(super) async fn get_terms_docs(
		&self,
		tx: &mut Transaction,
		terms: &Vec<Option<TermId>>,
	) -> Result<Vec<Option<(TermId, RoaringTreemap)>>, Error> {
		let mut terms_docs = Vec::with_capacity(terms.len());
		for opt_term_id in terms {
			if let Some(term_id) = opt_term_id {
				let docs = self.term_docs.get_docs(tx, *term_id).await?;
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

	pub(super) fn new_hits_iterator(
		&self,
		terms_docs: TermsDocs,
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
				return Ok(Some(HitsIterator::new(self.doc_ids.clone(), hits)));
			}
		}
		Ok(None)
	}

	pub(super) fn new_scorer(&self, terms_docs: TermsDocs) -> Result<Option<BM25Scorer>, Error> {
		if let Some(bm25) = &self.bm25 {
			return Ok(Some(BM25Scorer::new(
				self.postings.clone(),
				terms_docs,
				self.doc_lengths.clone(),
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
		if let Some(doc_id) = self.doc_ids.read().await.get_doc_id(tx, doc_key).await? {
			let mut hl = Highlighter::new(prefix, suffix, idiom, doc);
			for term_id in terms.iter().flatten() {
				let o = self.offsets.get_offsets(tx, doc_id, *term_id).await?;
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
		if let Some(doc_id) = self.doc_ids.read().await.get_doc_id(tx, doc_key).await? {
			let mut or = Offseter::default();
			for term_id in terms.iter().flatten() {
				let o = self.offsets.get_offsets(tx, doc_id, *term_id).await?;
				if let Some(o) = o {
					or.highlight(o.0);
				}
			}
			return or.try_into();
		}
		Ok(Value::None)
	}

	pub(crate) async fn statistics(&self, tx: &mut Transaction) -> Result<FtStatistics, Error> {
		// TODO do parallel execution
		Ok(FtStatistics {
			doc_ids: self.doc_ids.read().await.statistics(tx).await?,
			terms: self.terms.read().await.statistics(tx).await?,
			doc_lengths: self.doc_lengths.read().await.statistics(tx).await?,
			postings: self.postings.read().await.statistics(tx).await?,
		})
	}

	pub(crate) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		self.doc_ids.write().await.finish(tx).await?;
		self.doc_lengths.write().await.finish(tx).await?;
		self.postings.write().await.finish(tx).await?;
		self.terms.write().await.finish(tx).await?;
		Ok(())
	}
}

pub(crate) struct HitsIterator {
	doc_ids: Arc<RwLock<DocIds>>,
	iter: IntoIter,
}

impl HitsIterator {
	fn new(doc_ids: Arc<RwLock<DocIds>>, hits: RoaringTreemap) -> Self {
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
			if let Some(doc_key) = self.doc_ids.read().await.get_doc_key(tx, doc_id).await? {
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
	use crate::idx::trees::store::TreeStoreType;
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};
	use crate::sql::index::SearchParams;
	use crate::sql::scoring::Scoring;
	use crate::sql::statements::define::analyzer;
	use crate::sql::statements::DefineAnalyzerStatement;
	use crate::sql::{Thing, Value};
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
				assert_eq!(map.get(k), Some(&p), "{}", k);
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
		let scr = fti.new_scorer(td.clone()).unwrap().unwrap();
		let hits = fti.new_hits_iterator(td).unwrap();
		(hits, scr)
	}

	pub(super) async fn tx_fti(
		ds: &Datastore,
		store_type: TreeStoreType,
		az: &DefineAnalyzerStatement,
		order: u32,
		hl: bool,
	) -> (Transaction, FtIndex) {
		let write = matches!(store_type, TreeStoreType::Write);
		let mut tx = ds.transaction(write, false).await.unwrap();
		let fti = FtIndex::new(
			&mut tx,
			az.clone(),
			IndexKeyBase::default(),
			&SearchParams {
				az: az.name.clone(),
				doc_ids_order: order,
				doc_lengths_order: order,
				postings_order: order,
				terms_order: order,
				sc: Scoring::bm25(),
				hl,
			},
			TreeStoreType::Write,
		)
		.await
		.unwrap();
		(tx, fti)
	}

	pub(super) async fn finish(mut tx: Transaction, fti: FtIndex) {
		fti.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_ft_index() {
		let ds = Datastore::new("memory").await.unwrap();
		let (_, az) = analyzer("ANALYZER test TOKENIZERS blank;").unwrap();

		let btree_order = 5;

		let doc1: Thing = ("t", "doc1").into();
		let doc2: Thing = ("t", "doc2").into();
		let doc3: Thing = ("t", "doc3").into();

		{
			// Add one document
			let (mut tx, mut fti) =
				tx_fti(&ds, TreeStoreType::Write, &az, btree_order, false).await;
			fti.index_document(&mut tx, &doc1, vec![Value::from("hello the world")]).await.unwrap();
			finish(tx, fti).await;
		}

		{
			// Add two documents
			let (mut tx, mut fti) =
				tx_fti(&ds, TreeStoreType::Write, &az, btree_order, false).await;
			fti.index_document(&mut tx, &doc2, vec![Value::from("a yellow hello")]).await.unwrap();
			fti.index_document(&mut tx, &doc3, vec![Value::from("foo bar")]).await.unwrap();
			finish(tx, fti).await;
		}

		{
			let (mut tx, fti) = tx_fti(&ds, TreeStoreType::Read, &az, btree_order, false).await;
			// Check the statistics
			let statistics = fti.statistics(&mut tx).await.unwrap();
			assert_eq!(statistics.terms.keys_count, 7);
			assert_eq!(statistics.postings.keys_count, 8);
			assert_eq!(statistics.doc_ids.keys_count, 3);
			assert_eq!(statistics.doc_lengths.keys_count, 3);

			// Search & score
			let (hits, scr) = search(&mut tx, &fti, "hello").await;
			check_hits(
				&mut tx,
				hits,
				scr,
				vec![(&doc1, Some(-0.4859746)), (&doc2, Some(-0.4859746))],
			)
			.await;

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
			let (mut tx, mut fti) =
				tx_fti(&ds, TreeStoreType::Write, &az, btree_order, false).await;
			fti.index_document(&mut tx, &doc3, vec![Value::from("nobar foo")]).await.unwrap();
			finish(tx, fti).await;

			let (mut tx, fti) = tx_fti(&ds, TreeStoreType::Read, &az, btree_order, false).await;

			// We can still find 'foo'
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
			let (mut tx, mut fti) =
				tx_fti(&ds, TreeStoreType::Write, &az, btree_order, false).await;
			fti.remove_document(&mut tx, &doc1).await.unwrap();
			fti.remove_document(&mut tx, &doc2).await.unwrap();
			fti.remove_document(&mut tx, &doc3).await.unwrap();
			finish(tx, fti).await;
		}

		{
			let (mut tx, fti) = tx_fti(&ds, TreeStoreType::Read, &az, btree_order, false).await;
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
			let (_, az) = analyzer("ANALYZER test TOKENIZERS blank;").unwrap();

			let doc1: Thing = ("t", "doc1").into();
			let doc2: Thing = ("t", "doc2").into();
			let doc3: Thing = ("t", "doc3").into();
			let doc4: Thing = ("t", "doc4").into();

			let btree_order = 5;
			{
				let (mut tx, mut fti) =
					tx_fti(&ds, TreeStoreType::Write, &az, btree_order, hl).await;
				fti.index_document(
					&mut tx,
					&doc1,
					vec![Value::from("the quick brown fox jumped over the lazy dog")],
				)
				.await
				.unwrap();
				fti.index_document(
					&mut tx,
					&doc2,
					vec![Value::from("the fast fox jumped over the lazy dog")],
				)
				.await
				.unwrap();
				fti.index_document(
					&mut tx,
					&doc3,
					vec![Value::from("the dog sat there and did nothing")],
				)
				.await
				.unwrap();
				fti.index_document(
					&mut tx,
					&doc4,
					vec![Value::from("the other animals sat there watching")],
				)
				.await
				.unwrap();
				finish(tx, fti).await;
			}

			{
				let (mut tx, fti) = tx_fti(&ds, TreeStoreType::Read, &az, btree_order, hl).await;

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
						(&doc1, Some(-3.4388628)),
						(&doc2, Some(-3.621457)),
						(&doc3, Some(-2.258829)),
						(&doc4, Some(-2.393017)),
					],
				)
				.await;

				let (hits, scr) = search(&mut tx, &fti, "dog").await;
				check_hits(
					&mut tx,
					hits,
					scr,
					vec![
						(&doc1, Some(-0.7832165)),
						(&doc2, Some(-0.8248031)),
						(&doc3, Some(-0.87105393)),
					],
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
