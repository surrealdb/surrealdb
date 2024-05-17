pub(crate) mod analyzer;
mod doclength;
mod highlighter;
mod offsets;
mod postings;
pub(super) mod scorer;
pub(super) mod termdocs;
pub(crate) mod terms;

use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::ft::analyzer::{Analyzer, TermsList, TermsSet};
use crate::idx::ft::doclength::DocLengths;
use crate::idx::ft::highlighter::{Highlighter, Offseter};
use crate::idx::ft::offsets::Offsets;
use crate::idx::ft::postings::Postings;
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::{TermDocs, TermsDocs};
use crate::idx::ft::terms::{TermId, TermLen, Terms};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::IndexStores;
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs;
use crate::kvs::{Key, TransactionType};
use crate::sql::index::SearchParams;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::{Idiom, Object, Thing, Value};
use reblessive::tree::Stk;
use revision::revisioned;
use roaring::treemap::IntoIter;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::ops::BitAnd;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) type MatchRef = u8;

pub(crate) struct FtIndex {
	analyzer: Arc<Analyzer>,
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

#[revisioned(revision = 1)]
#[derive(Default, Serialize, Deserialize)]
struct State {
	total_docs_lengths: u128,
	doc_count: u64,
}

impl VersionedSerdeState for State {}

impl FtIndex {
	pub(crate) async fn new(
		ixs: &IndexStores,
		opt: &Options,
		txn: &Transaction,
		az: &str,
		index_key_base: IndexKeyBase,
		p: &SearchParams,
		tt: TransactionType,
	) -> Result<Self, Error> {
		let mut tx = txn.lock().await;
		let az = tx.get_db_analyzer(opt.ns(), opt.db(), az).await?;
		Self::with_analyzer(ixs, &mut tx, az, index_key_base, p, tt).await
	}
	async fn with_analyzer(
		ixs: &IndexStores,
		run: &mut kvs::Transaction,
		az: DefineAnalyzerStatement,
		index_key_base: IndexKeyBase,
		p: &SearchParams,
		tt: TransactionType,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bs_key();
		let state: State = if let Some(val) = run.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::default()
		};
		let doc_ids = Arc::new(RwLock::new(
			DocIds::new(ixs, run, tt, index_key_base.clone(), p.doc_ids_order, p.doc_ids_cache)
				.await?,
		));
		let doc_lengths = Arc::new(RwLock::new(
			DocLengths::new(
				ixs,
				run,
				index_key_base.clone(),
				p.doc_lengths_order,
				tt,
				p.doc_lengths_cache,
			)
			.await?,
		));
		let postings = Arc::new(RwLock::new(
			Postings::new(ixs, run, index_key_base.clone(), p.postings_order, tt, p.postings_cache)
				.await?,
		));
		let terms = Arc::new(RwLock::new(
			Terms::new(ixs, run, index_key_base.clone(), p.terms_order, tt, p.terms_cache).await?,
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
			analyzer: Arc::new(az.into()),
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

	pub(super) fn terms(&self) -> Arc<RwLock<Terms>> {
		self.terms.clone()
	}

	pub(super) fn analyzer(&self) -> Arc<Analyzer> {
		self.analyzer.clone()
	}

	pub(crate) async fn remove_document(
		&mut self,
		txn: &Transaction,
		rid: &Thing,
	) -> Result<(), Error> {
		let mut tx = txn.lock().await;
		// Extract and remove the doc_id (if any)
		if let Some(doc_id) = self.doc_ids.write().await.remove_doc(&mut tx, rid.into()).await? {
			self.state.doc_count -= 1;

			// Remove the doc length
			if let Some(doc_lengths) =
				self.doc_lengths.write().await.remove_doc_length(&mut tx, doc_id).await?
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
					p.remove_posting(&mut tx, term_id, doc_id).await?;
					// if the term is not present in any document in the index, we can remove it
					let doc_count = self.term_docs.remove_doc(&mut tx, term_id, doc_id).await?;
					if doc_count == 0 {
						t.remove_term_id(&mut tx, term_id).await?;
					}
				}
				// Remove the offsets if any
				if self.highlighting {
					for term_id in term_list {
						// TODO?: Removal can be done with a prefix on doc_id
						self.offsets.remove_offsets(&mut tx, doc_id, term_id).await?;
					}
				}
			}
		}
		Ok(())
	}

	pub(crate) async fn index_document(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<(), Error> {
		// Resolve the doc_id
		let mut tx = txn.lock().await;
		let resolved = self.doc_ids.write().await.resolve_doc_id(&mut tx, rid.into()).await?;
		let doc_id = *resolved.doc_id();
		drop(tx);

		// Extract the doc_lengths, terms en frequencies (and offset)
		let mut t = self.terms.write().await;
		let (doc_length, terms_and_frequencies, offsets) = if self.highlighting {
			let (dl, tf, ofs) = self
				.analyzer
				.extract_terms_with_frequencies_with_offsets(stk, ctx, opt, txn, &mut t, content)
				.await?;
			(dl, tf, Some(ofs))
		} else {
			let (dl, tf) = self
				.analyzer
				.extract_terms_with_frequencies(stk, ctx, opt, txn, &mut t, content)
				.await?;
			(dl, tf, None)
		};

		// Set the doc length
		let mut tx = txn.lock().await;
		let mut dl = self.doc_lengths.write().await;
		if resolved.was_existing() {
			if let Some(old_doc_length) = dl.get_doc_length_mut(&mut tx, doc_id).await? {
				self.state.total_docs_lengths -= old_doc_length as u128;
			}
		}
		dl.set_doc_length(&mut tx, doc_id, doc_length).await?;

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
			p.update_posting(&mut tx, term_id, doc_id, term_freq).await?;
			if let Some(old_term_ids) = &mut old_term_ids {
				old_term_ids.remove(term_id);
			}
			self.term_docs.set_doc(&mut tx, term_id, doc_id).await?;
			terms_ids.insert(term_id);
		}

		// Remove any remaining postings
		if let Some(old_term_ids) = &old_term_ids {
			for old_term_id in old_term_ids {
				p.remove_posting(&mut tx, old_term_id, doc_id).await?;
				let doc_count = self.term_docs.remove_doc(&mut tx, old_term_id, doc_id).await?;
				// if the term does not have anymore postings, we can remove the term
				if doc_count == 0 {
					t.remove_term_id(&mut tx, old_term_id).await?;
				}
			}
		}

		if self.highlighting {
			// Set the offset if any
			if let Some(ofs) = offsets {
				if !ofs.is_empty() {
					for (tid, or) in ofs {
						self.offsets.set_offsets(&mut tx, doc_id, tid, or).await?;
					}
				}
			}
			// In case of an update, w remove the offset for the terms that does not exist anymore
			if let Some(old_term_ids) = old_term_ids {
				for old_term_id in old_term_ids {
					self.offsets.remove_offsets(&mut tx, doc_id, old_term_id).await?;
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

	pub(super) async fn extract_querying_terms(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		query_string: String,
	) -> Result<(TermsList, TermsSet), Error> {
		let t = self.terms.read().await;
		let res =
			self.analyzer.extract_querying_terms(stk, ctx, opt, txn, &t, query_string).await?;
		Ok(res)
	}

	pub(super) async fn get_terms_docs(
		&self,
		tx: &mut kvs::Transaction,
		terms: &TermsList,
	) -> Result<Vec<Option<(TermId, RoaringTreemap)>>, Error> {
		let mut terms_docs = Vec::with_capacity(terms.len());
		for opt_term in terms {
			if let Some((term_id, _)) = opt_term {
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
		tx: &mut kvs::Transaction,
		thg: &Thing,
		terms: &[Option<(TermId, TermLen)>],
		prefix: Value,
		suffix: Value,
		partial: bool,
		idiom: &Idiom,
		doc: &Value,
	) -> Result<Value, Error> {
		let doc_key: Key = thg.into();
		if let Some(doc_id) = self.doc_ids.read().await.get_doc_id(tx, doc_key).await? {
			let mut hl = Highlighter::new(prefix, suffix, partial, idiom, doc);
			for (term_id, term_len) in terms.iter().flatten() {
				let o = self.offsets.get_offsets(tx, doc_id, *term_id).await?;
				if let Some(o) = o {
					hl.highlight(*term_len, o.0);
				}
			}
			return hl.try_into();
		}
		Ok(Value::None)
	}

	pub(super) async fn extract_offsets(
		&self,
		tx: &mut kvs::Transaction,
		thg: &Thing,
		terms: &[Option<(TermId, u32)>],
		partial: bool,
	) -> Result<Value, Error> {
		let doc_key: Key = thg.into();
		if let Some(doc_id) = self.doc_ids.read().await.get_doc_id(tx, doc_key).await? {
			let mut or = Offseter::new(partial);
			for (term_id, term_len) in terms.iter().flatten() {
				let o = self.offsets.get_offsets(tx, doc_id, *term_id).await?;
				if let Some(o) = o {
					or.highlight(*term_len, o.0);
				}
			}
			return or.try_into();
		}
		Ok(Value::None)
	}

	pub(crate) async fn statistics(&self, txn: &Transaction) -> Result<FtStatistics, Error> {
		// TODO do parallel execution
		let mut run = txn.lock().await;
		Ok(FtStatistics {
			doc_ids: self.doc_ids.read().await.statistics(&mut run).await?,
			terms: self.terms.read().await.statistics(&mut run).await?,
			doc_lengths: self.doc_lengths.read().await.statistics(&mut run).await?,
			postings: self.postings.read().await.statistics(&mut run).await?,
		})
	}

	pub(crate) async fn finish(&self, tx: &Transaction) -> Result<(), Error> {
		let mut run = tx.lock().await;
		self.doc_ids.write().await.finish(&mut run).await?;
		self.doc_lengths.write().await.finish(&mut run).await?;
		self.postings.write().await.finish(&mut run).await?;
		self.terms.write().await.finish(&mut run).await?;
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

	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) fn len(&self) -> usize {
		self.iter.len()
	}
	#[cfg(target_arch = "wasm32")]
	pub(crate) fn len(&self) -> usize {
		self.iter.size_hint().0
	}

	pub(crate) async fn next(
		&mut self,
		tx: &mut kvs::Transaction,
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
	use crate::ctx::Context;
	use crate::dbs::{Options, Transaction};
	use crate::idx::ft::scorer::{BM25Scorer, Score};
	use crate::idx::ft::{FtIndex, HitsIterator};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, TransactionType};
	use crate::sql::index::SearchParams;
	use crate::sql::statements::{DefineAnalyzerStatement, DefineStatement};
	use crate::sql::{Array, Statement, Thing, Value};
	use crate::syn;
	use futures::lock::Mutex;
	use reblessive::tree::Stk;
	use std::collections::HashMap;
	use std::sync::Arc;
	use test_log::test;

	async fn check_hits(
		txn: &Transaction,
		hits: Option<HitsIterator>,
		scr: BM25Scorer,
		e: Vec<(&Thing, Option<Score>)>,
	) {
		let mut tx = txn.lock().await;
		if let Some(mut hits) = hits {
			let mut map = HashMap::new();
			while let Some((k, d)) = hits.next(&mut tx).await.unwrap() {
				let s = scr.score(&mut tx, d).await.unwrap();
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
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		fti: &FtIndex,
		qs: &str,
	) -> (Option<HitsIterator>, BM25Scorer) {
		let (term_list, _) =
			fti.extract_querying_terms(stk, ctx, opt, txn, qs.to_string()).await.unwrap();
		let mut tx = txn.lock().await;
		let td = Arc::new(fti.get_terms_docs(&mut tx, &term_list).await.unwrap());
		drop(tx);
		let scr = fti.new_scorer(td.clone()).unwrap().unwrap();
		let hits = fti.new_hits_iterator(td).unwrap();
		(hits, scr)
	}

	pub(super) async fn tx_fti<'a>(
		ds: &Datastore,
		tt: TransactionType,
		az: &DefineAnalyzerStatement,
		order: u32,
		hl: bool,
	) -> (Context<'a>, Options, Transaction, FtIndex) {
		let ctx = Context::default();
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let txn = Arc::new(Mutex::new(tx));
		let mut tx = txn.lock().await;
		let fti = FtIndex::with_analyzer(
			ctx.get_index_stores(),
			&mut tx,
			az.clone(),
			IndexKeyBase::default(),
			&SearchParams {
				az: az.name.clone(),
				doc_ids_order: order,
				doc_lengths_order: order,
				postings_order: order,
				terms_order: order,
				sc: Default::default(),
				hl,
				doc_ids_cache: 100,
				doc_lengths_cache: 100,
				postings_cache: 100,
				terms_cache: 100,
			},
			tt,
		)
		.await
		.unwrap();
		drop(tx);
		(ctx, Options::default(), txn, fti)
	}

	pub(super) async fn finish(txn: &Transaction, fti: FtIndex) {
		fti.finish(txn).await.unwrap();
		txn.lock().await.commit().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_ft_index() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut q = syn::parse("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
		let Statement::Define(DefineStatement::Analyzer(az)) = q.0 .0.pop().unwrap() else {
			panic!()
		};
		let mut stack = reblessive::TreeStack::new();

		let btree_order = 5;

		let doc1: Thing = ("t", "doc1").into();
		let doc2: Thing = ("t", "doc2").into();
		let doc3: Thing = ("t", "doc3").into();

		stack
			.enter(|stk| async {
				// Add one document
				let (ctx, opt, txn, mut fti) =
					tx_fti(&ds, TransactionType::Write, &az, btree_order, false).await;
				fti.index_document(
					stk,
					&ctx,
					&opt,
					&txn,
					&doc1,
					vec![Value::from("hello the world")],
				)
				.await
				.unwrap();
				finish(&txn, fti).await;
			})
			.finish()
			.await;

		stack
			.enter(|stk| async {
				// Add two documents
				let (ctx, opt, txn, mut fti) =
					tx_fti(&ds, TransactionType::Write, &az, btree_order, false).await;
				fti.index_document(
					stk,
					&ctx,
					&opt,
					&txn,
					&doc2,
					vec![Value::from("a yellow hello")],
				)
				.await
				.unwrap();
				fti.index_document(stk, &ctx, &opt, &txn, &doc3, vec![Value::from("foo bar")])
					.await
					.unwrap();
				finish(&txn, fti).await;
			})
			.finish()
			.await;

		stack
			.enter(|stk| async {
				let (ctx, opt, txn, fti) =
					tx_fti(&ds, TransactionType::Read, &az, btree_order, false).await;
				// Check the statistics
				let statistics = fti.statistics(&txn).await.unwrap();
				assert_eq!(statistics.terms.keys_count, 7);
				assert_eq!(statistics.postings.keys_count, 8);
				assert_eq!(statistics.doc_ids.keys_count, 3);
				assert_eq!(statistics.doc_lengths.keys_count, 3);

				// Search & score
				let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "hello").await;
				check_hits(
					&txn,
					hits,
					scr,
					vec![(&doc1, Some(-0.4859746)), (&doc2, Some(-0.4859746))],
				)
				.await;

				let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "world").await;
				check_hits(&txn, hits, scr, vec![(&doc1, Some(0.4859746))]).await;

				let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "yellow").await;
				check_hits(&txn, hits, scr, vec![(&doc2, Some(0.4859746))]).await;

				let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "foo").await;
				check_hits(&txn, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

				let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "bar").await;
				check_hits(&txn, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

				let (hits, _) = search(stk, &ctx, &opt, &txn, &fti, "dummy").await;
				assert!(hits.is_none());
			})
			.finish()
			.await;

		stack
			.enter(|stk| async {
				// Reindex one document
				let (ctx, opt, txn, mut fti) =
					tx_fti(&ds, TransactionType::Write, &az, btree_order, false).await;
				fti.index_document(stk, &ctx, &opt, &txn, &doc3, vec![Value::from("nobar foo")])
					.await
					.unwrap();
				finish(&txn, fti).await;

				let (ctx, opt, txn, fti) =
					tx_fti(&ds, TransactionType::Read, &az, btree_order, false).await;

				// We can still find 'foo'
				let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "foo").await;
				check_hits(&txn, hits, scr, vec![(&doc3, Some(0.56902087))]).await;

				// We can't anymore find 'bar'
				let (hits, _) = search(stk, &ctx, &opt, &txn, &fti, "bar").await;
				assert!(hits.is_none());

				// We can now find 'nobar'
				let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "nobar").await;
				check_hits(&txn, hits, scr, vec![(&doc3, Some(0.56902087))]).await;
			})
			.finish()
			.await;

		{
			// Remove documents
			let (_, _, txn, mut fti) =
				tx_fti(&ds, TransactionType::Write, &az, btree_order, false).await;
			fti.remove_document(&txn, &doc1).await.unwrap();
			fti.remove_document(&txn, &doc2).await.unwrap();
			fti.remove_document(&txn, &doc3).await.unwrap();
			finish(&txn, fti).await;
		}

		stack
			.enter(|stk| async {
				let (ctx, opt, txn, fti) =
					tx_fti(&ds, TransactionType::Read, &az, btree_order, false).await;
				let (hits, _) = search(stk, &ctx, &opt, &txn, &fti, "hello").await;
				assert!(hits.is_none());
				let (hits, _) = search(stk, &ctx, &opt, &txn, &fti, "foo").await;
				assert!(hits.is_none());
			})
			.finish()
			.await;
	}

	async fn test_ft_index_bm_25(hl: bool) {
		// The function `extract_sorted_terms_with_frequencies` is non-deterministic.
		// the inner structures (BTrees) are built with the same terms and frequencies,
		// but the insertion order is different, ending up in different BTree structures.
		// Therefore it makes sense to do multiple runs.
		for _ in 0..10 {
			let ds = Datastore::new("memory").await.unwrap();
			let mut q = syn::parse("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
			let Statement::Define(DefineStatement::Analyzer(az)) = q.0 .0.pop().unwrap() else {
				panic!()
			};
			let mut stack = reblessive::TreeStack::new();

			let doc1: Thing = ("t", "doc1").into();
			let doc2: Thing = ("t", "doc2").into();
			let doc3: Thing = ("t", "doc3").into();
			let doc4: Thing = ("t", "doc4").into();

			let btree_order = 5;
			stack
				.enter(|stk| async {
					let (ctx, opt, txn, mut fti) =
						tx_fti(&ds, TransactionType::Write, &az, btree_order, hl).await;
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&txn,
						&doc1,
						vec![Value::from("the quick brown fox jumped over the lazy dog")],
					)
					.await
					.unwrap();
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&txn,
						&doc2,
						vec![Value::from("the fast fox jumped over the lazy dog")],
					)
					.await
					.unwrap();
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&txn,
						&doc3,
						vec![Value::from("the dog sat there and did nothing")],
					)
					.await
					.unwrap();
					fti.index_document(
						stk,
						&ctx,
						&opt,
						&txn,
						&doc4,
						vec![Value::from("the other animals sat there watching")],
					)
					.await
					.unwrap();
					finish(&txn, fti).await;
				})
				.finish()
				.await;

			stack
				.enter(|stk| async {
					let (ctx, opt, txn, fti) =
						tx_fti(&ds, TransactionType::Read, &az, btree_order, hl).await;

					let statistics = fti.statistics(&txn).await.unwrap();
					assert_eq!(statistics.terms.keys_count, 17);
					assert_eq!(statistics.postings.keys_count, 28);
					assert_eq!(statistics.doc_ids.keys_count, 4);
					assert_eq!(statistics.doc_lengths.keys_count, 4);

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "the").await;
					check_hits(
						&txn,
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

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "dog").await;
					check_hits(
						&txn,
						hits,
						scr,
						vec![
							(&doc1, Some(-0.7832165)),
							(&doc2, Some(-0.8248031)),
							(&doc3, Some(-0.87105393)),
						],
					)
					.await;

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "fox").await;
					check_hits(&txn, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "over").await;
					check_hits(&txn, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "lazy").await;
					check_hits(&txn, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "jumped").await;
					check_hits(&txn, hits, scr, vec![(&doc1, Some(0.0)), (&doc2, Some(0.0))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "nothing").await;
					check_hits(&txn, hits, scr, vec![(&doc3, Some(0.87105393))]).await;

					let (hits, scr) = search(stk, &ctx, &opt, &txn, &fti, "animals").await;
					check_hits(&txn, hits, scr, vec![(&doc4, Some(0.92279965))]).await;

					let (hits, _) = search(stk, &ctx, &opt, &txn, &fti, "dummy").await;
					assert!(hits.is_none());
				})
				.finish()
				.await;
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

	async fn concurrent_task(ds: Arc<Datastore>, az: DefineAnalyzerStatement) {
		let btree_order = 5;
		let doc1: Thing = ("t", "doc1").into();
		let content1 = Value::from(Array::from(vec!["Enter a search term", "Welcome", "Docusaurus blogging features are powered by the blog plugin.", "Simply add Markdown files (or folders) to the blog directory.", "blog", "Regular blog authors can be added to authors.yml.", "authors.yml", "The blog post date can be extracted from filenames, such as:", "2019-05-30-welcome.md", "2019-05-30-welcome/index.md", "A blog post folder can be convenient to co-locate blog post images:", "The blog supports tags as well!", "And if you don't want a blog: just delete this directory, and use blog: false in your Docusaurus config.", "blog: false", "MDX Blog Post", "Blog posts support Docusaurus Markdown features, such as MDX.", "Use the power of React to create interactive blog posts.", "Long Blog Post", "This is the summary of a very long blog post,", "Use a <!-- truncate --> comment to limit blog post size in the list view.", "<!--", "truncate", "-->", "First Blog Post", "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque elementum dignissim ultricies. Fusce rhoncus ipsum tempor eros aliquam consequat. Lorem ipsum dolor sit amet"]));
		let mut stack = reblessive::TreeStack::new();

		let start = std::time::Instant::now();
		while start.elapsed().as_secs() < 3 {
			stack
				.enter(|stk| {
					remove_insert_task(stk, ds.as_ref(), &az, btree_order, &doc1, &content1)
				})
				.finish()
				.await;
		}
	}
	#[test(tokio::test)]
	async fn concurrent_test() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let mut q = syn::parse("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
		let Statement::Define(DefineStatement::Analyzer(az)) = q.0 .0.pop().unwrap() else {
			panic!()
		};
		concurrent_task(ds.clone(), az.clone()).await;
		let task1 = tokio::spawn(concurrent_task(ds.clone(), az.clone()));
		let task2 = tokio::spawn(concurrent_task(ds.clone(), az.clone()));
		let _ = tokio::try_join!(task1, task2).expect("Tasks failed");
	}

	async fn remove_insert_task(
		stk: &mut Stk,
		ds: &Datastore,
		az: &DefineAnalyzerStatement,
		btree_order: u32,
		rid: &Thing,
		content: &Value,
	) {
		let (ctx, opt, txn, mut fti) =
			tx_fti(ds, TransactionType::Write, az, btree_order, false).await;
		fti.remove_document(&txn, rid).await.unwrap();
		fti.index_document(stk, &ctx, &opt, &txn, rid, vec![content.clone()]).await.unwrap();
		finish(&txn, fti).await;
	}

	#[test(tokio::test)]
	async fn remove_insert_sequence() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut stack = reblessive::TreeStack::new();
		let mut q = syn::parse("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
		let Statement::Define(DefineStatement::Analyzer(az)) = q.0 .0.pop().unwrap() else {
			panic!()
		};
		let doc: Thing = ("t", "doc1").into();
		let content = Value::from(Array::from(vec!["Enter a search term","Welcome","Docusaurus blogging features are powered by the blog plugin.","Simply add Markdown files (or folders) to the blog directory.","blog","Regular blog authors can be added to authors.yml.","authors.yml","The blog post date can be extracted from filenames, such as:","2019-05-30-welcome.md","2019-05-30-welcome/index.md","A blog post folder can be convenient to co-locate blog post images:","The blog supports tags as well!","And if you don't want a blog: just delete this directory, and use blog: false in your Docusaurus config.","blog: false","MDX Blog Post","Blog posts support Docusaurus Markdown features, such as MDX.","Use the power of React to create interactive blog posts.","Long Blog Post","This is the summary of a very long blog post,","Use a <!-- truncate --> comment to limit blog post size in the list view.","<!--","truncate","-->","First Blog Post","Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque elementum dignissim ultricies. Fusce rhoncus ipsum tempor eros aliquam consequat. Lorem ipsum dolor sit amet"]));

		for i in 0..5 {
			debug!("Attempt {i}");
			{
				let (ctx, opt, txn, mut fti) =
					tx_fti(&ds, TransactionType::Write, &az, 5, false).await;
				stack
					.enter(|stk| {
						fti.index_document(stk, &ctx, &opt, &txn, &doc, vec![content.clone()])
					})
					.finish()
					.await
					.unwrap();
				finish(&txn, fti).await;
			}

			{
				let (_, _, txn, fti) = tx_fti(&ds, TransactionType::Read, &az, 5, false).await;
				let s = fti.statistics(&txn).await.unwrap();
				assert_eq!(s.terms.keys_count, 113);
			}

			{
				let (_, _, txn, mut fti) = tx_fti(&ds, TransactionType::Write, &az, 5, false).await;
				fti.remove_document(&txn, &doc).await.unwrap();
				finish(&txn, fti).await;
			}

			{
				let (_, _, txn, fti) = tx_fti(&ds, TransactionType::Read, &az, 5, false).await;
				let s = fti.statistics(&txn).await.unwrap();
				assert_eq!(s.terms.keys_count, 0);
			}
		}
	}
}
