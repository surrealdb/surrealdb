use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::index::FullTextParams;
use crate::expr::{Idiom, Scoring, Thing, Value};
use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::docids::seqdocids::SeqDocIds;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::Tokens;
use crate::idx::ft::highlighter::{HighlightParams, Highlighter, Offseter};
use crate::idx::ft::offset::Offset;
use crate::idx::ft::search::Bm25Params;
use crate::idx::ft::{DocLength, Score, TermFrequency};
use crate::idx::planner::iterators::MatchesHitsIterator;
use crate::idx::trees::store::IndexStores;
use crate::key::index::tt::Tt;
use crate::kvs::KeyDecode;
use crate::kvs::Transaction;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use roaring::RoaringTreemap;
use roaring::treemap::IntoIter;
use std::collections::HashSet;
use std::ops::BitAnd;
use uuid::Uuid;

#[revisioned(revision = 1)]
#[derive(Debug, Default)]
struct TermDocument {
	f: TermFrequency,
	o: Vec<Offset>,
}

#[revisioned(revision = 1)]
#[derive(Debug, Default)]
struct DocLengthAndCount {
	total_docs_length: i128,
	doc_count: i64,
}

pub(in crate::idx) struct QueryTerms {
	#[allow(dead_code)]
	tokens: Tokens,
	#[allow(dead_code)]
	docs: Vec<Option<RoaringTreemap>>,
	#[allow(dead_code)]
	has_unknown_terms: bool,
}

impl QueryTerms {
	pub(in crate::idx) fn is_empty(&self) -> bool {
		self.tokens.list().is_empty()
	}

	pub(in crate::idx) fn contains_doc(&self, doc_id: DocId) -> bool {
		for d in self.docs.iter().flatten() {
			if d.contains(doc_id) {
				return true;
			}
		}
		false
	}
}

pub(crate) struct FullTextIndex {
	ikb: IndexKeyBase,
	analyzer: Analyzer,
	highlighting: bool,
	doc_ids: SeqDocIds,
	bm25: Option<Bm25Params>,
}

impl FullTextIndex {
	pub(crate) async fn new(
		nid: Uuid,
		ixs: &IndexStores,
		tx: &Transaction,
		ikb: IndexKeyBase,
		p: &FullTextParams,
	) -> Result<Self> {
		let az = tx.get_db_analyzer(&ikb.0.ns, &ikb.0.db, &p.az).await?;
		ixs.mappers().check(&az).await?;
		let analyzer = Analyzer::new(ixs, az)?;
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
			analyzer,
			highlighting: p.hl,
			doc_ids: SeqDocIds::new(nid, ikb.clone()),
			ikb,
			bm25,
		})
	}

	pub(crate) async fn remove_content(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<Option<DocId>> {
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let mut set = HashSet::new();
		let tx = ctx.tx();
		let nid = opt.id()?;
		// Get the doc id (if it exists)
		let doc_id = self.get_doc_id(&tx, rid).await?;
		if let Some(doc_id) = doc_id {
			// Delete the terms
			for tks in &tokens {
				for t in tks.list() {
					// Extract the term
					let s = tks.get_token_string(t)?;
					// Check if the term has already been deleted
					if set.insert(s) {
						// Delete the term
						let key = self.ikb.new_td(s, Some(doc_id));
						tx.del(key).await?;
						self.set_tt(&tx, s, doc_id, &nid, false).await?;
					}
				}
			}
			{
				let key = self.ikb.new_dl(doc_id);
				// get the doc length
				if let Some(v) = tx.get(key.clone(), None).await? {
					// Delete the doc length
					tx.del(key).await?;
					// Decrease the doc count and total doc length
					let dl: DocLength = revision::from_slice(&v)?;
					let dcl = DocLengthAndCount {
						total_docs_length: -(dl as i128),
						doc_count: -1,
					};
					let key = self.ikb.new_dc_with_id(doc_id, opt.id()?, Uuid::now_v7());
					tx.put(key, revision::to_vec(&dcl)?, None).await?;
				}
			}
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	/// This method assumes that remove_content has been called previously,
	/// as it does not remove the content (terms) but only removes the doc_id reference.
	pub(crate) async fn remove_doc(&self, ctx: &Context, doc_id: DocId) -> Result<()> {
		self.doc_ids.remove_doc_id(&ctx.tx(), doc_id).await
	}

	pub(crate) async fn index_content(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<()> {
		let tx = ctx.tx();
		let nid = opt.id()?;
		// Get the doc id (if it exists)
		let id = self.doc_ids.resolve_doc_id(ctx, rid.id.clone()).await?;
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let dl = if self.highlighting {
			self.index_with_offsets(&nid, &tx, id.doc_id(), tokens).await?
		} else {
			self.index_without_offsets(&nid, &tx, id.doc_id(), tokens).await?
		};
		{
			// Set the doc length
			let key = self.ikb.new_dl(id.doc_id());
			tx.set(key, revision::to_vec(&dl)?, None).await?;
		}
		{
			// Increase the doc count and total doc length
			let key = self.ikb.new_dc_with_id(id.doc_id(), opt.id()?, Uuid::now_v7());
			let dcl = DocLengthAndCount {
				total_docs_length: dl as i128,
				doc_count: 1,
			};
			tx.put(key, revision::to_vec(&dcl)?, None).await?;
		}
		// We're done
		Ok(())
	}

	async fn get_doc_length(&self, tx: &Transaction, doc_id: DocId) -> Result<Option<DocLength>> {
		let key = self.ikb.new_dl(doc_id);
		if let Some(v) = tx.get(key, None).await? {
			let dl: DocLength = revision::from_slice(&v)?;
			Ok(Some(dl))
		} else {
			Ok(None)
		}
	}

	async fn index_with_offsets(
		&self,
		nid: &Uuid,
		tx: &Transaction,
		id: DocId,
		tokens: Vec<Tokens>,
	) -> Result<DocLength> {
		let (dl, offsets) = Analyzer::extract_offsets(&tokens)?;
		let mut td = TermDocument::default();
		for (t, o) in offsets {
			{
				let key = self.ikb.new_td(t, Some(id));
				td.f = o.len() as TermFrequency;
				td.o = o;
				tx.set(key, revision::to_vec(&td)?, None).await?;
				self.set_tt(tx, t, id, nid, true).await?;
			}
		}
		Ok(dl)
	}

	async fn index_without_offsets(
		&self,
		nid: &Uuid,
		tx: &Transaction,
		id: DocId,
		tokens: Vec<Tokens>,
	) -> Result<DocLength> {
		let (dl, tf) = Analyzer::extract_frequencies(&tokens)?;
		let mut td = TermDocument::default();
		for (t, f) in tf {
			let key = self.ikb.new_td(t, Some(id));
			td.f = f;
			tx.set(key, revision::to_vec(&td)?, None).await?;
			self.set_tt(tx, t, id, nid, true).await?;
		}
		Ok(dl)
	}

	async fn set_tt(
		&self,
		tx: &Transaction,
		term: &str,
		doc_id: DocId,
		nid: &Uuid,
		add: bool,
	) -> Result<()> {
		let key = self.ikb.new_tt(term, doc_id, *nid, Uuid::now_v7(), add);
		tx.set(key, "", None).await
	}

	pub(in crate::idx) async fn extract_querying_terms(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		query_string: String,
	) -> Result<QueryTerms> {
		// We extract the tokens
		let tokens = self
			.analyzer
			.generate_tokens(stk, ctx, opt, FilteringStage::Querying, query_string)
			.await?;
		// We collect the term docs
		let mut docs = Vec::with_capacity(tokens.list().len());
		let mut unique_tokens = HashSet::new();
		let tx = ctx.tx();
		let mut has_unknown_terms = false;
		for token in tokens.list() {
			// Tokens can contain duplicates, not need to evaluate them again
			if unique_tokens.insert(token) {
				// Is the term known in the index?
				let term = tokens.get_token_string(token)?;
				let d = self.get_docs(&tx, term).await?;
				if !has_unknown_terms && d.is_none() {
					has_unknown_terms = true;
				}
				docs.push(d);
			}
		}
		Ok(QueryTerms {
			tokens,
			docs,
			has_unknown_terms,
		})
	}

	async fn get_docs(&self, tx: &Transaction, term: &str) -> Result<Option<RoaringTreemap>> {
		// First we read the compacted documents
		let mut docs = None;
		let td = self.ikb.new_td(term, None);
		if let Some(v) = tx.get(td, None).await? {
			docs = Some(RoaringTreemap::deserialize_from(&mut v.as_slice())?);
		}
		// Then we read the not yet compacted term/documents if any
		let (beg, end) = self.ikb.new_tt_range(term)?;
		for k in tx.keys(beg..end, u32::MAX, None).await? {
			let tt = Tt::decode(&k)?;
			if let Some(docs) = &mut docs {
				if tt.add {
					docs.insert(tt.doc_id);
				} else {
					docs.remove(tt.doc_id);
				}
			} else if tt.add {
				docs = Some(RoaringTreemap::from_iter(vec![tt.doc_id]));
			}
		}
		// If `docs` is empty, we return `None`
		Ok(docs.filter(|docs| !docs.is_empty()))
	}

	async fn compact_term_docs(&self, _tx: &Transaction) -> Result<()> {
		todo!()
	}

	pub(in crate::idx) fn new_hits_iterator(
		&self,
		qt: &QueryTerms,
	) -> Result<Option<FullTextHitsIterator>> {
		let mut hits: Option<RoaringTreemap> = None;
		for opt_docs in qt.docs.iter() {
			if let Some(docs) = opt_docs {
				if let Some(h) = hits {
					hits = Some(h.bitand(docs));
				} else {
					hits = Some(docs.clone());
				}
			} else {
				return Ok(None);
			}
		}
		if let Some(hits) = hits {
			if !hits.is_empty() {
				return Ok(Some(FullTextHitsIterator::new(self.ikb.clone(), hits)));
			}
		}
		Ok(None)
	}

	pub(in crate::idx) async fn get_doc_id(
		&self,
		tx: &Transaction,
		rid: &Thing,
	) -> Result<Option<DocId>> {
		if !rid.tb.eq(self.ikb.table()) {
			return Ok(None);
		}
		self.doc_ids.get_doc_id(tx, &rid.id).await
	}
	pub(in crate::idx) async fn new_scorer(&self, ctx: &Context) -> Result<Option<Scorer>> {
		if let Some(bm25) = &self.bm25 {
			let dlc = self.compute_doc_length_and_count(&ctx.tx()).await?;
			let sc = Scorer::new(dlc, bm25.clone());
			return Ok(Some(sc));
		}
		Ok(None)
	}

	async fn compute_doc_length_and_count(&self, tx: &Transaction) -> Result<DocLengthAndCount> {
		let mut dlc = DocLengthAndCount::default();
		let (beg, end) = self.ikb.new_dc_range()?;
		// Compute the total number of documents (DocCount) and the total number of terms (DocLength)
		// This key list is supposed to be small, subject to compaction.
		// The root key is the compacted values, and the others are deltas from transaction not yet compacted.
		for (_, v) in tx.getr(beg..end, None).await? {
			let st: DocLengthAndCount = revision::from_slice(&v)?;
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;
		}
		Ok(dlc)
	}

	async fn compact_doc_length_and_count(&self, tx: &Transaction) -> Result<()> {
		let dlc = self.compute_doc_length_and_count(tx).await?;
		let key = self.ikb.new_dc_compacted()?;
		tx.set(key, revision::to_vec(&dlc)?, None).await?;
		Ok(())
	}

	pub(crate) async fn compaction(&self, tx: &Transaction) -> Result<()> {
		self.compact_doc_length_and_count(tx).await?;
		self.compact_term_docs(tx).await?;
		Ok(())
	}

	pub(in crate::idx) async fn highlight(
		&self,
		tx: &Transaction,
		thg: &Thing,
		qt: &QueryTerms,
		hlp: HighlightParams,
		idiom: &Idiom,
		doc: &Value,
	) -> Result<Value> {
		let doc_id = self.get_doc_id(tx, thg).await?;
		if let Some(doc_id) = doc_id {
			let mut hl = Highlighter::new(hlp, idiom, doc);
			for tk in qt.tokens.list() {
				if let Some(td) =
					self.get_term_document(tx, doc_id, qt.tokens.get_token_string(tk)?).await?
				{
					hl.highlight(tk.get_char_len(), td.o);
				}
			}
			return hl.try_into();
		}
		Ok(Value::None)
	}

	async fn get_term_document(
		&self,
		tx: &Transaction,
		id: DocId,
		term: &str,
	) -> Result<Option<TermDocument>> {
		let key = self.ikb.new_td(term, Some(id));
		if let Some(v) = tx.get(key, None).await? {
			let td: TermDocument = revision::from_slice(&v)?;
			Ok(Some(td))
		} else {
			Ok(None)
		}
	}

	pub(in crate::idx) async fn read_offsets(
		&self,
		tx: &Transaction,
		thg: &Thing,
		qt: &QueryTerms,
		partial: bool,
	) -> Result<Value> {
		let doc_id = self.get_doc_id(tx, thg).await?;
		if let Some(doc_id) = doc_id {
			let mut or = Offseter::new(partial);
			for tk in qt.tokens.list() {
				let term = qt.tokens.get_token_string(tk)?;
				let o = self.get_term_document(tx, doc_id, term).await?;
				if let Some(o) = o {
					or.highlight(tk.get_char_len(), o.o);
				}
			}
			return or.try_into().map_err(anyhow::Error::new);
		}
		Ok(Value::None)
	}
}

pub(crate) struct FullTextHitsIterator {
	ikb: IndexKeyBase,
	iter: IntoIter,
}

impl FullTextHitsIterator {
	fn new(ikb: IndexKeyBase, hits: RoaringTreemap) -> Self {
		Self {
			ikb,
			iter: hits.into_iter(),
		}
	}
}

impl MatchesHitsIterator for FullTextHitsIterator {
	#[cfg(target_pointer_width = "64")]
	fn len(&self) -> usize {
		self.iter.len()
	}
	#[cfg(not(target_pointer_width = "64"))]
	fn len(&self) -> usize {
		self.iter.size_hint().0
	}

	async fn next(&mut self, tx: &Transaction) -> Result<Option<(Thing, DocId)>> {
		for doc_id in self.iter.by_ref() {
			if let Some(id) = SeqDocIds::get_id(&self.ikb, tx, doc_id).await? {
				let rid = Thing {
					tb: self.ikb.table().to_string(),
					id,
				};
				return Ok(Some((rid, doc_id)));
			}
		}
		Ok(None)
	}
}

pub(in crate::idx) struct Scorer {
	bm25: Bm25Params,
	average_doc_length: f32,
	doc_count: f32,
}

impl Scorer {
	fn new(dlc: DocLengthAndCount, bm25: Bm25Params) -> Self {
		let doc_count = dlc.doc_count as f32;
		let average_doc_length = (dlc.total_docs_length as f32) / doc_count;
		Self {
			bm25,
			doc_count,
			average_doc_length,
		}
	}

	async fn term_score(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		doc_id: DocId,
		term_doc_count: DocLength,
		term_frequency: TermFrequency,
	) -> Result<Score> {
		let doc_length = fti.get_doc_length(tx, doc_id).await?.unwrap_or(0);
		Ok(self.compute_bm25_score(term_frequency as f32, term_doc_count as f32, doc_length as f32))
	}

	pub(crate) async fn score(
		&self,
		fti: &FullTextIndex,
		tx: &Transaction,
		qt: &QueryTerms,
		doc_id: DocId,
	) -> Result<Score> {
		let mut sc = 0.0;
		let tl = qt.tokens.list();
		for (i, d) in qt.docs.iter().enumerate() {
			if let Some(docs) = d {
				if docs.contains(doc_id) {
					if let Some(token) = tl.get(i) {
						let term = qt.tokens.get_token_string(token)?;
						let td = fti.get_term_document(tx, doc_id, term).await?;
						if let Some(td) = td {
							sc += self.term_score(fti, tx, doc_id, docs.len(), td.f).await?;
						}
					}
				}
			}
		}
		Ok(sc)
	}

	// https://en.wikipedia.org/wiki/Okapi_BM25
	// Including the lower-bounding term frequency normalization (2011 CIKM)
	// Floor for Negative Scores
	fn compute_bm25_score(&self, term_freq: f32, term_doc_count: f32, doc_length: f32) -> f32 {
		// (n(qi) + 0.5)
		let denominator = term_doc_count + 0.5;
		// (N - n(qi) + 0.5)
		let numerator = self.doc_count - term_doc_count + 0.5;
		// Calculate IDF with floor
		let idf = (numerator / denominator).ln().max(0.0);
		if idf.is_nan() {
			return f32::NAN;
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
