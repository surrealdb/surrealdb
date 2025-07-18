use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::index::FullTextParams;
use crate::expr::statements::DefineAnalyzerStatement;
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
use std::collections::{HashMap, HashSet};
use std::ops::BitAnd;
use std::sync::Arc;
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
		Self::with_analyzer(nid, ixs, az, ikb, p)
	}

	fn with_analyzer(
		nid: Uuid,
		ixs: &IndexStores,
		az: Arc<DefineAnalyzerStatement>,
		ikb: IndexKeyBase,
		p: &FullTextParams,
	) -> Result<Self> {
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
		require_compaction: &mut bool,
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
					*require_compaction = true;
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
		require_compaction: &mut bool,
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
			*require_compaction = true;
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
		// We compute the not yet compacted term/documents if any
		let (beg, end) = self.ikb.new_tt_term_range(term)?;
		let mut deltas: HashMap<DocId, i64> = HashMap::new();
		for k in tx.keys(beg..end, u32::MAX, None).await? {
			let tt = Tt::decode(&k)?;
			let entry = deltas.entry(tt.doc_id).or_default();
			if tt.add {
				*entry += 1;
			} else {
				*entry -= 1;
			}
		}
		// Append the delta term docs to the consolidated docs
		let docs = self.append_term_docs_delta(tx, term, &deltas).await?;
		// If the final `docs` is empty, we return `None`
		if docs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(docs))
		}
	}

	async fn append_term_docs_delta(
		&self,
		tx: &Transaction,
		term: &str,
		deltas: &HashMap<DocId, i64>,
	) -> Result<RoaringTreemap> {
		// We read the compacted term docs
		let td = self.ikb.new_td(term, None);
		let mut docs = match tx.get(td, None).await? {
			None => RoaringTreemap::default(),
			Some(v) => revision::from_slice(&v)?,
		};
		// And apply the deltas
		for (doc_id, delta) in deltas {
			if *delta < 0 {
				docs.remove(*doc_id);
			} else if *delta > 0 {
				docs.insert(*doc_id);
			}
		}
		Ok(docs)
	}
	async fn set_term_docs_delta(
		&self,
		tx: &Transaction,
		term: &str,
		deltas: &HashMap<DocId, i64>,
	) -> Result<()> {
		let docs = self.append_term_docs_delta(tx, term, deltas).await?;
		let td = self.ikb.new_td(term, None);
		if docs.is_empty() {
			tx.del(td).await?;
		} else {
			tx.set(td, revision::to_vec(&docs)?, None).await?;
		}
		Ok(())
	}

	async fn compact_term_docs(&self, tx: &Transaction) -> Result<()> {
		let (beg, end) = self.ikb.new_tt_terms_range()?;
		let mut current_term = "".to_string();
		let mut deltas: HashMap<DocId, i64> = HashMap::new();
		let range = beg..end;
		for k in tx.keys(range.clone(), u32::MAX, None).await? {
			let tt = Tt::decode(&k)?;
			// Is this a new term?
			if current_term != tt.term {
				if !current_term.is_empty() && !deltas.is_empty() {
					self.set_term_docs_delta(tx, &current_term, &deltas).await?;
					deltas.clear();
				}
				current_term = tt.term.to_string();
			}
			// Update the deltas
			let entry = deltas.entry(tt.doc_id).or_default();
			if tt.add {
				*entry += 1;
			} else {
				*entry -= 1;
			}
		}
		// Delete the logs
		tx.delr(range).await?;
		if !current_term.is_empty() && !deltas.is_empty() {
			self.set_term_docs_delta(tx, &current_term, &deltas).await?;
		}
		Ok(())
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
			let dlc = self.compute_doc_length_and_count(&ctx.tx(), false).await?;
			let sc = Scorer::new(dlc, bm25.clone());
			return Ok(Some(sc));
		}
		Ok(None)
	}

	async fn compute_doc_length_and_count(
		&self,
		tx: &Transaction,
		compact_log: bool,
	) -> Result<DocLengthAndCount> {
		let mut dlc = DocLengthAndCount::default();
		let (beg, end) = self.ikb.new_dc_range()?;
		let range = beg..end;
		// Compute the total number of documents (DocCount) and the total number of terms (DocLength)
		// This key list is supposed to be small, subject to compaction.
		// The root key is the compacted values, and the others are deltas from transaction not yet compacted.
		for (_, v) in tx.getr(range.clone(), None).await? {
			let st: DocLengthAndCount = revision::from_slice(&v)?;
			dlc.doc_count += st.doc_count;
			dlc.total_docs_length += st.total_docs_length;
		}
		if compact_log {
			tx.delr(range).await?;
		}
		Ok(dlc)
	}

	async fn compact_doc_length_and_count(&self, tx: &Transaction) -> Result<()> {
		let dlc = self.compute_doc_length_and_count(tx, true).await?;
		let key = self.ikb.new_dc_compacted()?;
		tx.set(key, revision::to_vec(&dlc)?, None).await?;
		Ok(())
	}

	pub(crate) async fn compaction(&self, tx: &Transaction) -> Result<()> {
		self.compact_doc_length_and_count(tx).await?;
		self.compact_term_docs(tx).await?;
		Ok(())
	}

	pub(crate) async fn trigger_compaction(
		ikb: &IndexKeyBase,
		tx: &Transaction,
		nid: Uuid,
	) -> Result<()> {
		let ic = ikb.new_ic_key(nid);
		tx.put(ic, b"0", None).await?;
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

#[cfg(test)]
mod tests {
	use super::FullTextIndex;
	use crate::ctx::{Context, MutableContext};
	use crate::dbs::Options;
	use crate::expr::index::FullTextParams;
	use crate::expr::statements::DefineAnalyzerStatement;
	use crate::expr::{Array, Thing, Value};
	use crate::idx::IndexKeyBase;
	use crate::key::root::ic::Ic;
	use crate::kvs::{Datastore, LockType::*, Transaction, TransactionType};
	use crate::sql::{Statement, statements::DefineStatement};
	use crate::syn;
	use reblessive::tree::Stk;
	use std::sync::Arc;
	use std::time::{Duration, Instant};
	use test_log::test;
	use tokio::time::sleep;
	use uuid::Uuid;

	#[derive(Clone)]
	struct TestContext {
		ctx: Context,
		opt: Options,
		nid: Uuid,
		start: Arc<Instant>,
		ds: Arc<Datastore>,
		content: Arc<Value>,
		ikb: IndexKeyBase,
		fti: Arc<FullTextIndex>,
	}

	impl TestContext {
		async fn new() -> Self {
			let ds = Arc::new(Datastore::new("memory").await.unwrap());
			let ctx = ds.setup_ctx().unwrap().freeze();
			let mut q = syn::parse("DEFINE ANALYZER test TOKENIZERS blank;").unwrap();
			let Statement::Define(DefineStatement::Analyzer(az)) = q.0.0.pop().unwrap() else {
				panic!()
			};
			let az: Arc<DefineAnalyzerStatement> = Arc::new(az.into());
			let content = Arc::new(Value::from(Array::from(vec![
				"Enter a search term",
				"Welcome",
				"Docusaurus blogging features are powered by the blog plugin.",
				"Simply add Markdown files (or folders) to the blog directory.",
				"blog",
				"Regular blog authors can be added to authors.yml.",
				"authors.yml",
				"The blog post date can be extracted from filenames, such as:",
				"2019-05-30-welcome.md",
				"2019-05-30-welcome/index.md",
				"A blog post folder can be convenient to co-locate blog post images:",
				"The blog supports tags as well!",
				"And if you don't want a blog: just delete this directory, and use blog: false in your Docusaurus config.",
				"blog: false",
				"MDX Blog Post",
				"Blog posts support Docusaurus Markdown features, such as MDX.",
				"Use the power of React to create interactive blog posts.",
				"Long Blog Post",
				"This is the summary of a very long blog post,",
				"Use a <!-- truncate --> comment to limit blog post size in the list view.",
				"<!--",
				"truncate",
				"-->",
				"First Blog Post",
				"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque elementum dignissim ultricies. Fusce rhoncus ipsum tempor eros aliquam consequat. Lorem ipsum dolor sit amet",
			])));
			let ft_params = Arc::new(FullTextParams {
				az: az.name.clone(),
				sc: Default::default(),
				hl: false,
			});
			let nid = Uuid::from_u128(1);
			let ikb = IndexKeyBase::default();
			let opt = Options::default().with_id(nid);
			let fti = Arc::new(
				FullTextIndex::with_analyzer(
					nid,
					ctx.get_index_stores(),
					az.clone(),
					ikb.clone(),
					&ft_params,
				)
				.unwrap(),
			);
			let start = Arc::new(Instant::now());
			Self {
				ctx,
				opt,
				nid,
				ikb,
				start,
				ds,
				content,
				fti,
			}
		}

		async fn new_tx(&self, tt: TransactionType) -> Arc<Transaction> {
			Arc::new(self.ds.transaction(tt, Optimistic).await.unwrap())
		}

		async fn remove_insert_task(&self, stk: &mut Stk, rid: &Thing) {
			let mut ctx = MutableContext::new(&self.ctx);
			let tx = self.new_tx(TransactionType::Write).await;
			ctx.set_transaction(tx.clone());
			let ctx = ctx.freeze();

			let mut require_compaction = false;
			self.fti
				.remove_content(
					stk,
					&ctx,
					&self.opt,
					&rid,
					vec![self.content.as_ref().clone()],
					&mut require_compaction,
				)
				.await
				.unwrap();
			self.fti
				.index_content(
					stk,
					&ctx,
					&self.opt,
					&rid,
					vec![self.content.as_ref().clone()],
					&mut require_compaction,
				)
				.await
				.unwrap();

			if require_compaction {
				FullTextIndex::trigger_compaction(&self.ikb, &tx, self.nid).await.unwrap();
			}

			tx.commit().await.unwrap();
		}
	}

	async fn concurrent_doc_update(test: TestContext, rid: Arc<Thing>) {
		let mut stack = reblessive::TreeStack::new();
		while test.start.elapsed().as_secs() < 3 {
			stack.enter(|stk| test.remove_insert_task(stk, &rid)).finish().await;
		}
	}

	async fn compaction(test: TestContext) {
		let duration = Duration::from_secs(1);
		while test.start.elapsed().as_secs() < 4 {
			sleep(duration).await;
			test.ds.index_compaction(duration).await.unwrap();
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn concurrent_test() {
		let doc1: Arc<Thing> = Arc::new(("t", "doc1").into());
		let doc2: Arc<Thing> = Arc::new(("t", "doc2").into());

		let test = TestContext::new().await;
		let task1 = tokio::spawn(concurrent_doc_update(test.clone(), doc1.clone()));
		let task2 = tokio::spawn(concurrent_doc_update(test.clone(), doc2.clone()));
		let task3 = tokio::spawn(compaction(test.clone()));
		let _ = tokio::try_join!(task1, task2, task3).expect("Tasks failed");

		// Check that logs have been compacted:
		let tx = test.new_tx(TransactionType::Read).await;
		let (beg, end) = Ic::range();
		assert_eq!(tx.count(beg..end).await.unwrap(), 0);
		let (beg, end) = test.ikb.new_dc_range().unwrap();
		assert_eq!(tx.count(beg..end).await.unwrap(), 0);
		let (beg, end) = test.ikb.new_tt_terms_range().unwrap();
		assert_eq!(tx.count(beg..end).await.unwrap(), 0);
	}
}
