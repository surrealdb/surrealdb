use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::index::FullTextParams;
use crate::expr::statements::DefineIndexStatement;
use crate::expr::{Thing, Value};
use crate::idx::docids::DocId;
use crate::idx::docids::seqdocids::SeqDocIds;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::Tokens;
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::offsets::Offset;
use crate::idx::ft::postings::TermFrequency;
use crate::key::index::dl::Dl;
use crate::key::index::td::Td;
use crate::kvs::{Key, Transaction};
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use std::collections::HashSet;

#[revisioned(revision = 1)]
#[derive(Debug, Default)]
struct TermDocument {
	f: TermFrequency,
	o: Vec<Offset>,
}

pub(crate) struct FullTextIndex {
	analyzer: Analyzer,
	highlighting: bool,
	doc_ids: SeqDocIds,
}

impl FullTextIndex {
	pub(crate) async fn new(ctx: &Context, opt: &Options, p: &FullTextParams) -> Result<Self> {
		let tx = ctx.tx();
		let ixs = ctx.get_index_stores();
		let (ns, db) = opt.ns_db()?;
		let az = tx.get_db_analyzer(ns, db, &p.az).await?;
		ixs.mappers().check(&az).await?;
		let analyzer = Analyzer::new(ixs, az)?;
		Ok(Self {
			analyzer,
			highlighting: p.hl,
			doc_ids: SeqDocIds::new(),
		})
	}

	pub(crate) async fn remove_document(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		ix: &DefineIndexStatement,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<()> {
		let (ns, db) = opt.ns_db()?;
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let mut set = HashSet::new();
		let tx = ctx.tx();
		// Get the doc id (if it exists)
		let doc_key: Key = revision::to_vec(&rid.id)?;
		let id = self.doc_ids.get_doc_id(&tx, doc_key).await?;
		if let Some(id) = id {
			// Delete the terms
			for tks in &tokens {
				for t in tks.list() {
					// Extract the term
					let s = tks.get_token_string(t)?;
					// Check if the term has already been deleted
					if set.insert(s) {
						// Delete the term
						let key = Td::new(ns, db, &rid.tb, &ix.name, s, Some(id));
						tx.del(key).await?;
					}
				}
			}
			// Delete the doc length
			let key = Dl::new(ns, db, &rid.tb, &ix.name, id);
			tx.del(key).await?;
		}
		// We're done
		Ok(())
	}

	pub(crate) async fn index_document(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		ix: &DefineIndexStatement,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<()> {
		let (ns, db) = opt.ns_db()?;
		let tx = ctx.tx();
		// Get the doc id (if it exists)
		let doc_key: Key = revision::to_vec(&rid.id)?;
		let id = self.doc_ids.resolve_doc_id(&tx, doc_key).await?;
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let dl = if self.highlighting {
			Self::index_with_offsets(&tx, ns, db, ix, id.doc_id(), tokens).await?
		} else {
			Self::index_without_offsets(&tx, ns, db, ix, id.doc_id(), tokens).await?
		};
		// Set the doc length
		let key = Dl::new(ns, db, &rid.tb, &ix.name, id.doc_id());
		tx.set(key, revision::to_vec(&dl)?, None).await?;
		// We're done
		Ok(())
	}

	async fn index_with_offsets(
		tx: &Transaction,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		id: DocId,
		tokens: Vec<Tokens>,
	) -> Result<DocLength> {
		let (dl, offsets) = Analyzer::extract_offsets(&tokens)?;
		let mut td = TermDocument::default();
		for (t, o) in offsets {
			let key = Td::new(ns, db, &ix.what, &ix.name, t, Some(id));
			td.f = o.len() as TermFrequency;
			td.o = o;
			tx.set(key, revision::to_vec(&td)?, None).await?;
		}
		Ok(dl)
	}

	async fn index_without_offsets(
		tx: &Transaction,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		id: DocId,
		tokens: Vec<Tokens>,
	) -> Result<DocLength> {
		let (dl, tf) = Analyzer::extract_frequencies(&tokens)?;
		let mut td = TermDocument::default();
		for (t, f) in tf {
			let key = Td::new(ns, db, &ix.what, &ix.name, t, Some(id));
			td.f = f;
			tx.set(key, revision::to_vec(&td)?, None).await?;
		}
		Ok(dl)
	}
}
