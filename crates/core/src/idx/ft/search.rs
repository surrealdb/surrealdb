use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::Tokens;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::offsets::Offset;
use crate::idx::ft::postings::TermFrequency;
use crate::key::index::dl::Dl;
use crate::key::index::td::Td;
use crate::kvs::Transaction;
use crate::sql::index::Search2Params;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Thing, Value};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[revisioned(revision = 1)]
#[derive(Debug, Default, Serialize, Deserialize)]
struct TermDocument {
	f: TermFrequency,
	o: Vec<Offset>,
}

pub(crate) struct Search2 {
	analyzer: Analyzer,
	highlighting: bool,
}

impl Search2 {
	pub(crate) async fn new(
		ctx: &Context,
		opt: &Options,
		p: &Search2Params,
	) -> Result<Self, Error> {
		let tx = ctx.tx();
		let ixs = ctx.get_index_stores();
		let (ns, db) = opt.ns_db()?;
		let az = tx.get_db_analyzer(ns, db, &p.az).await?;
		ixs.mappers().check(&az).await?;
		let analyzer = Analyzer::new(ixs, az)?;
		Ok(Self {
			analyzer,
			highlighting: p.hl,
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
	) -> Result<(), Error> {
		let (ns, db) = opt.ns_db()?;
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let mut set = HashSet::new();
		let tx = ctx.tx();
		// Delete the terms
		for tks in &tokens {
			for t in tks.list() {
				// Extract the term
				let s = tks.get_token_string(t)?;
				// Check if the term has already been deleted
				if set.insert(s) {
					// Delete the term
					let key = Td::new(ns, db, &rid.tb, &ix.name, s, &rid.id);
					tx.del(key).await?;
				}
			}
		}
		// Delete the doc length
		let key = Dl::new(ns, db, &rid.tb, &ix.name, &rid.id);
		tx.del(key).await?;
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
	) -> Result<(), Error> {
		let (ns, db) = opt.ns_db()?;
		let tx = ctx.tx();
		// Collect the tokens.
		let tokens =
			self.analyzer.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing).await?;
		let dl = if self.highlighting {
			Self::index_with_offsets(&tx, ns, db, ix, rid, tokens).await?
		} else {
			Self::index_without_offsets(&tx, ns, db, ix, rid, tokens).await?
		};
		// Set the doc length
		let key = Dl::new(ns, db, &rid.tb, &ix.name, &rid.id);
		tx.set(key, revision::to_vec(&dl)?, None).await?;
		// We're done
		Ok(())
	}

	async fn index_with_offsets(
		tx: &Transaction,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		rid: &Thing,
		tokens: Vec<Tokens>,
	) -> Result<DocLength, Error> {
		let (dl, offsets) = Analyzer::extract_offsets(&tokens)?;
		let mut td = TermDocument::default();
		for (t, o) in offsets {
			let key = Td::new(ns, db, &rid.tb, &ix.name, t, &rid.id);
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
		rid: &Thing,
		tokens: Vec<Tokens>,
	) -> Result<DocLength, Error> {
		let (dl, tf) = Analyzer::extract_frequencies(&tokens)?;
		let mut td = TermDocument::default();
		for (t, f) in tf {
			let key = Td::new(ns, db, &rid.tb, &ix.name, t, &rid.id);
			td.f = f;
			tx.set(key, revision::to_vec(&td)?, None).await?;
		}
		Ok(dl)
	}
}
