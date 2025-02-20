use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::ft::analyzer::Analyzer;
use crate::kvs::Transaction;
use crate::sql::index::Search2Params;
use crate::sql::{Thing, Value};
use reblessive::tree::Stk;
use std::sync::Arc;

pub(crate) struct Search2 {
	tx: Arc<Transaction>,
	analyzer: Analyzer,
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
			tx,
			analyzer,
		})
	}

	pub(crate) async fn remove_document(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<(), Error> {
		todo!()
	}

	pub(crate) async fn index_document(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<(), Error> {
		todo!()
	}
}
