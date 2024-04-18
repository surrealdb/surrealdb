use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn delete(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check where clause
		self.check(stk, ctx, opt, txn, stm).await?;
		// Check if allowed
		self.allow(stk, ctx, opt, txn, stm).await?;
		// Erase document
		self.erase(ctx, opt, stm).await?;
		// Purge index data
		self.index(stk, ctx, opt, txn, stm).await?;
		// Purge record data
		self.purge(stk, ctx, opt, txn, stm).await?;
		// Run table queries
		self.table(stk, ctx, opt, txn, stm).await?;
		// Run lives queries
		self.lives(stk, ctx, opt, txn, stm).await?;
		// Run change feeds queries
		self.changefeeds(ctx, opt, txn, stm).await?;
		// Run event queries
		self.event(stk, ctx, opt, txn, stm).await?;
		// Yield document
		self.pluck(stk, ctx, opt, txn, stm).await
	}
}
