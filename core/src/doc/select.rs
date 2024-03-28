use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn select(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if record exists
		self.empty(ctx, opt, txn, stm).await?;
		// Check where clause
		self.check(stk, ctx, opt, txn, stm).await?;
		// Check if allowed
		self.allow(stk, ctx, opt, txn, stm).await?;
		// Yield document
		self.pluck(stk, ctx, opt, txn, stm).await
	}
}
