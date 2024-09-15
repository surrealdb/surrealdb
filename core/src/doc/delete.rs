use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Document {
	pub async fn delete(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check where clause
		self.check(stk, ctx, opt, stm).await?;
		// Check if allowed
		self.allow(stk, ctx, opt, stm).await?;
		// Erase document
		self.erase(ctx, opt, stm).await?;
		// Purge index data
		self.index(stk, ctx, opt, stm).await?;
		// Purge record data
		self.purge(stk, ctx, opt, stm).await?;
		// Run table queries
		self.table(stk, ctx, opt, stm).await?;
		// Run lives queries
		self.lives(stk, ctx, opt, stm).await?;
		// Run change feeds queries
		self.changefeeds(ctx, opt, stm).await?;
		// Run event queries
		self.event(stk, ctx, opt, stm).await?;
		// Yield document
		self.pluck(stk, ctx, opt, stm).await
	}
}
