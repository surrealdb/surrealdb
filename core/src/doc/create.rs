use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Document {
	pub async fn create(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if table has current relation status
		self.relation(ctx, opt, stm).await?;
		// Alter record data
		self.alter(stk, ctx, opt, stm).await?;
		// Merge fields data
		self.field(stk, ctx, opt, stm).await?;
		// Reset fields data
		self.reset(ctx, opt, stm).await?;
		// Clean fields data
		self.clean(stk, ctx, opt, stm).await?;
		// Check if allowed
		self.allow(stk, ctx, opt, stm).await?;
		// Store record data
		self.store(ctx, opt, stm).await?;
		// Store index data
		self.index(stk, ctx, opt, stm).await?;
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
