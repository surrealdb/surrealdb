use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn update(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check where clause
		self.check(ctx, opt, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, stm).await?;
		// Alter record data
		self.alter(ctx, opt, stm).await?;
		// Merge fields data
		self.field(ctx, opt, stm).await?;
		// Reset fields data
		self.reset(ctx, opt, stm).await?;
		// Clean fields data
		self.clean(ctx, opt, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, stm).await?;
		// Store index data
		self.index(ctx, opt, stm).await?;
		// Store record data
		self.store(ctx, opt, stm).await?;
		// Run table queries
		self.table(ctx, opt, stm).await?;
		// Run lives queries
		self.lives(ctx, opt, stm).await?;
		// Run event queries
		self.event(ctx, opt, stm).await?;
		// Yield document
		self.pluck(ctx, opt, stm).await
	}
}
