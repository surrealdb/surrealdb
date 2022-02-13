use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn create(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check value type
		self.admit(ctx, opt, exe, stm).await?;
		// Merge record data
		self.merge(ctx, opt, exe, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, exe, stm).await?;
		// Store index data
		self.index(ctx, opt, exe, stm).await?;
		// Store record data
		self.store(ctx, opt, exe, stm).await?;
		// Run table queries
		self.table(ctx, opt, exe, stm).await?;
		// Run lives queries
		self.lives(ctx, opt, exe, stm).await?;
		// Run event queries
		self.event(ctx, opt, exe, stm).await?;
		// Yield document
		self.pluck(ctx, opt, exe, stm).await
	}
}
