use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Document {
	pub async fn relate(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if table has correct relation status
		self.relation(ctx, opt, stm).await?;
		// Check whether current record exists
		match self.current.doc.is_some() {
			// We attempted to RELATE a document with an ID,
			// and this ID already exists in the database,
			// so we need to update the record instead.
			true => self.relate_update(stk, ctx, opt, stm).await,
			// We attempted to RELATE a document with an ID,
			// which does not exist in the database, or we
			// are creating a new record with a new ID.
			false => self.relate_create(stk, ctx, opt, stm).await,
		}
	}
	// Attempt to run an INSERT clause
	#[inline(always)]
	async fn relate_create(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Alter record data
		self.alter(stk, ctx, opt, stm).await?;
		// Store record edges
		self.edges(ctx, opt, stm).await?;
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
	// Attempt to run an UPDATE clause
	#[inline(always)]
	async fn relate_update(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if allowed
		self.allow(stk, ctx, opt, stm).await?;
		// Store record edges
		self.edges(ctx, opt, stm).await?;
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
