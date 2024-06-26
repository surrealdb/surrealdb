use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn insert(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check whether current record exists
		match self.current.doc.is_some() {
			// We attempted to INSERT a document with an ID,
			// and this ID already exists in the database,
			// so we need to update the record instead.
			true => self.insert_update(stk, ctx, opt, stm).await,
			// We attempted to INSERT a document with an ID,
			// which does not exist in the database, or we
			// are creating a new record with a new ID.
			false => {
				// First of all let's try to create the record
				match self.insert_create(stk, ctx, opt, stm).await {
					// We received an index exists error, so we
					// ignore the error, and attempt to update the
					// record using the ON DUPLICATE KEY clause
					// with the Record ID received in the error
					Err(Error::IndexExists {
						thing,
						..
					}) => Err(Error::RetryWithId(thing)),
					// If any other error was received, then let's
					// pass that error through and return an error
					Err(e) => Err(e),
					// Otherwise the record creation succeeded
					Ok(v) => Ok(v),
				}
			}
		}
	}
	// Attempt to run an INSERT clause
	#[inline(always)]
	async fn insert_create(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if table has correct relation status
		self.relation(ctx, opt, stm).await?;
		// Merge record data
		self.merge(stk, ctx, opt, stm).await?;
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
		// Store index data
		self.index(stk, ctx, opt, stm).await?;
		// Store record data
		self.store(ctx, opt, stm).await?;
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
	async fn insert_update(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if allowed
		self.allow(stk, ctx, opt, stm).await?;
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
		// Store index data
		self.index(stk, ctx, opt, stm).await?;
		// Store record data
		self.store(ctx, opt, stm).await?;
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
