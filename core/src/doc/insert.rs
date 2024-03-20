use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn insert(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check whether current record exists
		match self.current.doc.is_some() {
			// We attempted to INSERT a document with an ID,
			// and this ID already exists in the database,
			// so we need to update the record instead.
			true => self.insert_update(ctx, opt, txn, stm).await,
			// We attempted to INSERT a document with an ID,
			// which does not exist in the database, or we
			// are creating a new record with a new ID.
			false => {
				// First of all let's try to create the record
				match self.insert_create(ctx, opt, txn, stm).await {
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
	async fn insert_create(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if table has correct relation status
		self.relation(ctx, opt, txn, stm).await?;
		// Merge record data
		self.merge(ctx, opt, txn, stm).await?;
		// Merge fields data
		self.field(ctx, opt, txn, stm).await?;
		// Reset fields data
		self.reset(ctx, opt, txn, stm).await?;
		// Clean fields data
		self.clean(ctx, opt, txn, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, txn, stm).await?;
		// Store index data
		self.index(ctx, opt, txn, stm).await?;
		// Store record data
		self.store(ctx, opt, txn, stm).await?;
		// Run table queries
		self.table(ctx, opt, txn, stm).await?;
		// Run lives queries
		self.lives(ctx, opt, txn, stm).await?;
		// Run change feeds queries
		self.changefeeds(ctx, opt, txn, stm).await?;
		// Run event queries
		self.event(ctx, opt, txn, stm).await?;
		// Yield document
		self.pluck(ctx, opt, txn, stm).await
	}
	// Attempt to run an UPDATE clause
	async fn insert_update(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if allowed
		self.allow(ctx, opt, txn, stm).await?;
		// Alter record data
		self.alter(ctx, opt, txn, stm).await?;
		// Merge fields data
		self.field(ctx, opt, txn, stm).await?;
		// Reset fields data
		self.reset(ctx, opt, txn, stm).await?;
		// Clean fields data
		self.clean(ctx, opt, txn, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, txn, stm).await?;
		// Store index data
		self.index(ctx, opt, txn, stm).await?;
		// Store record data
		self.store(ctx, opt, txn, stm).await?;
		// Run table queries
		self.table(ctx, opt, txn, stm).await?;
		// Run lives queries
		self.lives(ctx, opt, txn, stm).await?;
		// Run change feeds queries
		self.changefeeds(ctx, opt, txn, stm).await?;
		// Run event queries
		self.event(ctx, opt, txn, stm).await?;
		// Yield document
		self.pluck(ctx, opt, txn, stm).await
	}
}
