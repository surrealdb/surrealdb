use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
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
		// Check current record
		match self.current.is_some() {
			// Run INSERT clause
			false => {
				// Check if allowed
				self.allow(ctx, opt, txn, stm).await?;
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
				// Run event queries
				self.event(ctx, opt, txn, stm).await?;
				// Yield document
				self.pluck(ctx, opt, txn, stm).await
			}
			// Run UPDATE clause
			true => {
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
				// Run event queries
				self.event(ctx, opt, txn, stm).await?;
				// Yield document
				self.pluck(ctx, opt, txn, stm).await
			}
		}
	}
}
