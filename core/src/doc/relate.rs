use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn relate(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if table has correct relation status
		self.relation(ctx, opt, txn, stm).await?;
		// Check current record
		match self.current.doc.is_some() {
			// Create new edge
			false => {
				// Store record edges
				self.edges(ctx, opt, txn, stm).await?;
				// Alter record data
				self.alter(stk, ctx, opt, txn, stm).await?;
				// Merge fields data
				self.field(stk, ctx, opt, txn, stm).await?;
				// Reset fields data
				self.reset(ctx, opt, txn, stm).await?;
				// Clean fields data
				self.clean(stk, ctx, opt, txn, stm).await?;
				// Check if allowed
				self.allow(stk, ctx, opt, txn, stm).await?;
				// Store record data
				self.store(ctx, opt, txn, stm).await?;
				// Store index data
				self.index(stk, ctx, opt, txn, stm).await?;
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
			// Update old edge
			true => {
				// Check if allowed
				self.allow(stk, ctx, opt, txn, stm).await?;
				// Store record edges
				self.edges(ctx, opt, txn, stm).await?;
				// Alter record data
				self.alter(stk, ctx, opt, txn, stm).await?;
				// Merge fields data
				self.field(stk, ctx, opt, txn, stm).await?;
				// Reset fields data
				self.reset(ctx, opt, txn, stm).await?;
				// Clean fields data
				self.clean(stk, ctx, opt, txn, stm).await?;
				// Check if allowed
				self.allow(stk, ctx, opt, txn, stm).await?;
				// Store record data
				self.store(ctx, opt, txn, stm).await?;
				// Store index data
				self.index(stk, ctx, opt, txn, stm).await?;
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
	}
}
