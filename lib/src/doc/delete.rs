use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn delete(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check value type
		self.admit(ctx, opt, txn, stm).await?;
		// Check where clause
		self.check(ctx, opt, txn, stm).await?;
		// Erase document
		self.erase(ctx, opt, txn, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, txn, stm).await?;
		// Purge index data
		self.index(ctx, opt, txn, stm).await?;
		// Purge record data
		self.purge(ctx, opt, txn, stm).await?;
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
