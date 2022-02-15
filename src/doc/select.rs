use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn select(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction<'_>,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if record exists
		self.empty(ctx, opt, txn, stm).await?;
		// Check where clause
		self.check(ctx, opt, txn, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, txn, stm).await?;
		// Yield document
		self.pluck(ctx, opt, txn, stm).await
	}
}
