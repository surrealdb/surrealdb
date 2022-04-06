use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn check(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = stm.conds() {
			if cond.expr.compute(ctx, opt, txn, Some(&self.current)).await?.is_truthy() {
			// Check if the expression is truthy
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
}
