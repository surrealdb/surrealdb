use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;

impl<'a> Document<'a> {
	pub async fn check(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		exe: Option<&QueryExecutor>,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = stm.conds() {
			// Check if the expression is truthy
			if !cond
				.compute(ctx, opt, txn, exe, self.id.as_ref(), Some(&self.current))
				.await?
				.is_truthy()
			{
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
}
