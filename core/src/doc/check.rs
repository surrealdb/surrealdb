use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::sql::Cond;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn check(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		Self::check_cond(stk, ctx, opt, txn, stm.conds(), &self.current).await
	}

	pub(crate) async fn check_cond(
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		cond: Option<&Cond>,
		doc: &CursorDoc<'_>,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = cond {
			// Check if the expression is truthy
			if !cond.compute(stk, ctx, opt, txn, Some(doc)).await?.is_truthy() {
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
}
