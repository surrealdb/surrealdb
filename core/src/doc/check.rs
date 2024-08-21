use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::sql::Cond;
use reblessive::tree::Stk;

impl Document {
	pub async fn check(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		Self::check_cond(stk, ctx, opt, stm.conds(), &self.current).await
	}

	pub(crate) async fn check_cond(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		cond: Option<&Cond>,
		doc: &CursorDoc,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = cond {
			// Check if the expression is truthy
			if !cond.compute(stk, ctx, opt, Some(doc)).await?.is_truthy() {
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
}
