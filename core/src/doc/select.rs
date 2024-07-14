use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn select(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if record exists
		self.empty(ctx, opt, stm).await?;
		// Check where clause
		self.check(stk, ctx, opt, stm).await?;
		// Check if allowed
		self.allow(stk, ctx, opt, stm).await?;
		self.current.doc.compute(stk, ctx, opt, None).await
	}
}
