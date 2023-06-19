use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn select(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if record exists
		self.empty(ctx, opt, stm).await?;
		// Check where clause
		self.check(ctx, opt, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, stm).await?;
		// Yield document
		self.pluck(ctx, opt, stm).await
	}
}
