use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl Document {
	pub async fn select(
		&self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Check if record exists
		self.empty(ctx, opt, exe, stm).await?;
		// Check where clause
		self.check(ctx, opt, exe, stm).await?;
		// Check if allowed
		self.allow(ctx, opt, exe, stm).await?;
		// Yield document
		self.pluck(ctx, opt, exe, stm).await
	}
}
