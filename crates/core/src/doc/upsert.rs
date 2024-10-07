use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Document {
	pub async fn upsert(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		match self.upsert_process(stk, ctx, opt, stm).await {
			// We attempted to INSERT a document with an ID,
			// and this ID already exists in the database,
			// so we need to UPDATE the record instead.
			Err(Error::RecordExists {
				thing,
			}) => Err(Error::RetryWithId(thing)),
			// If any other error was received, then let's
			// pass that error through and return an error
			Err(e) => Err(e),
			// Otherwise the record creation succeeded
			Ok(v) => Ok(v),
		}
	}
	/// Attempt to run an UPSERT clause
	async fn upsert_process(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		self.check_permissions_quick(stk, ctx, opt, stm).await?;
		self.check_table_type(ctx, opt, stm).await?;
		self.check_data_fields(stk, ctx, opt, stm).await?;
		self.check_where_condition(stk, ctx, opt, stm).await?;
		self.check_permissions_table(stk, ctx, opt, stm).await?;
		self.process_record_data(stk, ctx, opt, stm).await?;
		self.process_table_fields(stk, ctx, opt, stm).await?;
		self.cleanup_table_fields(stk, ctx, opt, stm).await?;
		self.default_record_data(ctx, opt, stm).await?;
		self.check_permissions_table(stk, ctx, opt, stm).await?;
		self.store_record_data(ctx, opt, stm).await?;
		self.store_index_data(stk, ctx, opt, stm).await?;
		self.process_table_views(stk, ctx, opt, stm).await?;
		self.process_table_lives(stk, ctx, opt, stm).await?;
		self.process_table_events(stk, ctx, opt, stm).await?;
		self.process_changefeeds(ctx, opt, stm).await?;
		self.pluck(stk, ctx, opt, stm).await
	}
}
