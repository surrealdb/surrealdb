use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Document {
	pub(super) async fn upsert(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// On the first iteration, we do not first attempt
		// to fetch the record from the storage engine. After
		// trying to create the record, if the record already
		// exists then we will fetch the record from storage,
		// and will update the record subsequently
		match self.is_iteration_initial() {
			// We haven't yet checked if the record exists
			// so let's assume that the record does not exist
			// and attempt to create the record in the database
			true => match self.upsert_create(stk, ctx, opt, stm).await {
				// We received an index exists error, so we
				// ignore the error, and attempt to update the
				// record using the ON DUPLICATE KEY UPDATE
				// clause with the ID received in the error
				Err(Error::IndexExists {
					thing,
					index,
					value,
				}) => match self.is_specific_record_id() {
					// No specific Record ID has been specified, so retry
					false => Err(Error::RetryWithId(thing)),
					// A specific Record ID was specified, so error
					true => Err(Error::IndexExists {
						thing,
						index,
						value,
					}),
				},
				// We attempted to INSERT a document with an ID,
				// and this ID already exists in the database,
				// so we need to UPDATE the record instead.
				Err(Error::RecordExists {
					thing,
				}) => Err(Error::RetryWithId(thing)),
				// If an error was received, but this statement
				// is potentially retryable because it might
				// depend on any initially stored value, then we
				// need to retry and update the document. If this
				// error was because of a schema issue then we
				// need to presume that we might need to retry
				// after fetching the initial record value
				// from storage before processing schema again.
				Err(e) if e.is_schema_related() && stm.is_repeatable() => {
					Err(Error::RetryWithId(self.inner_id()?))
				}
				// If any other error was received, then let's
				// pass that error through and return an error
				Err(e) => Err(e),
				// Otherwise the record creation succeeded
				Ok(v) => Ok(v),
			},
			// If we first attempted to create the record,
			// but the record existed already, then we will
			// fetch the record from the storage engine,
			// and will update the record subsequently
			false => self.upsert_update(stk, ctx, opt, stm).await,
		}
	}
	/// Attempt to run an UPSERT statement to
	/// create a record which does not exist
	async fn upsert_create(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		self.check_permissions_quick(stk, ctx, opt, stm).await?;
		self.check_table_type(ctx, opt, stm).await?;
		self.check_data_fields(stk, ctx, opt, stm).await?;
		self.process_record_data(stk, ctx, opt, stm).await?;
		self.process_table_fields(stk, ctx, opt, stm).await?;
		self.cleanup_table_fields(ctx, opt, stm).await?;
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
	/// Attempt to run an UPSERT statement to
	/// update a record which already exists
	async fn upsert_update(
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
		self.cleanup_table_fields(ctx, opt, stm).await?;
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
