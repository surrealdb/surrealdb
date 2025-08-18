use anyhow::anyhow;
use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::Context;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::err::Error;
use crate::val::Value;

impl Document {
	pub(super) async fn upsert(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Even though we haven't tried to create first this can still not be the
		// 'initial iteration' if the initial doc is not set.
		//
		// If this is not the initial iteration we immediatly skip trying to create and
		// go straight to updating.
		if !self.is_iteration_initial() {
			return self.upsert_update(stk, ctx, opt, stm).await;
		}

		ctx.tx().lock().await.new_save_point();

		// First try to create the value and if that is not possible due to an existing
		// value fall back to update instead.
		//
		// This is done this way to make the create path fast and take priority over the
		// update path.
		let retry = match self.upsert_create(stk, ctx, opt, stm).await {
			Err(IgnoreError::Error(e)) => match e.downcast() {
				// We received an index exists error, so we
				// ignore the error, and attempt to update the
				// record using the ON DUPLICATE KEY UPDATE
				// clause with the ID received in the error
				Ok(Error::IndexExists {
					thing,
					..
				}) if !self.is_specific_record_id() => thing,
				// We attempted to INSERT a document with an ID,
				// and this ID already exists in the database,
				// so we need to UPDATE the record instead.
				Ok(Error::RecordExists {
					thing,
				}) => thing,

				// If an error was received, but this statement
				// is potentially retryable because it might
				// depend on any initially stored value, then we
				// need to retry and update the document. If this
				// error was because of a schema issue then we
				// need to presume that we might need to retry
				// after fetching the initial record value
				// from storage before processing schema again.
				Ok(e) => {
					if e.is_schema_related() && stm.is_repeatable() {
						self.inner_id()?
					} else {
						return Err(IgnoreError::Error(anyhow!(e)));
					}
				}
				Err(e) => {
					ctx.tx().lock().await.rollback_to_save_point().await?;
					return Err(IgnoreError::Error(e));
				}
			},
			Err(IgnoreError::Ignore) => {
				ctx.tx().lock().await.release_last_save_point()?;
				return Err(IgnoreError::Ignore);
			}
			Ok(x) => {
				ctx.tx().lock().await.release_last_save_point()?;
				return Ok(x);
			}
		};

		// Create failed so now fall back to running an update.

		ctx.tx().lock().await.rollback_to_save_point().await?;

		if ctx.is_done(true).await? {
			return Err(IgnoreError::Ignore);
		}

		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let val = ctx.tx().get_record(ns, db, &retry.table, &retry.key, opt.version).await?;

		self.modify_for_update_retry(retry, val);

		// Skip generate_record_id in retry mode since the ID is already set correctly
		if !self.retry {
			self.generate_record_id(stk, ctx, opt, stm).await?;
		}

		self.upsert_update(stk, ctx, opt, stm).await
	}
	/// Attempt to run an UPSERT statement to
	/// create a record which does not exist
	async fn upsert_create(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
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
	) -> Result<Value, IgnoreError> {
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
