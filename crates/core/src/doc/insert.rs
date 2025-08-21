use anyhow::Result;
use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::Context;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::err;
use crate::err::Error;
use crate::expr::statements::InsertStatement;
use crate::val::Value;

impl Document {
	pub(super) async fn insert(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &InsertStatement,
	) -> Result<Value, IgnoreError> {
		// Even though we haven't tried to create first this can still not be the
		// 'initial iteration' if the initial doc is not set.
		//
		// If this is not the initial iteration we immediatly skip trying to create and
		// go straight to updating.
		if !self.is_iteration_initial() {
			return self.insert_update(stk, ctx, opt, &Statement::Insert(stm)).await;
		}

		// is this retryable?
		// it is retryable when some data is present on the insert statement to update.
		let retryable = stm.update.is_some();
		if retryable {
			// it is retryable so generate a save point we can roll back to.
			ctx.tx().lock().await.new_save_point();
		}

		// First try to create the value and if that is not possible due to an existing
		// value fall back to update instead.
		//
		// This is done this way to make the create path fast and take priority over the
		// update path.
		let retry = match self.insert_create(stk, ctx, opt, &Statement::Insert(stm)).await {
			// We received an index exists error, so we
			// ignore the error, and attempt to update the
			// record using the ON DUPLICATE KEY UPDATE
			// clause with the ID received in the error
			Err(IgnoreError::Error(e)) => match e.downcast_ref::<err::Error>() {
				Some(Error::IndexExists {
					..
				}) => {
					// if not retryable return the error.
					//
					// or if the statement contained a specific record id, we
					// don't retry to
					if !retryable || self.is_specific_record_id() {
						if retryable {
							ctx.tx().lock().await.rollback_to_save_point().await?;
						}

						// Ignore flag; disables error.
						// Error::Ignore is never raised to the user.
						if stm.ignore {
							return Err(IgnoreError::Ignore);
						}

						return Err(IgnoreError::Error(e));
					}
					let Ok(Error::IndexExists {
						thing,
						..
					}) = e.downcast()
					else {
						// Checked above
						unreachable!()
					};
					thing
				}
				// We attempted to INSERT a document with an ID,
				// and this ID already exists in the database,
				// so we need to update the record instead using
				// the ON DUPLICATE KEY UPDATE statement clause
				Some(Error::RecordExists {
					..
				}) => {
					// if not retryable return the error.
					if !retryable {
						// Ignore flag; disables error.
						// Error::Ignore is never raised to the user.
						if stm.ignore {
							return Err(IgnoreError::Ignore);
						}
						return Err(IgnoreError::Error(e));
					}
					let Ok(Error::RecordExists {
						thing,
						..
					}) = e.downcast()
					else {
						// Checked above
						unreachable!()
					};
					thing
				}
				_ => {
					// if retryable we need to do something with the savepoint.
					if retryable {
						ctx.tx().lock().await.rollback_to_save_point().await?;
					}
					return Err(IgnoreError::Error(e));
				}
			},
			Err(IgnoreError::Ignore) => {
				if retryable {
					ctx.tx().lock().await.release_last_save_point()?;
				}
				return Err(IgnoreError::Ignore);
			}
			Ok(x) => {
				if retryable {
					ctx.tx().lock().await.release_last_save_point()?;
				}
				return Ok(x);
			}
		};

		// Insertion failed so instead do an update.
		ctx.tx().lock().await.rollback_to_save_point().await?;

		if ctx.is_done(true).await? {
			// Don't process the document
			return Err(IgnoreError::Ignore);
		}

		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let val = ctx.tx().get_record(ns, db, &retry.table, &retry.key, opt.version).await?;

		self.modify_for_update_retry(retry, val);

		// we restarted, so we might need to generate a record id again?
		self.generate_record_id(stk, ctx, opt, &Statement::Insert(stm)).await?;

		self.insert_update(stk, ctx, opt, &Statement::Insert(stm)).await
	}

	/// Attempt to run an INSERT statement to
	/// create a record which does not exist
	async fn insert_create(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		self.check_permissions_quick(stk, ctx, opt, stm).await?;
		self.check_table_type(ctx, opt, stm).await?;
		self.check_data_fields(stk, ctx, opt, stm).await?;
		self.process_merge_data().await?;
		self.store_edges_data(ctx, opt, stm).await?;
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
	/// Attempt to run an INSERT statement to
	/// update a record which already exists
	async fn insert_update(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		self.check_permissions_quick(stk, ctx, opt, stm).await?;
		self.check_table_type(ctx, opt, stm).await?;
		self.check_data_fields(stk, ctx, opt, stm).await?;
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
