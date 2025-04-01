use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::statements::InsertStatement;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Document {
	pub(super) async fn insert(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &InsertStatement,
	) -> Result<Value, Error> {
		// Even though we haven't tried to create first this can still not be the 'initial iteration' if
		// the initial doc is not set.
		//
		// If this is not the initial iteration we immediatly skip trying to create and go straight
		// to updating.
		if !self.is_iteration_initial() {
			return self.insert_update(stk, ctx, opt, &Statement::Insert(stm)).await;
		}

		// is this retryable?
		// it is retryable when some data is present on the insert statement to update.
		let retryable = stm.update.is_some();
		if retryable {
			// it is retryable so generate a save point we can roll back to.
			ctx.tx().lock().await.new_save_point().await;
		}

		// First try to create the value and if that is not possible due to an existing value fall
		// back to update instead.
		//
		// This is done this way to make the create path fast and take priority over the update
		// path.
		let retry = match self.insert_create(stk, ctx, opt, &Statement::Insert(stm)).await {
			// We received an index exists error, so we
			// ignore the error, and attempt to update the
			// record using the ON DUPLICATE KEY UPDATE
			// clause with the ID received in the error
			Err(Error::IndexExists {
				thing,
				index,
				value,
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
						return Err(Error::Ignore);
					}

					return Err(Error::IndexExists {
						thing,
						index,
						value,
					});
				}
				thing
			}
			// We attempted to INSERT a document with an ID,
			// and this ID already exists in the database,
			// so we need to update the record instead using
			// the ON DUPLICATE KEY UPDATE statement clause
			Err(Error::RecordExists {
				thing,
			}) => {
				// if not retryable return the error.
				if !retryable {
					// Ignore flag; disables error.
					// Error::Ignore is never raised to the user.
					if stm.ignore {
						return Err(Error::Ignore);
					}
					return Err(Error::RecordExists {
						thing,
					});
				}
				thing
			}
			Err(Error::Ignore) => {
				if retryable {
					ctx.tx().lock().await.release_last_save_point().await?;
				}
				return Err(Error::Ignore);
			}
			Err(e) => {
				// if retryable we need to do something with the savepoint.
				if retryable {
					ctx.tx().lock().await.rollback_to_save_point().await?;
				}
				return Err(e);
			}
			Ok(x) => {
				if retryable {
					ctx.tx().lock().await.release_last_save_point().await?;
				}
				return Ok(x);
			}
		};

		// Insertion failed so instead do an update.
		ctx.tx().lock().await.rollback_to_save_point().await?;

		if ctx.is_done(true)? {
			// Don't process the document
			return Err(Error::Ignore);
		}

		let (ns, db) = opt.ns_db()?;
		let val = ctx.tx().get_record(ns, db, &retry.tb, &retry.id, opt.version).await?;

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
	) -> Result<Value, Error> {
		self.check_permissions_quick(stk, ctx, opt, stm).await?;
		self.check_table_type(ctx, opt, stm).await?;
		self.check_data_fields(stk, ctx, opt, stm).await?;
		self.process_merge_data(stk, ctx, opt, stm).await?;
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
	) -> Result<Value, Error> {
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
