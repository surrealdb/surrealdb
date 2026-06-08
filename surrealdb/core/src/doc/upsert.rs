use anyhow::anyhow;
use reblessive::tree::Stk;

use super::IgnoreError;
use crate::catalog::providers::TableProvider;
use crate::ctx::FrozenContext;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::err::Error;
use crate::val::Value;

impl Document {
	pub(crate) async fn upsert(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Error for tracking initial failures
		let mut error: Option<anyhow::Error> = None;
		// Skip the create attempt when we already have the document
		if !self.is_iteration_initial() {
			return self.upsert_update(stk, ctx, opt, stm).await;
		}
		// Save point so a failed create attempt can be rolled back
		ctx.tx().new_save_point().await?;
		// Try create first; on a recoverable conflict, fall through to update
		let retry = match self.upsert_create(stk, ctx, opt, stm).await {
			// Record created successfully
			Ok(x) => {
				ctx.tx().release_last_save_point().await?;
				return Ok(x);
			}
			// We should ignore this record
			Err(IgnoreError::Ignore) => {
				ctx.tx().release_last_save_point().await?;
				return Err(IgnoreError::Ignore);
			}
			// There was an error creating the record
			Err(IgnoreError::Error(e)) => match e.downcast() {
				// We got an index exists error
				Ok(Error::IndexExists {
					record,
					..
				}) if !self.is_specific_record_id() => record,
				// This record already exists
				Ok(Error::RecordExists {
					record,
				}) => record,
				// There was a possible schema error
				Ok(e) if e.is_schema_related() && stm.is_repeatable() => {
					error = Some(e.into());
					self.inner_id()?
				}
				// There was a conflict error
				Ok(e) => {
					ctx.tx().rollback_to_save_point().await?;
					self.mutated = false;
					return Err(IgnoreError::Error(anyhow!(e)));
				}
				// Unrelated error — always surface
				Err(e) => {
					ctx.tx().rollback_to_save_point().await?;
					self.mutated = false;
					return Err(IgnoreError::Error(e));
				}
			},
		};
		// Roll back the create attempt before falling through to update
		ctx.tx().rollback_to_save_point().await?;
		// Reset any mutation tracking, for retry
		self.mutated = false;
		// Check if the request is finished
		if ctx.is_done(None).await? {
			return Err(IgnoreError::Ignore);
		}
		// Get the namespace id
		let ns = self.doc_ctx.ns().namespace_id;
		// Get the database id
		let db = self.doc_ctx.db().database_id;
		// Get the already stored record
		let val = ctx.tx().get_record(ns, db, &retry.table, &retry.key, opt.version).await?;
		// Reset the document for the retry
		self.modify_for_update_retry(retry, val);
		// Update the document with `UPDATE`
		let res = self.upsert_update(stk, ctx, opt, stm).await;
		// Return any carried over error if set
		match error {
			Some(e) => match res {
				Err(_) => Err(IgnoreError::Error(e)),
				Ok(v) => Ok(v),
			},
			None => res,
		}
	}

	/// Attempt to run an UPSERT statement to
	/// create a record which does not exist
	async fn upsert_create(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Ensure we can store this type of record
		self.check_table_type_upsert()?;
		// Ensure we can write to the table at all
		self.check_permissions_quick_create(ctx, opt)?;
		// Ensure any input data is computed
		self.compute_input_data(stk, ctx, opt, stm).await?;
		// Set the specified record content
		self.process_record_data(stk, ctx, opt).await?;
		// Generate a new record id if necessary
		self.generate_record_id()?;
		// Ensure all special fields are valid
		self.check_data_fields()?;
		// Set the default record field values
		self.default_record_data()?;
		// Process the field schema for the table
		self.process_table_fields(stk, ctx, opt, stm).await?;
		// Clean up table fields and NONE values
		self.cleanup_table_fields()?;
		// Check table permissions after create
		self.check_create_permissions(stk, ctx, opt, &self.current).await?;
		// Store the document and index data
		self.store_record_data(ctx, stm).await?;
		self.store_index_data(stk, ctx, opt).await?;
		// Process additional table operations
		self.process_table_references(stk, ctx, opt).await?;
		self.process_table_views(stk, ctx, opt, super::Action::Create).await?;
		self.process_table_events(stk, ctx, opt, super::Action::Create).await?;
		self.process_table_lives(stk, ctx, opt, super::Action::Create).await?;
		self.process_changefeeds(ctx, opt).await?;
		// Check table permissions for output
		self.check_select_permissions(stk, ctx, opt, &self.current).await?;
		// Process the projected output document
		self.output_write(stk, ctx, opt, stm.output(), stm).await
	}

	/// Attempt to run an UPSERT statement to
	/// update a record which already exists
	async fn upsert_update(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Ensure the record actually exists
		self.check_record_exists()?;
		// Ensure we can store this type of record
		self.check_table_type_upsert()?;
		// SECURITY: evaluate the table-level update permission BEFORE any
		// user-supplied expression in the WHERE clause or data clause.
		// Otherwise a `WHERE THROW ...` / `SET x = THROW ...` could exfiltrate
		// field values before the permission check rejects the operation.
		self.check_update_permissions(stk, ctx, opt, &self.current).await?;
		// Ensure any input data is computed
		self.compute_input_data(stk, ctx, opt, stm).await?;
		// Ensure all special fields are valid
		self.check_data_fields()?;
		// Check if the WHERE condition is truthy
		self.check_where_condition(stk, ctx, opt, stm.cond()).await?;
		// Set the specified record content
		self.process_record_data(stk, ctx, opt).await?;
		// Set the default record field values
		self.default_record_data()?;
		// Process the field schema for the table
		self.process_table_fields(stk, ctx, opt, stm).await?;
		// Clean up table fields and NONE values
		self.cleanup_table_fields()?;
		// Check table permissions after update
		self.recheck_update_permissions(stk, ctx, opt, &self.current).await?;
		// Store the document and index data
		self.store_record_data(ctx, stm).await?;
		self.store_index_data(stk, ctx, opt).await?;
		// Process additional table operations
		self.process_table_references(stk, ctx, opt).await?;
		self.process_table_views(stk, ctx, opt, super::Action::Update).await?;
		self.process_table_events(stk, ctx, opt, super::Action::Update).await?;
		self.process_table_lives(stk, ctx, opt, super::Action::Update).await?;
		self.process_changefeeds(ctx, opt).await?;
		// Check table permissions for output
		self.check_select_permissions(stk, ctx, opt, &self.current).await?;
		// Process the projected output document
		self.output_write(stk, ctx, opt, stm.output(), stm).await
	}
}
