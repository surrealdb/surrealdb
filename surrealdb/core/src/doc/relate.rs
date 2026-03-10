use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::FrozenContext;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::val::Value;

impl Document {
	pub(super) async fn relate(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Process the record data
		self.process_record_data(stk, ctx, opt, stm).await?;
		// Generate a record id
		self.generate_record_id()?;
		// Set default field values
		self.default_record_data(ctx, opt, stm).await?;
		// Check if table has correct relation status
		self.check_table_type(stm).await?;
		// Check whether current record exists
		if self.current.doc.as_ref().is_nullish() {
			// If the current document is null, it doesn't exist yet so we need to create a
			// new relation.
			self.relate_create(stk, ctx, opt, stm).await
		} else {
			// If the doc is some the relation does exist and we should update instead.
			self.relate_update(stk, ctx, opt, stm).await
		}
	}
	/// Attempt to run a RELATE clause
	async fn relate_create(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		self.check_permissions_quick(opt, stm).await?;
		self.check_table_type(stm).await?;
		self.check_data_fields(stk, ctx, opt, stm).await?;
		self.store_edges_data(ctx, opt, stm).await?;
		self.default_record_data(ctx, opt, stm).await?;
		self.process_table_fields(stk, ctx, opt, stm).await?;
		self.cleanup_table_fields(ctx, opt, stm).await?;
		self.check_permissions_table(stk, ctx, opt, stm).await?;
		self.store_record_data(ctx, opt, stm).await?;
		self.store_index_data(stk, ctx, opt).await?;
		self.process_table_views(stk, ctx, opt, stm).await?;
		self.process_table_lives(stk, ctx, opt, stm).await?;
		self.process_changefeeds(ctx, opt, stm).await?;
		self.process_table_events(stk, ctx, opt, stm).await?;
		self.pluck_generic(stk, ctx, opt, stm).await
	}
	/// Attempt to run an UPDATE clause
	async fn relate_update(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		self.check_permissions_quick(opt, stm).await?;
		self.check_table_type(stm).await?;
		self.check_data_fields(stk, ctx, opt, stm).await?;
		self.check_permissions_table(stk, ctx, opt, stm).await?;
		self.store_edges_data(ctx, opt, stm).await?;
		self.default_record_data(ctx, opt, stm).await?;
		self.process_table_fields(stk, ctx, opt, stm).await?;
		self.cleanup_table_fields(ctx, opt, stm).await?;
		self.check_permissions_table(stk, ctx, opt, stm).await?;
		self.store_record_data(ctx, opt, stm).await?;
		self.store_index_data(stk, ctx, opt).await?;
		self.process_table_views(stk, ctx, opt, stm).await?;
		self.process_table_lives(stk, ctx, opt, stm).await?;
		self.process_table_events(stk, ctx, opt, stm).await?;
		self.process_changefeeds(ctx, opt, stm).await?;
		self.pluck_generic(stk, ctx, opt, stm).await
	}
}
