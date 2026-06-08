use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::FrozenContext;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::val::Value;

impl Document {
	pub(crate) async fn delete(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Check if the record actually exists
		self.check_record_exists()?;
		// SECURITY: evaluate the table-level update permission BEFORE any
		// user-supplied expression in the WHERE clause or data clause.
		// Otherwise a `WHERE THROW ...` could exfiltrate field values
		// before the permission check rejects the operation.
		self.check_delete_permissions(stk, ctx, opt, &self.current).await?;
		// Check if the WHERE condition is truthy
		self.check_where_condition(stk, ctx, opt, stm.cond()).await?;
		// Clean up any outgoing references this record holds
		self.cleanup_table_references(stk, ctx, opt).await?;
		// Empty the record data
		self.clear_record_data();
		// Clear the document and index data
		self.store_index_data(stk, ctx, opt).await?;
		self.purge_record_data(stk, ctx, opt).await?;
		self.process_table_views(stk, ctx, opt, super::Action::Delete).await?;
		self.process_table_events(stk, ctx, opt, super::Action::Delete).await?;
		self.process_table_lives(stk, ctx, opt, super::Action::Delete).await?;
		self.process_changefeeds(ctx, opt).await?;
		// Check table permissions for output
		self.check_select_permissions(stk, ctx, opt, &self.initial).await?;
		// Process the projected output document
		self.output_write(stk, ctx, opt, stm.output(), stm).await
	}
}
