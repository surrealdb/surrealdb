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
		// Reject writes to read-only view tables (after the permission gate)
		self.check_table_not_view(opt)?;
		// Check if the WHERE condition is truthy
		self.check_where_condition(stk, ctx, opt, stm.cond()).await?;
		// Clean up any outgoing references this record holds
		self.cleanup_table_references(stk, ctx, opt).await?;
		// Empty the record data
		self.clear_record_data();
		// Clear the document and index data. If a doc-ID index is mid-build it
		// enqueues this delete for later replay rather than dropping the record
		// now, so it still needs the shared `!di`/`!dd` mapping when the builder
		// replays; `store_index_data` reports that deferral.
		let doc_id_removal_deferred = self.store_index_data(stk, ctx, opt).await?;
		// Release the record's entry in the table's shared doc-ID space, once every
		// index has dropped the record. When a build deferred the removal, the
		// builder's replay owns it (see kvs::index::replay) — dropping the mapping
		// here would leave that replay unable to resolve the doc-ID — so a durable
		// `!dp` marker is written instead, in this same transaction, guaranteeing
		// the reclaim is completed even if the build never replays the delete.
		if doc_id_removal_deferred {
			self.defer_doc_id_removal(ctx).await?;
		} else {
			self.remove_doc_id(ctx).await?;
		}
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
