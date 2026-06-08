use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::FrozenContext;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::val::Value;

impl Document {
	pub(crate) async fn create(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Ensure we can write to the table at all
		self.check_permissions_quick_create(ctx, opt)?;
		// Ensure any input data is computed
		self.compute_input_data(stk, ctx, opt, stm).await?;
		// Set the specified record content
		self.process_record_data(stk, ctx, opt).await?;
		// Generate a new record id if necessary
		self.generate_record_id()?;
		// Ensure we can store this type of record
		self.check_table_type_create()?;
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
}
