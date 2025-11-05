use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::Context;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::val::Value;

impl Document {
	pub(super) async fn delete(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		self.check_record_exists(ctx, opt, stm).await?;
		self.check_permissions_quick(stk, ctx, opt, stm).await?;
		self.check_where_condition(stk, ctx, opt, stm).await?;
		self.check_permissions_table(stk, ctx, opt, stm).await?;
		self.cleanup_table_references(stk, ctx, opt).await?;
		self.clear_record_data();
		self.store_index_data(stk, ctx, opt, stm).await?;
		self.purge(stk, ctx, opt, stm).await?;
		self.process_table_views(stk, ctx, opt, stm).await?;
		self.process_table_lives(stk, ctx, opt, stm).await?;
		self.process_table_events(stk, ctx, opt, stm).await?;
		self.process_changefeeds(ctx, opt, stm).await?;
		self.pluck(stk, ctx, opt, stm).await
	}
}
