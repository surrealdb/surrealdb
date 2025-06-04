use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::expr::value::Value;
use reblessive::tree::Stk;

use super::IgnoreError;

impl Document {
	pub(super) async fn select(
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
		self.pluck(stk, ctx, opt, stm).await
	}
}
