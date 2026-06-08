use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::Document;
use crate::expr::{Idiom, SelectStatement};
use crate::val::Value;

impl Document {
	pub(crate) async fn select(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &SelectStatement,
		omit: &[Idiom],
	) -> Result<Value, IgnoreError> {
		// Check if the record actually exists
		self.check_record_exists()?;
		// SECURITY: evaluate the table-level select permission BEFORE the
		// WHERE clause so a `WHERE THROW ...` cannot leak record values.
		self.check_select_permissions(stk, ctx, opt, &self.current).await?;
		// Check if the WHERE condition is truthy
		self.check_where_condition(stk, ctx, opt, stm.cond.as_ref()).await?;
		// Process the projected output document
		self.output_select(stk, ctx, opt, stm, omit).await
	}
}
