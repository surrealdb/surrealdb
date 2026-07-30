use reblessive::tree::Stk;

use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::block::Block;
use crate::expr::{Expr, FlowResult};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "Block::compute", skip_all)]
pub(crate) async fn block_compute(
	this: &Block,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	// Duplicate context
	let mut ctx = Some(Context::new_child(ctx).freeze());
	// Loop over the statements
	let mut res = Value::None;
	for v in this.iter() {
		match v {
			Expr::Let(x) => {
				res = crate::legacy::set_statement_compute(x, stk, &mut ctx, opt, doc).await?
			}
			v => {
				res = stk
					.run(|stk| {
						crate::legacy::expr_compute(
							v,
							stk,
							ctx.as_ref().expect("context should be initialized"),
							opt,
							doc,
						)
					})
					.await?
			}
		}
	}
	// Return nothing
	Ok(res)
}
