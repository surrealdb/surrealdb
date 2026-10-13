use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::EngineError;
use crate::expr::statements::ifelse::IfelseStatement;
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "IfelseStatement::compute", skip_all)]
pub(crate) async fn ifelse_statement_compute(
	this: &IfelseStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	for (cond, then) in &this.exprs {
		if let Some(d) = ctx.is_timedout().await? {
			return Err(ControlFlow::from(anyhow::Error::new(EngineError::QueryTimedout(d))));
		}
		let v = stk.run(|stk| crate::legacy::expr_compute(cond, stk, ctx, opt, doc)).await?;
		if v.is_truthy() {
			return stk.run(|stk| crate::legacy::expr_compute(then, stk, ctx, opt, doc)).await;
		}
	}
	match this.close {
		Some(ref v) => stk.run(|stk| crate::legacy::expr_compute(v, stk, ctx, opt, doc)).await,
		None => Ok(Value::None),
	}
}
