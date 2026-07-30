use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::ParameterCapturePass;
use crate::expr::closure::ClosureExpr;
use crate::val::{Closure, Value};

#[instrument(level = "trace", name = "ClosureExpr::compute", skip_all)]
pub(crate) async fn closure_expr_compute(this: &ClosureExpr, ctx: &FrozenContext) -> Result<Value> {
	let captures = ParameterCapturePass::capture(ctx, &this.body);

	Ok(Value::Closure(Box::new(Closure::Expr {
		args: this.args.clone(),
		returns: this.returns.clone(),
		captures,
		body: this.body.clone(),
	})))
}
