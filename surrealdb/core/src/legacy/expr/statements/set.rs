use reblessive::tree::Stk;

use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::statements::set::SetStatement;
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// Compute the set statement, must be called with a valid a ctx that is
/// Some.
///
/// Will keep the ctx Some unless an error happens in which case the calling
/// function should return the error.
#[instrument(level = "trace", name = "SetStatement::compute", skip_all)]
pub(crate) async fn set_statement_compute(
	this: &SetStatement,
	stk: &mut Stk,
	ctx: &mut Option<FrozenContext>,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	assert!(ctx.is_some(), "SetStatement::compute must be called with a set option.");

	if this.is_protected_set() {
		return Err(ControlFlow::from(anyhow::Error::new(ExecError::InvalidParam {
			name: this.name.to_string(),
		})));
	}

	let result = stk
		.run(|stk| {
			crate::legacy::expr_compute(
				&this.what,
				stk,
				ctx.as_ref().expect("context should be initialized"),
				opt,
				doc,
			)
		})
		.await?;
	let result = match &this.kind {
		Some(kind) => result
			.coerce_to_kind(kind)
			.map_err(|e| ExecError::SetCoerce {
				name: this.name.to_string(),
				error: Box::new(e),
			})
			.map_err(anyhow::Error::new)?,
		None => result,
	};

	// SECURITY: a block threads one child context through every statement it runs,
	// so a LET that rebinds its own parameter (`{ LET $a = 1; LET $a = [$a]; ... }`)
	// accumulates nesting across statements without any storage write to catch it,
	// bypassing the top-level LET guard in the executor. Cap it here, where a block
	// LET binds, so a binding cannot grow deep enough to overflow the stack on a
	// later recursive clone/drop that reads the parameter back.
	crate::val::check_value_depth(&result)?;

	let mut c = Context::unfreeze(ctx.take().expect("context should be initialized"))?;
	c.add_value(this.name.clone(), result.into());
	*ctx = Some(c.freeze());
	Ok(Value::None)
}
