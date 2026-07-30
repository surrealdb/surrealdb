use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::EngineError;
use crate::exec::Error as ExecError;
use crate::expr::statements::foreach::{ForeachIter, ForeachStatement};
use crate::expr::{ControlFlow, Error as ExprError, Expr, FlowResult};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "ForeachStatement::compute", skip_all)]
pub(crate) async fn foreach_statement_compute(
	this: &ForeachStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	// Check the loop data
	let data = stk.run(|stk| crate::legacy::expr_compute(&this.range, stk, ctx, opt, doc)).await?;
	let iter = match data {
		Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
		Value::Range(r) => {
			let r =
				r.coerce_to_typed::<i64>().map_err(ExprError::from).map_err(anyhow::Error::new)?;
			ForeachIter::Range(r.iter().map(Value::from))
		}

		v => {
			return Err(ControlFlow::from(anyhow::Error::new(ExecError::InvalidStatementTarget {
				value: v.to_sql(),
			})));
		}
	};

	// Loop over the values
	for v in iter {
		if let Some(d) = ctx.is_timedout().await? {
			return Err(ControlFlow::from(anyhow::Error::new(EngineError::QueryTimedout(d))));
		}
		// Duplicate context
		let ctx = Context::new_child(ctx).freeze();
		// Set the current parameter
		let key = this.param.as_str().to_owned();
		let mut ctx = Context::unfreeze(ctx)?;
		ctx.add_value(key, v.into());
		let mut ctx = Some(ctx.freeze());
		// Loop over the code block statements
		for v in this.block.iter() {
			// Compute each block entry
			let res = match v {
				Expr::Let(x) => {
					crate::legacy::set_statement_compute(x, stk, &mut ctx, opt, doc).await
				}
				v => {
					stk.run(|stk| {
						crate::legacy::expr_compute(
							v,
							stk,
							ctx.as_ref().expect("context should be initialized"),
							opt,
							doc,
						)
					})
					.await
				}
			};
			// Catch any special errors
			match res {
				Err(ControlFlow::Continue) => break,
				Err(ControlFlow::Break) => return Ok(Value::None),
				Err(err) => return Err(err),
				_ => (),
			};
		}
		// Cooperatively yield if the task has been running for too long.
		yield_now!();
	}
	// Ok all good
	Ok(Value::None)
}
