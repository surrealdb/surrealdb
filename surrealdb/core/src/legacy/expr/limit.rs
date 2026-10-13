use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::limit::Limit;
use crate::val::{Number, Value};

pub(crate) async fn limit_process(
	this: &Limit,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<u32> {
	match stk
		.run(|stk| crate::legacy::expr_compute(&this.0, stk, ctx, opt, doc))
		.await
		.catch_return()
	{
		// This is a valid limiting number
		Ok(Value::Number(Number::Int(v))) if v >= 0 => {
			if v > u32::MAX as i64 {
				Err(anyhow::Error::new(ExecError::InvalidLimit {
					value: v.to_string(),
				}))
			} else {
				Ok(v as u32)
			}
		}
		// An invalid value was specified
		Ok(v) => Err(anyhow::Error::new(ExecError::InvalidLimit {
			value: v.into_raw_string(),
		})),
		// A different error occurred
		Err(e) => Err(e),
	}
}
