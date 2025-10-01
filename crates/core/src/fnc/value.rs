use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Operation;
use crate::val::{Closure, Value};

pub async fn chain(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(value, worker): (Value, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		worker.compute(stk, ctx, opt, doc, vec![value]).await
	} else {
		Ok(Value::None)
	}
}

pub async fn diff((val1, val2): (Value, Value)) -> Result<Value> {
	Ok(Operation::operations_to_value(val1.diff(&val2)))
}

pub async fn patch((mut val, diff): (Value, Value)) -> Result<Value> {
	val.patch(diff)?;
	Ok(val)
}
