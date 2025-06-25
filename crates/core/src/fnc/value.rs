use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::idiom::Idiom;
use crate::val::{Closure, Value};
use anyhow::Result;
use reblessive::tree::Stk;

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

pub async fn diff(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(val1, val2): (Value, Value),
) -> Result<Value> {
	if let Some(opt) = opt {
		Ok(val1.diff(&val2, Idiom::default()).into())
	} else {
		Ok(Value::None)
	}
}

pub async fn patch(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(mut val, diff): (Value, Value),
) -> Result<Value> {
	if let Some(opt) = opt {
		val.patch(diff)?;
		Ok(val)
	} else {
		Ok(Value::None)
	}
}
