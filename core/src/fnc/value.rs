use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::value::Value;
use crate::sql::{Closure, Function};
use reblessive::tree::Stk;

pub async fn chain(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(value, worker): (Value, Closure),
) -> Result<Value, Error> {
	if let Some(opt) = opt {
		let fnc = Function::Anonymous(worker.into(), vec![value]);
		fnc.compute(stk, ctx, opt, doc).await
	} else {
		Ok(Value::None)
	}
}

pub async fn diff(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(val1, val2): (Value, Value),
) -> Result<Value, Error> {
	if let Some(opt) = opt {
		let val1 = val1.compute(stk, ctx, opt, doc).await?;
		let val2 = val2.compute(stk, ctx, opt, doc).await?;
		Ok(val1.diff(&val2, Idiom::default()).into())
	} else {
		Ok(Value::None)
	}
}

pub async fn patch(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(val, diff): (Value, Value),
) -> Result<Value, Error> {
	if let Some(opt) = opt {
		let mut val = val.compute(stk, ctx, opt, doc).await?;
		val.patch(diff)?;
		Ok(val)
	} else {
		Ok(Value::None)
	}
}
