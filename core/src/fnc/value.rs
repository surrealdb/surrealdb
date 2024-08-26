use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::value::Value;
use crate::sql::{Closure, Function};
use reblessive::tree::Stk;

pub async fn chain(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(value, worker): (Value, Closure),
) -> Result<Value, Error> {
	let fnc = Function::Anonymous(worker.into(), vec![value]);
	fnc.compute(stk, ctx, opt, doc).await
}

pub fn diff((val1, val2): (Value, Value)) -> Result<Value, Error> {
	Ok(val1.diff(&val2, Idiom::default()).into())
}

pub fn patch((mut val, diff): (Value, Value)) -> Result<Value, Error> {
	val.patch(diff)?;
	Ok(val)
}
