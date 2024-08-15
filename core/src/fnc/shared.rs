use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::{Closure, Function};

pub async fn chain(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(value, worker): (Value, Closure),
) -> Result<Value, Error> {
	let fnc = Function::Anonymous(worker.into(), vec![value]);
	fnc.compute(stk, ctx, opt, doc).await
}
