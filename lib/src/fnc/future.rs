use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

pub fn run(_: &Context, expr: Value) -> Result<Value, Error> {
	Ok(expr)
}
