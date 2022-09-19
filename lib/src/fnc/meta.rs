use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

pub fn id(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(match args.remove(0) {
		Value::Thing(v) => v.id.into(),
		_ => Value::None,
	})
}

pub fn tb(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(match args.remove(0) {
		Value::Thing(v) => v.tb.into(),
		_ => Value::None,
	})
}
