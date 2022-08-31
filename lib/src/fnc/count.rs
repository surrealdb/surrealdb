use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

pub fn count(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 => match args.remove(0) {
			Value::Array(v) => Ok(v.iter().filter(|v| v.is_truthy()).count().into()),
			v => Ok((v.is_truthy() as i64).into()),
		},
		0 => Ok(1.into()),
		_ => unreachable!(),
	}
}
