use crate::err::Error;
use crate::sql::value::Value;

pub fn id((arg,): (Value,)) -> Result<Value, Error> {
	Ok(match arg {
		Value::Thing(v) => v.id.into(),
		_ => Value::None,
	})
}

pub fn tb((arg,): (Value,)) -> Result<Value, Error> {
	Ok(match arg {
		Value::Thing(v) => v.tb.into(),
		_ => Value::None,
	})
}
