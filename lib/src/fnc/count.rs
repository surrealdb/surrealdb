use crate::err::Error;
use crate::sql::value::Value;

pub fn count((arg,): (Option<Value>,)) -> Result<Value, Error> {
	Ok(arg
		.map(|val| match val {
			Value::Array(v) => v.iter().filter(|v| v.is_truthy()).count().into(),
			v => (v.is_truthy() as i64).into(),
		})
		.unwrap_or_else(|| 1.into()))
}
