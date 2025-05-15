use super::args::Optional;
use crate::err::Error;
use crate::expr::value::Value;

pub fn count((Optional(arg),): (Optional<Value>,)) -> Result<Value, Error> {
	Ok(arg
		.map(|val| match val {
			Value::Array(v) => v.iter().filter(|v| v.is_truthy()).count().into(),
			v => (v.is_truthy() as i64).into(),
		})
		.unwrap_or_else(|| 1.into()))
}
