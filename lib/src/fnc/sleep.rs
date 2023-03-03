use crate::err::Error;
use crate::sql::Value;
use std::thread;

/// Sleep during the provided duration parameter.
pub fn sleep((val,): (Value,)) -> Result<Value, Error> {
	thread::sleep(val.as_duration().into());
	Ok(Value::None)
}
