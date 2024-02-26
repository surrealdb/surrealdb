use crate::err::Error;
use crate::sql::Value;

/// Returns a boolean that is false if the input is truthy and true otherwise.
pub fn not((val,): (Value,)) -> Result<Value, Error> {
	Ok((!val.is_truthy()).into())
}
