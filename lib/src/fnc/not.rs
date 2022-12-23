use crate::sql::Value;
use crate::Error;

/// Returns a boolean that is false if the input is truthy and true otherwise.
pub fn not((val,): (Value,)) -> Result<Value, Error> {
	Ok((!val.is_truthy()).into())
}
