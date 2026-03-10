use anyhow::Result;

use crate::val::Value;

/// Returns a boolean that is false if the input is truthy and true otherwise.
pub fn not((val,): (Value,)) -> Result<Value> {
	Ok((!val.is_truthy()).into())
}
