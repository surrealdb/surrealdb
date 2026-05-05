use anyhow::Result;

use super::args::Optional;
use crate::val::Value;

pub fn storage((Optional(arg),): (Optional<Value>,)) -> Result<Value> {
	Ok(arg
		.map(|val| {
			let bytes = revision::to_vec(&val).unwrap_or_default();
			Value::from(bytes.len() as i64)
		})
		.unwrap_or_else(|| Value::from(0i64)))
}
