use anyhow::Result;

use super::args::Optional;
use crate::val::Value;

pub fn storage((Optional(arg),): (Optional<Value>,)) -> Result<Value> {
	Ok(arg
		.map(|val| match val {
			Value::Array(v) => {
				let total: i64 = v
					.iter()
					.map(|v| match v {
						Value::Number(n) => n.clone().as_int(),
						_ => 0,
					})
					.sum();
				Value::from(total)
			}
			v => {
				let bytes = revision::to_vec(&v).unwrap_or_default();
				Value::from(bytes.len() as i64)
			}
		})
		.unwrap_or_else(|| Value::from(0i64)))
}
