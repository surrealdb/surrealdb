use anyhow::Result;
use surrealdb_expr::val::Value;

use super::args::Optional;

pub fn count((Optional(arg),): (Optional<Value>,)) -> Result<Value> {
	Ok(arg
		.map(|val| {
			if let Value::Array(v) = &val {
				v.iter().filter(|v| v.is_truthy()).count().into()
			} else if let Value::Set(v) = &val {
				v.iter().filter(|v| v.is_truthy()).count().into()
			} else {
				(val.is_truthy() as i64).into()
			}
		})
		.unwrap_or_else(|| 1.into()))
}
