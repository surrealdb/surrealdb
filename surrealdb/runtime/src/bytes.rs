use anyhow::Result;
use surrealdb_expr::val::{Bytes, Value};

pub fn len((bytes,): (Bytes,)) -> Result<Value> {
	Ok(bytes.len().into())
}
