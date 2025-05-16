use crate::sql::{Bytes, Value};
use anyhow::Result;

pub fn len((bytes,): (Bytes,)) -> Result<Value> {
	Ok(bytes.len().into())
}
