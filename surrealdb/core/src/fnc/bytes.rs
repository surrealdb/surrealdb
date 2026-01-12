use anyhow::Result;

use crate::val::{Bytes, Value};

pub fn len((bytes,): (Bytes,)) -> Result<Value> {
	Ok(bytes.len().into())
}
