use crate::err::Error;
use crate::expr::{Bytes, Value};

pub fn len((bytes,): (Bytes,)) -> Result<Value, Error> {
	Ok(bytes.len().into())
}
