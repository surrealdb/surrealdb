use crate::err::Error;
use crate::sql::{Bytes, Value};

pub fn len((bytes,): (Bytes,)) -> Result<Value, Error> {
	Ok(bytes.len().into())
}

pub fn stream((bytes,): (Bytes,)) -> Result<Value, Error> {
	Ok(Value::Stream(bytes.into()))
}
