use crate::ctx::Context;
use crate::err::Error;
use crate::sql::stream::Stream;
use crate::sql::{Bytes, Value};

pub fn len((bytes,): (Bytes,)) -> Result<Value, Error> {
	Ok(bytes.len().into())
}

pub fn stream(ctx: &Context, (bytes,): (Bytes,)) -> Result<Value, Error> {
	Ok(Value::Stream(Stream::from_bytes(ctx, bytes)?))
}
