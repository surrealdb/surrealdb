use crate::ctx::Context;
use crate::err::Error;
use crate::sql::stream::Stream;
use crate::sql::{Bytes, Value};

pub async fn bytes(ctx: &Context, (stream,): (Stream,)) -> Result<Value, Error> {
	let bytes = stream.consume_bytes(ctx).await?;
	Ok(Value::Bytes(Bytes(bytes)))
}
