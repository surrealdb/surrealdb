use crate::err::Error;
use crate::sql::stream::Stream;
use crate::sql::{Bytes, Bytesize, Value};

use super::args::Optional;

pub async fn consume((stream, Optional(max)): (Stream, Optional<String>)) -> Result<Value, Error> {
	let max = max.map(Bytesize::parse).transpose()?;
	let bytes = stream.consume_bytes(max).await?;
	Ok(Value::Bytes(Bytes(bytes)))
}
