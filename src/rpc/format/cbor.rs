use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use serde_cbor::Value as Data;
use surrealdb::sql::Datetime;
use surrealdb::sql::Thing;
use surrealdb::sql::Uuid;
use surrealdb::sql::Value;

pub fn req(msg: Message) -> Result<Request, Failure> {
	match msg {
		Message::Text(val) => {
			surrealdb::sql::value(&val).map_err(|_| Failure::PARSE_ERROR)?.try_into()
		}
		Message::Binary(val) => serde_cbor::from_slice::<Data>(&val)
			.map_err(|_| Failure::PARSE_ERROR)
			.map(Cbor)?
			.try_into(),
		_ => Err(Failure::INVALID_REQUEST),
	}
}

pub fn res(res: Response) -> Result<(usize, Message), Failure> {
	// Serialize the response with simplified type information
	let res = serde_cbor::to_vec(&res.simplify()).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}

#[derive(Debug)]
pub struct Cbor(pub Data);

impl TryFrom<Cbor> for Value {
	type Error = Failure;
	fn try_from(val: Cbor) -> Result<Self, Failure> {
		match val.0 {
			Data::Null => Ok(Value::Null),
			Data::Bool(v) => Ok(Value::from(v)),
			Data::Integer(v) => Ok(Value::from(v)),
			Data::Float(v) => Ok(Value::from(v)),
			Data::Bytes(v) => Ok(Value::Bytes(v.into())),
			Data::Text(v) => Ok(Value::from(v)),
			Data::Array(v) => v
				.into_iter()
				.map(|v| <Cbor as TryInto<Value>>::try_into(Cbor(v)))
				.collect::<Result<Value, Failure>>(),
			Data::Map(v) => v
				.into_iter()
				.map(|(k, v)| {
					let k = <Cbor as TryInto<Value>>::try_into(Cbor(k)).map(|v| v.as_raw_string());
					let v = <Cbor as TryInto<Value>>::try_into(Cbor(v));
					Ok((k?, v?))
				})
				.collect::<Result<Value, Failure>>(),
			Data::Tag(t, v) => {
				match t {
					// A literal uuid
					7877325_1 => match *v {
						Data::Text(v) => match Uuid::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err(Failure::custom("Expected a valid UUID value")),
						},
						_ => Err(Failure::custom("Expected a CBOR text data type")),
					},
					// A literal datetime
					7877325_2 => match *v {
						Data::Text(v) => match Datetime::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err(Failure::custom("Expected a valid Datetime value")),
						},
						_ => Err(Failure::custom("Expected a CBOR text data type")),
					},
					// A literal uuid
					7877325_3 => match *v {
						Data::Text(v) => match Thing::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err(Failure::custom("Expected a valid RecordID value")),
						},
						_ => Err(Failure::custom("Expected a CBOR text data type")),
					},
					// An unknown tag
					_ => Err(Failure::custom("Encountered an unknown CBOR tag")),
				}
			}
			_ => Err(Failure::custom("Encountered an unknown CBOR data type")),
		}
	}
}
