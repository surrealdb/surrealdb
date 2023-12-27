use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use rmpv::Value as Data;
use surrealdb::sql::Value;

pub fn req(msg: Message) -> Result<Request, Failure> {
	match msg {
		Message::Text(val) => {
			surrealdb::sql::value(&val).map_err(|_| Failure::PARSE_ERROR)?.try_into()
		}
		Message::Binary(val) => rmpv::decode::read_value(&mut val.as_slice())
			.map_err(|_| Failure::PARSE_ERROR)
			.map(Pack)?
			.try_into(),
		_ => Err(Failure::INVALID_REQUEST),
	}
}

pub fn res(res: Response) -> Result<(usize, Message), Failure> {
	// Serialize the response with simplified type information
	let res = serde_pack::to_vec(&res.simplify()).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}

#[derive(Debug)]
pub struct Pack(pub Data);

impl TryFrom<Pack> for Value {
	type Error = Failure;
	fn try_from(val: Pack) -> Result<Self, Failure> {
		match val.0 {
			Data::Nil => Ok(Value::Null),
			Data::Boolean(v) => Ok(Value::from(v)),
			Data::Integer(v) if v.is_i64() => match v.as_i64() {
				Some(v) => Ok(Value::from(v)),
				None => Ok(Value::Null),
			},
			Data::Integer(v) if v.is_u64() => match v.as_u64() {
				Some(v) => Ok(Value::from(v)),
				None => Ok(Value::Null),
			},
			Data::F32(v) => Ok(Value::from(v)),
			Data::F64(v) => Ok(Value::from(v)),
			Data::String(v) => match v.into_str() {
				Some(v) => Ok(Value::from(v)),
				None => Ok(Value::Null),
			},
			Data::Binary(v) => Ok(Value::Bytes(v.into())),
			Data::Array(v) => v
				.into_iter()
				.map(|v| <Pack as TryInto<Value>>::try_into(Pack(v)))
				.collect::<Result<Value, Failure>>(),
			Data::Map(v) => v
				.into_iter()
				.map(|(k, v)| {
					let k = <Pack as TryInto<Value>>::try_into(Pack(k)).map(|v| v.as_raw_string());
					let v = <Pack as TryInto<Value>>::try_into(Pack(v));
					Ok((k?, v?))
				})
				.collect::<Result<Value, Failure>>(),
			_ => Ok(Value::Null),
		}
	}
}
