mod convert;

pub use convert::Cbor;
use surrealdb::rpc::RpcError;

use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use ciborium::Value as Data;

pub fn req_ws(msg: Message) -> Result<Request, RpcError> {
	match msg {
		Message::Text(val) => {
			surrealdb::sql::value(&val).map_err(|_| RpcError::ParseError)?.try_into()
		}
		Message::Binary(val) => ciborium::from_reader::<Data, _>(&mut val.as_slice())
			.map_err(|_| RpcError::ParseError)
			.map(Cbor)?
			.try_into(),
		_ => Err(RpcError::InvalidRequest),
	}
}

pub fn res_ws(res: Response) -> Result<(usize, Message), RpcError> {
	// Convert the response into a value
	let val: Cbor = res.into_value().try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into CBOR binary data
	ciborium::into_writer(&val.0, &mut res).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}
