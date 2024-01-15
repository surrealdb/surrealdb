mod convert;

pub use convert::Pack;

use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;

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
	// Convert the response into a value
	let val: Pack = res.into_value().try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into MsgPack binary data
	rmpv::encode::write_value(&mut res, &val.0).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}
