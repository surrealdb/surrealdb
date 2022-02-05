use http::header::{HeaderValue, CONTENT_TYPE};
use http::StatusCode;
use serde::Serialize;

pub enum Output {
	None,
	Json(Vec<u8>), // JSON
	Cbor(Vec<u8>), // CBOR
	Pack(Vec<u8>), // MessagePack
}

pub fn json<T>(val: &T) -> Output
where
	T: Serialize,
{
	match serde_json::to_vec(val) {
		Ok(v) => Output::Json(v),
		Err(_) => Output::None,
	}
}

pub fn cbor<T>(val: &T) -> Output
where
	T: Serialize,
{
	match serde_cbor::to_vec(val) {
		Ok(v) => Output::Cbor(v),
		Err(_) => Output::None,
	}
}

pub fn pack<T>(val: &T) -> Output
where
	T: Serialize,
{
	match serde_pack::to_vec(val) {
		Ok(v) => Output::Pack(v),
		Err(_) => Output::None,
	}
}

impl warp::Reply for Output {
	fn into_response(self) -> warp::reply::Response {
		match self {
			Output::Json(v) => {
				let mut res = warp::reply::Response::new(v.into());
				let con = HeaderValue::from_static("application/json");
				res.headers_mut().insert(CONTENT_TYPE, con);
				res
			}
			Output::Cbor(v) => {
				let mut res = warp::reply::Response::new(v.into());
				let con = HeaderValue::from_static("application/cbor");
				res.headers_mut().insert(CONTENT_TYPE, con);
				res
			}
			Output::Pack(v) => {
				let mut res = warp::reply::Response::new(v.into());
				let con = HeaderValue::from_static("application/msgpack");
				res.headers_mut().insert(CONTENT_TYPE, con);
				res
			}
			Output::None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
		}
	}
}
