use crate::err::Error;
use bytes::Bytes;

pub(crate) fn bytes_to_utf8(bytes: &Bytes) -> Result<&str, warp::Rejection> {
	std::str::from_utf8(bytes).map_err(|_| warp::reject::custom(Error::Request))
}
