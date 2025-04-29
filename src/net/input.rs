use bytes::Bytes;

use crate::err::Error;

pub(crate) fn bytes_to_utf8(bytes: &Bytes) -> Result<&str, Error> {
	std::str::from_utf8(bytes).map_err(|_| Error::Request)
}
