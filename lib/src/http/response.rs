use bytes::Bytes;
use futures::Stream;
use lib_http::HeaderMap;

pub struct Response {
	response
}

impl Response {
	pub async fn text(self) -> Result<String, Error> {
		todo!()
	}

	pub async fn bytes(self) -> Result<Bytes, Error> {
		todo!()
	}

	pub async fn bytes_stream(self) -> impl Stream<Item = Result<Bytes, Error>> {
		todo!()
	}

	pub async fn body(self) -> Result<Vec<u8>, Error> {
		todo!()
	}

	pub fn is_success(&self) -> bool {
		todo!()
	}

	pub fn headers(&self) -> &HeaderMap {
		todo!()
	}

	pub fn error_for_status(self) -> Result<Self, Error> {
		todo!()
	}
}
