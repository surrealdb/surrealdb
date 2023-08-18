use super::{stream::WasmStream, BoxError};
use crate::http::{
	header::{HeaderMap, HeaderValue},
	status::StatusCode,
	version::Version,
	Error, SerializeError,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

pub struct Response {
	inner: lib_http::Response<web_sys::Response>,
}

impl Response {
	pub(super) fn from_js(response: web_sys::Response) -> Result<Self, Error> {
		let inner = lib_http::Response::builder()
			.status(response.status())
			.body(response)
			.map_err(|e| BoxError::from(e))?;

		Ok(Response {
			inner,
		})
	}

	pub fn headers(&self) -> &HeaderMap<HeaderValue> {
		self.inner.headers()
	}

	pub fn headers_mut(&mut self) -> &mut HeaderMap<HeaderValue> {
		self.inner.headers_mut()
	}

	pub fn version(&self) -> Version {
		self.inner.version()
	}

	pub fn version_mut(&mut self) -> &mut Version {
		self.inner.version_mut()
	}

	pub fn status(&self) -> StatusCode {
		self.inner.status()
	}

	pub fn status_mut(&mut self) -> &mut StatusCode {
		self.inner.status_mut()
	}

	async fn into_buffer(self) -> Result<Vec<u8>, Error> {
		let array_buffer = JsFuture::from(self.inner.into_body().array_buffer()?).await?;
		let uint8_array = js_sys::Uint8Array::new(&array_buffer);
		let length = uint8_array.byte_length();
		let mut buffer = vec![0u8; length as usize];
		uint8_array.copy_to(&mut buffer);
		Ok(buffer)
	}

	pub async fn text(self) -> Result<String, Error> {
		let buffer = self.into_buffer().await?;
		Ok(String::from_utf8(buffer)?)
	}

	pub async fn bytes(self) -> Result<Bytes, Error> {
		let buffer = self.into_buffer().await?;
		Ok(Bytes::from(buffer))
	}

	pub fn bytes_stream(self) -> impl Stream<Item = Result<Bytes, Error>> {
		// web_sys has no support fo
		let body = self.inner.into_body();
		// Body should not be used already since all methods which consume the body take self by
		// value.
		let stream = body.body().unwrap();
		let stream = WasmStream::new(stream);
		stream.map(|item| -> Result<Bytes, Error> {
			match item {
				Ok(x) => {
					let array = x.dyn_into::<js_sys::Uint8Array>().map_err(|_| {
						BoxError::from("Readablestream returned value of wrong type")
					})?;
					let length = array.byte_length();
					let mut buffer = vec![0u8; length as usize];
					array.copy_to(&mut buffer);
					Ok(Bytes::from(buffer))
				}
				Err(e) => Err(Error::from(e)),
			}
		})
	}

	pub async fn body(self) -> Result<Vec<u8>, Error> {
		Ok(self.bytes().await?.to_vec())
	}

	pub async fn json<D>(self) -> Result<D, Error>
	where
		D: DeserializeOwned,
	{
		let full = self.bytes().await?;
		serde_json::from_slice(&full).map_err(SerializeError::from).map_err(Error::Decode)
	}

	pub fn is_success(&self) -> bool {
		self.inner.status().is_success()
	}

	pub fn error_for_status(self) -> Result<Self, Error> {
		let status = self.status();
		if status.is_server_error() || status.is_client_error() {
			return Err(Error::StatusCode(status));
		}
		Ok(self)
	}

	pub fn error_for_status_ref(&self) -> Result<&Self, Error> {
		let status = self.status();
		if status.is_server_error() || status.is_client_error() {
			return Err(Error::StatusCode(status));
		}
		Ok(self)
	}
}
