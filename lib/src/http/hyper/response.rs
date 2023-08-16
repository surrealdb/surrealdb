use crate::http::SerializeError;

use super::super::Error;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use hyper::body::{self, HttpBody};
use lib_http::{HeaderMap, HeaderValue, StatusCode, Version};
use serde::{de::DeserializeOwned, Deserialize};
use std::str;

pub struct Response {
	inner: hyper::Response<hyper::Body>,
}

impl Response {
	pub fn from_hyper(inner: hyper::Response<hyper::Body>) -> Self {
		Response {
			inner,
		}
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

	pub async fn text(self) -> Result<String, Error> {
		let bytes = self.bytes().await?;
		Ok(str::from_utf8(&bytes)?.to_owned())
	}

	pub async fn bytes(self) -> Result<Bytes, Error> {
		body::to_bytes(self.inner.into_body()).await.map_err(Error::Backend)
	}

	pub fn bytes_stream(self) -> impl Stream<Item = Result<Bytes, Error>> {
		let body = self.inner.into_body();
		TryStreamExt::map_err(body, Error::Backend)
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
