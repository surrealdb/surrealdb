use crate::http::{
	header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE},
	method::Method,
};
use base64_lib::write::EncoderWriter;
use serde::Serialize;
use std::time::Duration;
use url::Url;

use super::{Body, Client, Error, Response, SerializeError};

pub struct Request {
	pub(super) method: Method,
	pub(super) url: Url,
	pub(super) headers: HeaderMap,
	pub(super) timeout: Option<Duration>,
	pub(super) client: Client,
	pub(super) body: Body,
}

impl Request {
	pub fn new<U, M>(method: M, url: U, client: Client) -> Result<Self, Error>
	where
		Url: TryFrom<U>,
		<Url as TryFrom<U>>::Error: Into<Error>,
		Method: TryFrom<M>,
		<Method as TryFrom<M>>::Error: Into<Error>,
	{
		let url = Url::try_from(url).map_err(|e| e.into())?;
		let method = Method::try_from(method).map_err(|e| e.into())?;
		Ok(Self {
			url,
			method,
			headers: HeaderMap::new(),
			timeout: None,
			client,
			body: Body::empty(),
		})
	}

	pub async fn send(self) -> Result<Response, Error> {
		self.client.clone().inner.execute(self).await
	}

	pub fn basic_auth<U, P>(mut self, username: U, password: Option<P>) -> Self
	where
		U: std::fmt::Display,
		P: std::fmt::Display,
	{
		use std::io::Write;

		let mut buf = b"Basic ".to_vec();

		{
			let mut encoder = EncoderWriter::new(&mut buf, &base64_lib::prelude::BASE64_STANDARD);
			let _ = write!(&mut encoder, "{}:", username);
			if let Some(password) = password {
				let _ = write!(&mut encoder, "{}:", password);
			}

			// Writing into a vector shouldn't fail
			encoder.finish().unwrap();
		}

		let mut header = HeaderValue::from_bytes(buf.as_slice())
			.expect("basic auth header should always be valid");
		header.set_sensitive(true);
		self.headers.insert(AUTHORIZATION, header);
		self
	}

	pub fn bearer_auth<T>(mut self, token: T) -> Result<Self, Error>
	where
		T: std::fmt::Display,
	{
		let token = format!("Bearer {token}");
		let mut header = HeaderValue::from_str(&token).map_err(|_| Error::InvalidToken)?;
		header.set_sensitive(true);
		self.headers.insert(AUTHORIZATION, header);
		Ok(self)
	}

	pub fn query<T: Serialize + ?Sized>(mut self, query: &T) -> Result<Self, Error> {
		{
			let mut pairs = self.url.query_pairs_mut();
			let serializer = serde_urlencoded::Serializer::new(&mut pairs);
			query.serialize(serializer).map_err(SerializeError::from).map_err(Error::Encode)?;
		}
		Ok(self)
	}

	/// Merges the existing headers with the new given ones.
	pub fn headers(mut self, headers: HeaderMap) -> Self {
		self.headers.extend(headers);
		self
	}

	pub fn header<K, V>(mut self, name: K, value: V) -> Result<Self, Error>
	where
		HeaderName: TryFrom<K>,
		<HeaderName as TryFrom<K>>::Error: Into<Error>,
		HeaderValue: TryFrom<V>,
		<HeaderValue as TryFrom<V>>::Error: Into<Error>,
	{
		let name = HeaderName::try_from(name).map_err(|e| e.into())?;
		let value = HeaderValue::try_from(value).map_err(|e| e.into())?;
		self.headers.append(name, value);
		Ok(self)
	}

	pub fn timeout(mut self, duration: Duration) -> Self {
		self.timeout = Some(duration);
		self
	}

	pub fn body<B>(mut self, body: B) -> Self
	where
		Body: From<B>,
	{
		self.body = body.into();
		self
	}

	pub fn json<S>(mut self, value: &S) -> Result<Self, Error>
	where
		S: Serialize,
	{
		let value =
			serde_json::to_vec(value).map_err(SerializeError::from).map_err(Error::Encode)?;
		self.body = Body::from(value);
		let header_value = HeaderValue::from_static("application/json");
		self.headers.insert(CONTENT_TYPE, header_value);
		Ok(self)
	}

	pub fn text<S>(mut self, value: S) -> Result<Self, Error>
	where
		String: From<S>,
	{
		let value = String::from(value);
		self.body = Body::from(value);
		let header_value = HeaderValue::from_static("plain/text");
		self.headers.insert(CONTENT_TYPE, header_value);
		Ok(self)
	}
}
