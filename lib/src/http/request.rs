use base64_lib::write::EncoderWriter;
use lib_http::{header::AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, Method};
use serde::Serialize;
use std::time::Duration;
use url::Url;

use super::{Client, Error, IntoUrl, Response};

pub struct Request {
	method: Method,
	url: Url,
	headers: HeaderMap,
	timeout: Option<Duration>,
	client: Client,
}

impl Request {
	pub fn new<U, M>(method: M, url: U, client: Client) -> Result<Self, Error>
	where
		U: IntoUrl,
		Method: TryFrom<M>,
		<Method as TryFrom<M>>::Error: Into<Error>,
	{
		let url = url.into_url();
		let method = Method::try_fro(method).map_err(|e| e.into())?;
		Ok(Self {
			url,
			method,
			headers: HeaderMap::new(),
			timeout: None,
			client,
		})
	}

	pub async fn send(self) -> Result<Response, Error> {
		todo!()
	}

	pub fn basic_auth<U, P>(mut self, username: U, password: Option<P>) -> Self
	where
		U: std::fmt::Display,
		P: std::fmt::Display,
	{
		use std::fmt::Write;

		let mut buf = b"Basic ".to_vec();

		let mut encoder = EncoderWriter::new(&mut buf, &base64_lib::prelude::BASE64_STANDARD);
		let _ = write!(&mut encoder, "{}:", username);
		if let Some(password) = password {
			let _ = write!(&mut encoder, "{}:", password);
		}

		let mut header =
			HeaderValue::from_bytes(&buf).expect("basic auth header should always be valid");
		header.set_sensitive(true);
		self.headers.insert(AUTHORIZATION, header);
		self
	}

	pub fn bearer_auth<T>(mut self, token: T) -> Result<Self, Error>
	where
		T: std::fmt::Display,
	{
		let token = format!("Bearer {token}");
		let header = HeaderValue::from_str(&token).map_err(|_| Error::InvalidToken)?;
		header.set_sensitive(true);
		self.headers.insert(AUTHORIZATION, header);
		Ok(self)
	}

	/// Merges the existing headers with the new given ones.
	pub fn headers(mut self, headers: HeaderMap) -> Self {
		self.headers.extend(headers.into_iter());
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
		self.header.append(name, value);
		Ok(self)
	}

	pub fn timeout(mut self, duration: Duration) -> Self {
		todo!()
	}

	pub fn body<B>(mut self, body: B) -> Self {
		todo!()
	}

	pub fn json<S>(mut self, value: S) -> Result<Self, Error>
	where
		S: Serialize,
	{
		todo!()
	}
}
