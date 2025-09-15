use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};

/// Typed header implementation for the `Accept` header.
#[derive(Debug)]
pub enum Accept {
	TextPlain,
	ApplicationJson,
	ApplicationCbor,
	ApplicationOctetStream,
	Surrealdb,
}

impl std::fmt::Display for Accept {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Accept::TextPlain => write!(f, "text/plain"),
			Accept::ApplicationJson => write!(f, "application/json"),
			Accept::ApplicationCbor => write!(f, "application/cbor"),
			Accept::ApplicationOctetStream => write!(f, "application/octet-stream"),
			Accept::Surrealdb => write!(f, "application/surrealdb"),
		}
	}
}

impl Header for Accept {
	fn name() -> &'static HeaderName {
		&http::header::ACCEPT
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let parts: Vec<&str> =
			value.to_str().map_err(|_| headers::Error::invalid())?.split(';').collect();

		match parts[0] {
			"text/plain" => Ok(Accept::TextPlain),
			"application/json" => Ok(Accept::ApplicationJson),
			"application/cbor" => Ok(Accept::ApplicationCbor),
			"application/octet-stream" => Ok(Accept::ApplicationOctetStream),
			"application/surrealdb" => Ok(Accept::Surrealdb),
			// TODO: Support more (all?) mime-types
			_ => Err(headers::Error::invalid()),
		}
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl From<Accept> for HeaderValue {
	fn from(value: Accept) -> Self {
		HeaderValue::from(&value)
	}
}

#[expect(clippy::fallible_impl_from)]
impl From<&Accept> for HeaderValue {
	fn from(value: &Accept) -> Self {
		HeaderValue::from_str(value.to_string().as_str()).unwrap()
	}
}
