use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};

/// Typed header implementation for the `ContentType` header.
pub enum ContentType {
	TextPlain,
	ApplicationJson,
	ApplicationCbor,
	ApplicationOctetStream,
	Surrealdb,
}

impl std::fmt::Display for ContentType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ContentType::TextPlain => write!(f, "text/plain"),
			ContentType::ApplicationJson => write!(f, "application/json"),
			ContentType::ApplicationCbor => write!(f, "application/cbor"),
			ContentType::ApplicationOctetStream => write!(f, "application/octet-stream"),
			ContentType::Surrealdb => write!(f, "application/surrealdb"),
		}
	}
}

impl Header for ContentType {
	fn name() -> &'static HeaderName {
		&http::header::CONTENT_TYPE
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let parts: Vec<&str> =
			value.to_str().map_err(|_| headers::Error::invalid())?.split(';').collect();

		match parts[0] {
			"text/plain" => Ok(ContentType::TextPlain),
			"application/json" => Ok(ContentType::ApplicationJson),
			"application/cbor" => Ok(ContentType::ApplicationCbor),
			"application/octet-stream" => Ok(ContentType::ApplicationOctetStream),
			"application/surrealdb" => Ok(ContentType::Surrealdb),
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

impl From<ContentType> for HeaderValue {
	fn from(value: ContentType) -> Self {
		HeaderValue::from(&value)
	}
}

#[expect(clippy::fallible_impl_from)]
impl From<&ContentType> for HeaderValue {
	fn from(value: &ContentType) -> Self {
		HeaderValue::from_str(value.to_string().as_str()).unwrap()
	}
}
