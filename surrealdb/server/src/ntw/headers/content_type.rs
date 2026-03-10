use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};

/// Typed header implementation for the `Content-Type` HTTP header.
///
/// This enum represents the content types supported by SurrealDB's HTTP API
/// for request and response serialization. It implements the axum `Header` trait
/// for automatic parsing and encoding in HTTP handlers.
#[derive(Debug)]
pub enum ContentType {
	/// Plain text format (`text/plain`)
	TextPlain,
	/// JSON format (`application/json`)
	ApplicationJson,
	/// CBOR (Concise Binary Object Representation) format (`application/cbor`)
	ApplicationCbor,
	/// Generic binary format (`application/octet-stream`)
	ApplicationOctetStream,
	/// SurrealDB Flatbuffers format (`application/surrealdb+flatbuffers`)
	ApplicationSurrealDBFlatbuffers,
}

/// Pre-allocated static header value for `text/plain` content type
pub(super) static HEADER_VALUE_TEXT_PLAIN: HeaderValue =
	HeaderValue::from_static(surrealdb_core::api::format::PLAIN);
/// Pre-allocated static header value for `application/json` content type
pub(super) static HEADER_VALUE_APPLICATION_JSON: HeaderValue =
	HeaderValue::from_static(surrealdb_core::api::format::JSON);
/// Pre-allocated static header value for `application/cbor` content type
pub(super) static HEADER_VALUE_APPLICATION_CBOR: HeaderValue =
	HeaderValue::from_static(surrealdb_core::api::format::CBOR);
/// Pre-allocated static header value for `application/octet-stream` content type
pub(super) static HEADER_VALUE_APPLICATION_OCTET_STREAM: HeaderValue =
	HeaderValue::from_static(surrealdb_core::api::format::OCTET_STREAM);
/// Pre-allocated static header value for `application/surrealdb+flatbuffers` content type
pub(super) static HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS: HeaderValue =
	HeaderValue::from_static(surrealdb_core::api::format::FLATBUFFERS);

impl std::fmt::Display for ContentType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ContentType::TextPlain => f.write_str(surrealdb_core::api::format::PLAIN),
			ContentType::ApplicationJson => f.write_str(surrealdb_core::api::format::JSON),
			ContentType::ApplicationCbor => f.write_str(surrealdb_core::api::format::CBOR),
			ContentType::ApplicationOctetStream => {
				f.write_str(surrealdb_core::api::format::OCTET_STREAM)
			}
			ContentType::ApplicationSurrealDBFlatbuffers => {
				f.write_str(surrealdb_core::api::format::FLATBUFFERS)
			}
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
			surrealdb_core::api::format::PLAIN => Ok(ContentType::TextPlain),
			surrealdb_core::api::format::JSON => Ok(ContentType::ApplicationJson),
			surrealdb_core::api::format::CBOR => Ok(ContentType::ApplicationCbor),
			surrealdb_core::api::format::OCTET_STREAM => Ok(ContentType::ApplicationOctetStream),
			surrealdb_core::api::format::FLATBUFFERS => {
				Ok(ContentType::ApplicationSurrealDBFlatbuffers)
			}
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

impl From<&ContentType> for HeaderValue {
	fn from(value: &ContentType) -> Self {
		match value {
			ContentType::TextPlain => HEADER_VALUE_TEXT_PLAIN.clone(),
			ContentType::ApplicationJson => HEADER_VALUE_APPLICATION_JSON.clone(),
			ContentType::ApplicationCbor => HEADER_VALUE_APPLICATION_CBOR.clone(),
			ContentType::ApplicationOctetStream => HEADER_VALUE_APPLICATION_OCTET_STREAM.clone(),
			ContentType::ApplicationSurrealDBFlatbuffers => {
				HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS.clone()
			}
		}
	}
}
