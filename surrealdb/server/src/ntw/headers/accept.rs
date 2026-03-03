use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};

use crate::ntw::headers::content_type::{
	HEADER_VALUE_APPLICATION_CBOR, HEADER_VALUE_APPLICATION_JSON,
	HEADER_VALUE_APPLICATION_OCTET_STREAM, HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS,
	HEADER_VALUE_TEXT_PLAIN,
};

/// Typed header implementation for the `Accept` header.
#[derive(Debug)]
pub enum Accept {
	TextPlain,
	ApplicationJson,
	ApplicationCbor,
	ApplicationOctetStream,
	ApplicationFlatbuffers,
}

impl std::fmt::Display for Accept {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Accept::TextPlain => f.write_str(surrealdb_core::api::format::PLAIN),
			Accept::ApplicationJson => f.write_str(surrealdb_core::api::format::JSON),
			Accept::ApplicationCbor => f.write_str(surrealdb_core::api::format::CBOR),
			Accept::ApplicationOctetStream => {
				f.write_str(surrealdb_core::api::format::OCTET_STREAM)
			}
			Accept::ApplicationFlatbuffers => f.write_str(surrealdb_core::api::format::FLATBUFFERS),
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
			surrealdb_core::api::format::ANY => Ok(Accept::ApplicationJson),
			surrealdb_core::api::format::PLAIN => Ok(Accept::TextPlain),
			surrealdb_core::api::format::JSON => Ok(Accept::ApplicationJson),
			surrealdb_core::api::format::CBOR => Ok(Accept::ApplicationCbor),
			surrealdb_core::api::format::OCTET_STREAM => Ok(Accept::ApplicationOctetStream),
			surrealdb_core::api::format::FLATBUFFERS => Ok(Accept::ApplicationFlatbuffers),
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

impl From<&Accept> for HeaderValue {
	fn from(value: &Accept) -> Self {
		match value {
			Accept::TextPlain => HEADER_VALUE_TEXT_PLAIN.clone(),
			Accept::ApplicationJson => HEADER_VALUE_APPLICATION_JSON.clone(),
			Accept::ApplicationCbor => HEADER_VALUE_APPLICATION_CBOR.clone(),
			Accept::ApplicationOctetStream => HEADER_VALUE_APPLICATION_OCTET_STREAM.clone(),
			Accept::ApplicationFlatbuffers => {
				HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS.clone()
			}
		}
	}
}
