use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};

/// Typed header implementation for the `Content-Type` HTTP header.
///
/// This enum represents the content types supported by SurrealDB's HTTP API
/// for request and response serialization. It implements the axum `Header` trait
/// for automatic parsing and encoding in HTTP handlers.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum ContentType {
	/// Plain text format (`text/plain`)
	TextPlain,
	/// JSON format (`application/json`)
	ApplicationJson,
	/// CBOR (Concise Binary Object Representation) format (`application/cbor`)
	ApplicationCbor,
	/// CBOR v2 format (`application/cbor; v=2`)
	ApplicationCbor2,
	/// Generic binary format (`application/octet-stream`)
	ApplicationOctetStream,
	/// SurrealDB Flatbuffers format (`application/surrealdb+flatbuffers`)
	ApplicationSurrealDBFlatbuffers,
	/// SurrealDB shaped frames (`application/vnd.surrealdb.shaped`): a request is CBOR, and a
	/// response is CBOR whose result rows are shaped blocks
	ApplicationSurrealDBShaped,
}

/// Pre-allocated static header value for `text/plain` content type
pub(super) static HEADER_VALUE_TEXT_PLAIN: HeaderValue =
	HeaderValue::from_static(surrealdb_rpc::format::PLAIN);
/// Pre-allocated static header value for `application/json` content type
pub(super) static HEADER_VALUE_APPLICATION_JSON: HeaderValue =
	HeaderValue::from_static(surrealdb_rpc::format::JSON);
/// Pre-allocated static header value for `application/cbor` content type
pub(super) static HEADER_VALUE_APPLICATION_CBOR: HeaderValue =
	HeaderValue::from_static(surrealdb_rpc::format::CBOR);
/// Pre-allocated static header value for `application/cbor; v=2` content type
pub(super) static HEADER_VALUE_APPLICATION_CBOR2: HeaderValue =
	HeaderValue::from_static(surrealdb_rpc::format::CBOR2);
/// Pre-allocated static header value for `application/octet-stream` content type
pub(super) static HEADER_VALUE_APPLICATION_OCTET_STREAM: HeaderValue =
	HeaderValue::from_static(surrealdb_rpc::format::OCTET_STREAM);
/// Pre-allocated static header value for `application/surrealdb+flatbuffers` content type
pub(super) static HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS: HeaderValue =
	HeaderValue::from_static(surrealdb_rpc::format::FLATBUFFERS);
/// Pre-allocated static header value for `application/vnd.surrealdb.shaped` content type
pub(super) static HEADER_VALUE_APPLICATION_SURREAL_DB_SHAPED: HeaderValue =
	HeaderValue::from_static(surrealdb_rpc::format::SHAPED);

impl std::fmt::Display for ContentType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ContentType::TextPlain => f.write_str(surrealdb_rpc::format::PLAIN),
			ContentType::ApplicationJson => f.write_str(surrealdb_rpc::format::JSON),
			ContentType::ApplicationCbor => f.write_str(surrealdb_rpc::format::CBOR),
			ContentType::ApplicationCbor2 => f.write_str(surrealdb_rpc::format::CBOR2),
			ContentType::ApplicationOctetStream => f.write_str(surrealdb_rpc::format::OCTET_STREAM),
			ContentType::ApplicationSurrealDBFlatbuffers => {
				f.write_str(surrealdb_rpc::format::FLATBUFFERS)
			}
			ContentType::ApplicationSurrealDBShaped => f.write_str(surrealdb_rpc::format::SHAPED),
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
		let s = value.to_str().map_err(|_| headers::Error::invalid())?;
		let parts: Vec<&str> = s.split(';').collect();

		let media_type = parts[0].trim();
		match media_type {
			surrealdb_rpc::format::PLAIN => Ok(ContentType::TextPlain),
			surrealdb_rpc::format::JSON => Ok(ContentType::ApplicationJson),
			surrealdb_rpc::format::CBOR => {
				let mut out = ContentType::ApplicationCbor;
				let mut seen_version = false;
				for param in &parts[1..] {
					let param = param.trim();
					if let Some((k, v)) = param.split_once('=')
						&& k.trim().eq_ignore_ascii_case("v")
					{
						if seen_version {
							return Err(headers::Error::invalid());
						}
						seen_version = true;
						let v = v.trim();
						let v = v.strip_prefix('"').and_then(|s| s.strip_suffix('"')).unwrap_or(v);
						out = match v {
							"1" => ContentType::ApplicationCbor,
							"2" => ContentType::ApplicationCbor2,
							_ => return Err(headers::Error::invalid()),
						};
					}
				}
				Ok(out)
			}
			surrealdb_rpc::format::OCTET_STREAM => Ok(ContentType::ApplicationOctetStream),
			surrealdb_rpc::format::FLATBUFFERS => Ok(ContentType::ApplicationSurrealDBFlatbuffers),
			surrealdb_rpc::format::SHAPED => Ok(ContentType::ApplicationSurrealDBShaped),
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
			ContentType::ApplicationCbor2 => HEADER_VALUE_APPLICATION_CBOR2.clone(),
			ContentType::ApplicationOctetStream => HEADER_VALUE_APPLICATION_OCTET_STREAM.clone(),
			ContentType::ApplicationSurrealDBFlatbuffers => {
				HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS.clone()
			}
			ContentType::ApplicationSurrealDBShaped => {
				HEADER_VALUE_APPLICATION_SURREAL_DB_SHAPED.clone()
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ntw::headers::accept::Accept;

	fn decode_content_type(raw: &'static str) -> Result<ContentType, headers::Error> {
		let val = HeaderValue::from_static(raw);
		ContentType::decode(&mut std::iter::once(&val))
	}

	fn decode_accept(raw: &'static str) -> Result<Accept, headers::Error> {
		let val = HeaderValue::from_static(raw);
		Accept::decode(&mut std::iter::once(&val))
	}

	#[test]
	fn cbor_version_parameter_decoding() {
		assert_eq!(decode_content_type("application/cbor").unwrap(), ContentType::ApplicationCbor);
		assert_eq!(
			decode_content_type("application/cbor; v=1").unwrap(),
			ContentType::ApplicationCbor
		);
		assert_eq!(
			decode_content_type("application/cbor; v=\"1\"").unwrap(),
			ContentType::ApplicationCbor
		);
		assert_eq!(
			decode_content_type("application/cbor; v=2").unwrap(),
			ContentType::ApplicationCbor2
		);
		assert_eq!(
			decode_content_type("application/cbor; v=\"2\"").unwrap(),
			ContentType::ApplicationCbor2
		);
		assert!(decode_content_type("application/cbor; v=3").is_err());
		assert!(decode_content_type("application/cbor; v=\"3\"").is_err());
		assert!(decode_content_type("application/cbor; v=1; v=2").is_err());

		assert!(matches!(decode_accept("application/cbor").unwrap(), Accept::ApplicationCbor));
		assert!(matches!(decode_accept("application/cbor; v=1").unwrap(), Accept::ApplicationCbor));
		assert!(matches!(
			decode_accept("application/cbor; v=\"1\"").unwrap(),
			Accept::ApplicationCbor
		));
		assert!(matches!(
			decode_accept("application/cbor; v=2").unwrap(),
			Accept::ApplicationCbor2
		));
		assert!(matches!(
			decode_accept("application/cbor; v=\"2\"").unwrap(),
			Accept::ApplicationCbor2
		));
		assert!(decode_accept("application/cbor; v=3").is_err());
		assert!(decode_accept("application/cbor; v=1; v=2").is_err());
	}
}
