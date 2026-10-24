use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};

use crate::ntw::headers::content_type::{
	HEADER_VALUE_APPLICATION_CBOR, HEADER_VALUE_APPLICATION_CBOR2, HEADER_VALUE_APPLICATION_JSON,
	HEADER_VALUE_APPLICATION_OCTET_STREAM, HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS,
	HEADER_VALUE_APPLICATION_SURREAL_DB_SHAPED, HEADER_VALUE_TEXT_PLAIN,
};

/// Typed header implementation for the `Accept` header.
#[derive(Debug)]
pub enum Accept {
	TextPlain,
	ApplicationJson,
	ApplicationCbor,
	ApplicationCbor2,
	ApplicationOctetStream,
	ApplicationFlatbuffers,
	/// CBOR responses whose result rows are shaped blocks.
	ApplicationShaped,
}

impl std::fmt::Display for Accept {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Accept::TextPlain => f.write_str(surrealdb_rpc::format::PLAIN),
			Accept::ApplicationJson => f.write_str(surrealdb_rpc::format::JSON),
			Accept::ApplicationCbor => f.write_str(surrealdb_rpc::format::CBOR),
			Accept::ApplicationCbor2 => f.write_str(surrealdb_rpc::format::CBOR2),
			Accept::ApplicationOctetStream => f.write_str(surrealdb_rpc::format::OCTET_STREAM),
			Accept::ApplicationFlatbuffers => f.write_str(surrealdb_rpc::format::FLATBUFFERS),
			Accept::ApplicationShaped => f.write_str(surrealdb_rpc::format::SHAPED),
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
		let s = value.to_str().map_err(|_| headers::Error::invalid())?;
		let parts: Vec<&str> = s.split(';').collect();

		let media_type = parts[0].trim();
		match media_type {
			surrealdb_rpc::format::ANY => Ok(Accept::ApplicationJson),
			surrealdb_rpc::format::PLAIN => Ok(Accept::TextPlain),
			surrealdb_rpc::format::JSON => Ok(Accept::ApplicationJson),
			surrealdb_rpc::format::CBOR => {
				let mut out = Accept::ApplicationCbor;
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
							"1" => Accept::ApplicationCbor,
							"2" => Accept::ApplicationCbor2,
							_ => return Err(headers::Error::invalid()),
						};
					}
				}
				Ok(out)
			}
			surrealdb_rpc::format::OCTET_STREAM => Ok(Accept::ApplicationOctetStream),
			surrealdb_rpc::format::FLATBUFFERS => Ok(Accept::ApplicationFlatbuffers),
			surrealdb_rpc::format::SHAPED => Ok(Accept::ApplicationShaped),
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
			Accept::ApplicationCbor2 => HEADER_VALUE_APPLICATION_CBOR2.clone(),
			Accept::ApplicationOctetStream => HEADER_VALUE_APPLICATION_OCTET_STREAM.clone(),
			Accept::ApplicationFlatbuffers => {
				HEADER_VALUE_APPLICATION_SURREAL_DB_FLATBUFFERS.clone()
			}
			Accept::ApplicationShaped => HEADER_VALUE_APPLICATION_SURREAL_DB_SHAPED.clone(),
		}
	}
}
