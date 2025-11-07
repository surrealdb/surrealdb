use axum_extra::TypedHeader;
use axum_extra::headers::Header;
use axum_extra::typed_header::{TypedHeaderRejection, TypedHeaderRejectionReason};
use http::header::{InvalidHeaderValue, SERVER};
use http::{HeaderName, HeaderValue};
use surrealdb::headers::VERSION;
use surrealdb_core::cnf::SERVER_NAME;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::cnf::{PKG_NAME, PKG_VERSION};
use crate::net::error::Error;

mod accept;
mod auth_db;
mod auth_ns;
mod content_type;
mod db;
mod id;
mod ns;

pub use accept::Accept;
pub use auth_db::SurrealAuthDatabase;
pub use auth_ns::SurrealAuthNamespace;
pub use content_type::ContentType;
pub use db::SurrealDatabase;
pub use id::SurrealId;
pub use ns::SurrealNamespace;

pub fn add_header(
	enabled: bool,
	header: String,
	default: &HeaderName,
) -> Result<SetResponseHeaderLayer<Option<HeaderValue>>, InvalidHeaderValue> {
	let header_value = if enabled {
		Some(HeaderValue::try_from(header)?)
	} else {
		None
	};
	Ok(SetResponseHeaderLayer::if_not_present(default.clone(), header_value))
}

pub fn add_version_header(
	enabled: bool,
) -> Result<SetResponseHeaderLayer<Option<HeaderValue>>, InvalidHeaderValue> {
	add_header(enabled, format!("{}/{}", PKG_NAME, *PKG_VERSION), &VERSION)
}

pub fn add_server_header(
	enabled: bool,
) -> Result<SetResponseHeaderLayer<Option<HeaderValue>>, InvalidHeaderValue> {
	add_header(enabled, SERVER_NAME.to_owned(), &SERVER)
}

// Parse a TypedHeader, returning None if the header is missing and an error if
// the header is invalid.
pub fn parse_typed_header<H>(
	header: Result<TypedHeader<H>, TypedHeaderRejection>,
) -> Result<Option<String>, Error>
where
	H: std::ops::Deref<Target = String> + Header,
{
	match header {
		Ok(TypedHeader(val)) => Ok(Some(val.to_string())),
		Err(e) => match e.reason() {
			TypedHeaderRejectionReason::Missing => Ok(None),
			_ => Err(Error::InvalidHeader(H::name().to_owned(), e.to_string())),
		},
	}
}
