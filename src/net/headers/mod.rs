use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::err::Error;
use axum::extract::rejection::TypedHeaderRejection;
use axum::extract::rejection::TypedHeaderRejectionReason;
use axum::headers::Header;
use axum::TypedHeader;
use http::header::SERVER;
use http::HeaderValue;
use surrealdb::cnf::SERVER_NAME;
use surrealdb::headers::VERSION;
use tower_http::set_header::SetResponseHeaderLayer;

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

pub fn add_version_header() -> SetResponseHeaderLayer<HeaderValue> {
	let val = format!("{PKG_NAME}-{}", *PKG_VERSION);
	SetResponseHeaderLayer::if_not_present(VERSION.to_owned(), HeaderValue::try_from(val).unwrap())
}

pub fn add_server_header() -> SetResponseHeaderLayer<HeaderValue> {
	SetResponseHeaderLayer::if_not_present(SERVER, HeaderValue::try_from(SERVER_NAME).unwrap())
}

// Parse a TypedHeader, returning None if the header is missing and an error if the header is invalid.
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
			_ => Err(Error::InvalidHeader(H::name().to_owned(), e)),
		},
	}
}
