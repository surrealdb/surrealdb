//! HTTP header types and utilities for SurrealDB's network layer.
//!
//! This module provides custom typed headers for SurrealDB-specific HTTP headers
//! (e.g., `Surreal-NS`, `Surreal-DB`, `Surreal-Auth-*`) and utilities for managing
//! standard HTTP headers like `Content-Type`, `Accept`, `Server`, and version headers.

use axum_extra::TypedHeader;
use axum_extra::headers::Header;
use axum_extra::typed_header::{TypedHeaderRejection, TypedHeaderRejectionReason};
use http::header::{InvalidHeaderValue, SERVER};
use http::{HeaderName, HeaderValue};
use surrealdb::headers::VERSION;
use surrealdb_core::cnf::SERVER_NAME;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::cnf::{PKG_NAME, PKG_VERSION};
use crate::ntw::error::Error;

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

/// Creates a Tower middleware layer that conditionally adds a response header.
///
/// This function creates a `SetResponseHeaderLayer` that adds a header to responses
/// only if the header is not already present. The header is only added when `enabled` is true.
///
/// # Parameters
/// - `enabled`: Whether to add the header to responses
/// - `header`: The header value as a string
/// - `default`: The header name to use
///
/// # Returns
/// Returns a `SetResponseHeaderLayer` that can be added to the Axum router,
/// or an error if the header value is invalid.
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

/// Creates a Tower middleware layer that adds the SurrealDB version header to responses.
///
/// When enabled, adds a custom version header (e.g., `surrealdb/2.0.0`) to HTTP responses.
/// This header is only added if not already present in the response.
///
/// # Parameters
/// - `enabled`: Whether to add the version header to responses
///
/// # Returns
/// Returns a `SetResponseHeaderLayer` configured with the version information,
/// or an error if the header value is invalid.
pub fn add_version_header(
	enabled: bool,
) -> Result<SetResponseHeaderLayer<Option<HeaderValue>>, InvalidHeaderValue> {
	add_header(enabled, format!("{}/{}", PKG_NAME, *PKG_VERSION), &VERSION)
}

/// Creates a Tower middleware layer that adds the `Server` header to responses.
///
/// When enabled, adds the standard HTTP `Server` header with SurrealDB's server name.
/// This header is only added if not already present in the response.
///
/// # Parameters
/// - `enabled`: Whether to add the server header to responses
///
/// # Returns
/// Returns a `SetResponseHeaderLayer` configured with the server name,
/// or an error if the header value is invalid.
pub fn add_server_header(
	enabled: bool,
) -> Result<SetResponseHeaderLayer<Option<HeaderValue>>, InvalidHeaderValue> {
	add_header(enabled, SERVER_NAME.to_owned(), &SERVER)
}

/// Parses a typed HTTP header from an Axum extractor result.
///
/// This utility function converts the result of extracting a typed header from an Axum request
/// into a more convenient form. It distinguishes between missing headers (returns `Ok(None)`)
/// and invalid headers (returns `Err`).
///
/// # Parameters
/// - `header`: The result of extracting a `TypedHeader` from an Axum handler
///
/// # Returns
/// - `Ok(Some(value))`: The header was present and valid
/// - `Ok(None)`: The header was missing (not an error)
/// - `Err(Error::InvalidHeader)`: The header was present but invalid
///
/// # Type Parameters
/// - `H`: The header type that implements `Header` and derefs to `String`
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
