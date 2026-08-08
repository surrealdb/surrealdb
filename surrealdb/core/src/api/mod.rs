pub mod err;
pub mod invocation;
pub mod middleware;
pub(crate) use crate::catalog::api_path as path;
pub mod request;
pub mod response;

use http::HeaderName;

/// Header name for SurrealDB request ID tracking
pub const X_SURREAL_REQUEST_ID: HeaderName = HeaderName::from_static("x-surreal-request-id");
