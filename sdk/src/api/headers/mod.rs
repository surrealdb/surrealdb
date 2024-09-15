//! HTTP headers used by SurrealDB

use reqwest::header::HeaderName;

pub static ID: HeaderName = HeaderName::from_static("surreal-id");
pub static NS: HeaderName = HeaderName::from_static("surreal-ns");
pub static DB: HeaderName = HeaderName::from_static("surreal-db");
pub static AUTH_NS: HeaderName = HeaderName::from_static("surreal-auth-ns");
pub static AUTH_DB: HeaderName = HeaderName::from_static("surreal-auth-db");
pub static VERSION: HeaderName = HeaderName::from_static("surreal-version");
