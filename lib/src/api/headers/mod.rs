//! HTTP headers used by SurrealDB

use reqwest::header::HeaderName;

pub static ID: HeaderName = HeaderName::from_static("surreal-id");
pub static ID_LEGACY: HeaderName = HeaderName::from_static("id");
pub static NS: HeaderName = HeaderName::from_static("surreal-ns");
pub static NS_LEGACY: HeaderName = HeaderName::from_static("ns");
pub static DB: HeaderName = HeaderName::from_static("surreal-db");
pub static DB_LEGACY: HeaderName = HeaderName::from_static("db");
pub static AUTH_NS: HeaderName = HeaderName::from_static("surreal-auth-ns");
pub static AUTH_DB: HeaderName = HeaderName::from_static("surreal-auth-db");
pub static VERSION: HeaderName = HeaderName::from_static("surreal-version");
pub static VERSION_LEGACY: HeaderName = HeaderName::from_static("version");
