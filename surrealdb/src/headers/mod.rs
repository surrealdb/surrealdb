//! HTTP headers used by SurrealDB

use reqwest::header::HeaderName;
use surrealdb_rpc::headers;

pub static ID: HeaderName = HeaderName::from_static(headers::ID);
pub static NS: HeaderName = HeaderName::from_static(headers::NS);
pub static DB: HeaderName = HeaderName::from_static(headers::DB);
pub static AUTH_NS: HeaderName = HeaderName::from_static(headers::AUTH_NS);
pub static AUTH_DB: HeaderName = HeaderName::from_static(headers::AUTH_DB);
pub static VERSION: HeaderName = HeaderName::from_static(headers::VERSION);
