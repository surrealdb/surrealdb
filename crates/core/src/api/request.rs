use http::HeaderMap;
use surrealdb_types::{Duration, SurrealValue, Value};

#[derive(Clone, SurrealValue)]
pub struct ApiRequest {
    // Request
    pub body: Value,
    pub headers: HeaderMap,

    // Processing options
    pub timeout: Option<Duration>,
}