use http::HeaderMap;
use surrealdb_types::SurrealValue;
use crate::types::{PublicDuration, PublicValue};

#[derive(Clone, Default, SurrealValue)]
#[surreal(default)]
pub struct ApiRequest {
    // Request
    pub body: PublicValue,
    pub headers: HeaderMap,

    // Processing options
    pub timeout: Option<PublicDuration>,
}