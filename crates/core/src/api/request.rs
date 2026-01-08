use std::collections::BTreeMap;

use http::HeaderMap;
use surrealdb_types::SurrealValue;
use crate::{catalog::ApiMethod, sql::expression::convert_public_value_to_internal, types::{PublicObject, PublicValue}, val::{Value, convert_value_to_public_value}};

#[derive(Clone, Default, SurrealValue)]
#[surreal(default)]
pub struct ApiRequest {
    // Request
    pub body: PublicValue,
    pub headers: HeaderMap,
	pub params: PublicObject,
	pub method: ApiMethod,
	pub query: BTreeMap<String, String>,
}

impl TryFrom<Value> for ApiRequest {
	type Error = anyhow::Error;

	fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
		convert_value_to_public_value(value)?.into_t()
	}
}

impl From<ApiRequest> for Value {
	fn from(value: ApiRequest) -> Self {
		convert_public_value_to_internal(value.into_value())
	}
}