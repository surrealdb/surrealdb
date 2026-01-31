use http::{HeaderMap, StatusCode};
use surrealdb_types::SurrealValue;

use crate::sql::expression::convert_public_value_to_internal;
use crate::types::{PublicObject, PublicValue};
use crate::val::{Value, convert_value_to_public_value};

#[derive(Debug, Default, SurrealValue)]
#[surreal(default)]
pub struct ApiResponse {
	pub status: StatusCode,
	pub body: PublicValue,
	pub headers: HeaderMap,
	pub context: PublicObject,
}

impl TryFrom<Value> for ApiResponse {
	type Error = anyhow::Error;

	fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
		convert_value_to_public_value(value)?.into_t()
	}
}

impl From<ApiResponse> for Value {
	fn from(value: ApiResponse) -> Self {
		convert_public_value_to_internal(value.into_value())
	}
}
