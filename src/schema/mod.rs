use crate::rpc::response::Failure;
use surrealdb::sql::Value;

pub mod typed_surrealql_v1;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Schema {
	TypedSurrealQLV1,
}

impl Schema {
	pub fn is_valid(self, v: &serde_json::Value) -> bool {
		match self {
			Schema::TypedSurrealQLV1 => typed_surrealql_v1::TYPED_SURREALQL_V1.validate(v).is_ok(),
		}
	}

	pub fn decode(self, v: Value) -> Result<Value, Failure> {
		match self {
			Schema::TypedSurrealQLV1 => match v {
				Value::Object(v) => typed_surrealql_v1::decode(v),
				_ => return Err(Failure::PARSE_ERROR),
			},
		}
	}

	pub fn encode(self, v: Value, root: bool) -> Result<Value, Failure> {
		match self {
			Schema::TypedSurrealQLV1 => typed_surrealql_v1::encode(v, root),
		}
	}
}
