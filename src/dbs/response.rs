use crate::sql::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Responses(pub Vec<Response>);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Response {
	pub time: String,
	pub status: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub detail: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub result: Option<Value>,
}
