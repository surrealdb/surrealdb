use crate::sql::literal::Literal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Responses(pub Vec<Response>);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Response {
	pub sql: String,
	pub time: String,
	pub status: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	// pub result: Option<String>,
	pub result: Option<Literal>,
}
