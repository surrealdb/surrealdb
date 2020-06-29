use crate::sql::statement::Statement;
use serde::{Deserialize, Serialize};

// pub type Output = std::result::Result<warp::reply::Json, ErrorResponse>;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Responses(pub Vec<Response>);

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Response {
	pub sql: String,
	pub time: String,
	pub status: String,
	pub detail: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub result: Option<String>,
}
