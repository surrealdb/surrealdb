use crate::sql::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
	Ok,
	Err,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Responses(pub Vec<Response>);

impl Responses {
	pub fn first(mut self) -> Response {
		self.0.remove(0)
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Response {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sql: Option<String>,
	pub time: String,
	pub status: Status,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub detail: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub result: Option<Value>,
}

impl Response {
	// Check if response succeeded
	pub fn is_ok(&self) -> bool {
		match self.status {
			Status::Ok => true,
			Status::Err => false,
		}
	}
	// Check if response failed
	pub fn is_err(&self) -> bool {
		match self.status {
			Status::Ok => false,
			Status::Err => true,
		}
	}
	// Retrieve the response as a result
	pub fn output(self) -> Result<Value, String> {
		match self.status {
			Status::Ok => Ok(self.result.unwrap()),
			Status::Err => Err(self.detail.unwrap()),
		}
	}
}
