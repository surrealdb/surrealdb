use crate::err::Error;
use crate::sql::value::Value;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Serialize;
use std::panic::UnwindSafe;
use std::time::Duration;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Response";

#[derive(Debug)]
pub enum QueryType {
	// Any kind of query
	Other,
	// Indicates that the response live query id must be tracked
	Live,
	// Indicates that the live query should be removed from tracking
	Kill,
}

/// The return value when running a query set on the database.
#[derive(Debug)]
pub struct Response {
	pub time: Duration,
	pub result: Result<Value, Error>,
	// Record the query type in case processing the response is necessary (such as tracking live queries).
	pub query_type: QueryType,
}

impl Response {
	/// Return the transaction duration as a string
	pub fn speed(&self) -> String {
		format!("{:?}", self.time)
	}
	/// Retrieve the response as a normal result
	pub fn output(self) -> Result<Value, Error> {
		self.result
	}
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
	Ok,
	Err,
}

impl Serialize for Response {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let mut val = serializer.serialize_struct(TOKEN, 3)?;
		val.serialize_field("time", &self.time)?;
		match &self.result {
			Ok(v) => {
				val.serialize_field("status", &Status::Ok)?;
				val.serialize_field("result", v)?;
			}
			Err(e) => {
				val.serialize_field("status", &Status::Err)?;
				val.serialize_field("result", &Value::from(e.to_string()))?;
			}
		}
		val.end()
	}
}

// User facing response sent to WS and HTTP connections directly
#[derive(Serialize)]
pub struct ApiResponse {
	time: String,
	status: Status,
	result: Value,
}

impl From<Response> for ApiResponse {
	fn from(r: Response) -> Self {
		let time = r.speed();
		let (status, result) = match r.result {
			Ok(value) => (Status::Ok, value),
			Err(error) => (Status::Err, error.to_string().into()),
		};
		Self {
			time,
			status,
			result,
		}
	}
}
