use crate::err::Error;
use crate::sql::value::Value;
use revision::revisioned;
use revision::Revisioned;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Response";

#[derive(Debug)]
#[non_exhaustive]
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
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[doc(hidden)]
#[non_exhaustive]
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
		val.serialize_field("time", self.speed().as_str())?;
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

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
#[doc(hidden)]
#[non_exhaustive]
pub struct QueryMethodResponse {
	pub time: String,
	pub status: Status,
	pub result: Value,
}

impl From<&Response> for QueryMethodResponse {
	fn from(res: &Response) -> Self {
		let time = res.speed();
		let (status, result) = match &res.result {
			Ok(value) => (Status::Ok, value.clone()),
			Err(error) => (Status::Err, Value::from(error.to_string())),
		};
		Self {
			status,
			result,
			time,
		}
	}
}

#[doc(hidden)]
impl Revisioned for Response {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> std::result::Result<(), revision::Error> {
		QueryMethodResponse::from(self).serialize_revisioned(writer)
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(
		_reader: &mut R,
	) -> std::result::Result<Self, revision::Error> {
		unreachable!("deserialising `Response` directly is not supported")
	}

	fn revision() -> u16 {
		1
	}
}
