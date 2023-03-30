use crate::err::Error;
use crate::sql::value::Value;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::time::Duration;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Response";

/// The return value when running a query set on the database.
#[derive(Debug)]
pub struct Response {
	pub time: Duration,
	pub result: Result<Value, Error>,
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

impl Serialize for Response {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match &self.result {
			Ok(v) => {
				let mut val = serializer.serialize_struct(TOKEN, 3)?;
				val.serialize_field("time", self.speed().as_str())?;
				val.serialize_field("status", "OK")?;
				val.serialize_field("result", v)?;
				val.end()
			}
			Err(e) => {
				let mut val = serializer.serialize_struct(TOKEN, 3)?;
				val.serialize_field("time", self.speed().as_str())?;
				val.serialize_field("status", "ERR")?;
				val.serialize_field("detail", e)?;
				val.end()
			}
		}
	}
}
