use crate::err::Error;
use crate::sql::value::Value;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::time::Duration;

pub type Responses = Vec<Response>;

#[derive(Debug)]
pub struct Response {
	pub sql: Option<String>,
	pub time: Duration,
	pub result: Result<Value, Error>,
}

impl Response {
	// Return the transaction speed
	pub fn speed(&self) -> String {
		format!("{:?}", self.time)
	}
	// Retrieve the response as a result
	pub fn output(&self) -> Result<&Value, &Error> {
		match &self.result {
			Ok(v) => Ok(v),
			Err(e) => Err(e),
		}
	}
}

impl Serialize for Response {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match &self.result {
			Ok(v) => match v {
				Value::None => match &self.sql {
					Some(s) => {
						let mut val = serializer.serialize_struct("Response", 3)?;
						val.serialize_field("sql", s.as_str())?;
						val.serialize_field("time", self.speed().as_str())?;
						val.serialize_field("status", "OK")?;
						val.end()
					}
					None => {
						let mut val = serializer.serialize_struct("Response", 2)?;
						val.serialize_field("time", self.speed().as_str())?;
						val.serialize_field("status", "OK")?;
						val.end()
					}
				},
				v => match &self.sql {
					Some(s) => {
						let mut val = serializer.serialize_struct("Response", 4)?;
						val.serialize_field("sql", s.as_str())?;
						val.serialize_field("time", self.speed().as_str())?;
						val.serialize_field("status", "OK")?;
						val.serialize_field("result", v)?;
						val.end()
					}
					None => {
						let mut val = serializer.serialize_struct("Response", 3)?;
						val.serialize_field("time", self.speed().as_str())?;
						val.serialize_field("status", "OK")?;
						val.serialize_field("result", v)?;
						val.end()
					}
				},
			},
			Err(e) => match &self.sql {
				Some(s) => {
					let mut val = serializer.serialize_struct("Response", 4)?;
					val.serialize_field("sql", s.as_str())?;
					val.serialize_field("time", self.speed().as_str())?;
					val.serialize_field("status", "ERR")?;
					val.serialize_field("detail", e)?;
					val.end()
				}
				None => {
					let mut val = serializer.serialize_struct("Response", 3)?;
					val.serialize_field("time", self.speed().as_str())?;
					val.serialize_field("status", "ERR")?;
					val.serialize_field("detail", e)?;
					val.end()
				}
			},
		}
	}
}
