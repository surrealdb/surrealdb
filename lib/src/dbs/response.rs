use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::Object;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::time::Duration;

/// The return value when running a query set on the database.
#[derive(Debug)]
pub struct Response {
	pub sql: Option<String>,
	pub time: Duration,
	pub result: Result<Value, Error>,
}

impl Response {
	/// Return the transaction duration as a string
	pub fn speed(&self) -> String {
		format!("{:?}", self.time)
	}
	/// Retrieve the response as a result by reference
	pub fn output(&self) -> Result<&Value, &Error> {
		match &self.result {
			Ok(v) => Ok(v),
			Err(e) => Err(e),
		}
	}
}

impl From<Response> for Value {
	fn from(v: Response) -> Value {
		// Get the response speed
		let time = v.speed();
		// Get the response status
		let status = v.output().map_or_else(|_| "ERR", |_| "OK");
		// Convert the response
		match v.result {
			Ok(val) => match v.sql {
				Some(sql) => Value::Object(Object(map! {
					String::from("sql") => sql.into(),
					String::from("time") => time.into(),
					String::from("status") => status.into(),
					String::from("result") => val,
				})),
				None => Value::Object(Object(map! {
					String::from("time") => time.into(),
					String::from("status") => status.into(),
					String::from("result") => val,
				})),
			},
			Err(err) => match v.sql {
				Some(sql) => Value::Object(Object(map! {
					String::from("sql") => sql.into(),
					String::from("time") => time.into(),
					String::from("status") => status.into(),
					String::from("detail") => err.to_string().into(),
				})),
				None => Value::Object(Object(map! {
					String::from("time") => time.into(),
					String::from("status") => status.into(),
					String::from("detail") => err.to_string().into(),
				})),
			},
		}
	}
}

impl Serialize for Response {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match &self.result {
			Ok(v) => match &self.sql {
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
