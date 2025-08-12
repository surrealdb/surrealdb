use std::fmt;
use std::time::Duration;

use anyhow::Result;
use revision::{Revisioned, revisioned};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

use crate::expr::TopLevelExpr;
use crate::val::{Object, Strand, Value};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Response";

#[revisioned(revision = 1)]
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryType {
	// Any kind of query
	#[default]
	Other,
	// Indicates that the response live query id must be tracked
	Live,
	// Indicates that the live query should be removed from tracking
	Kill,
}

impl fmt::Display for QueryType {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			QueryType::Other => "other".fmt(f),
			QueryType::Live => "live".fmt(f),
			QueryType::Kill => "kill".fmt(f),
		}
	}
}

impl QueryType {
	/// Returns the query type for the given toplevel expression.
	pub fn for_toplevel_expr(expr: &TopLevelExpr) -> Self {
		match expr {
			TopLevelExpr::Live(_) => QueryType::Live,
			TopLevelExpr::Kill(_) => QueryType::Kill,
			_ => QueryType::Other,
		}
	}
}

/// The return value when running a query set on the database.
#[derive(Debug)]
pub struct Response {
	pub time: Duration,
	pub result: Result<Value>,
	// Record the query type in case processing the response is necessary (such as tracking live
	// queries).
	pub query_type: QueryType,
}

impl Response {
	/// Retrieve the response as a normal result
	pub fn output(self) -> Result<Value> {
		self.result
	}

	/// Convert's the response into a value as it is send across the net.
	pub fn into_value(self) -> Value {
		let mut res = Object::new();
		res.insert("time".to_owned(), Strand::new(format!("{:?}", self.time)).unwrap().into());

		if !matches!(self.query_type, QueryType::Other) {
			res.insert("type".to_owned(), Strand::new(self.query_type.to_string()).unwrap().into());
		}

		match self.result {
			Ok(v) => {
				res.insert("status".to_owned(), strand!("OK").to_owned().into());
				res.insert("result".to_owned(), v);
			}
			Err(e) => {
				res.insert("status".to_owned(), strand!("ERR").to_owned().into());
				res.insert("result".to_owned(), Strand::new(e.to_string()).unwrap().into());
			}
		}

		res.into()
	}
}

#[revisioned(revision = 1)]
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
		let includes_type = !matches!(self.query_type, QueryType::Other);
		let mut val = serializer.serialize_struct(
			TOKEN,
			if includes_type {
				3
			} else {
				4
			},
		)?;

		val.serialize_field("time", &format!("{:?}", self.time))?;
		if includes_type {
			val.serialize_field("type", &self.query_type)?;
		}

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
pub struct QueryMethodResponse {
	pub time: String,
	pub status: Status,
	pub result: Value,
}

impl From<&Response> for QueryMethodResponse {
	fn from(res: &Response) -> Self {
		let time = format!("{:?}", res.time);
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

impl Revisioned for Response {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		QueryMethodResponse::from(self).serialize_revisioned(writer)
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(_reader: &mut R) -> Result<Self, revision::Error> {
		unreachable!("deserialising `Response` directly is not supported")
	}

	fn revision() -> u16 {
		1
	}
}
