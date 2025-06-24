use crate::expr::{Expr, TopLevelExpr};
use crate::val::Value as CoreValue;
use anyhow::Result;
use revision::{Revisioned, revisioned};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Response";

#[revisioned(revision = 1)]
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum QueryType {
	// Any kind of query
	#[default]
	Other,
	// Indicates that the response live query id must be tracked
	Live,
	// Indicates that the live query should be removed from tracking
	Kill,
}

impl QueryType {
	/// Returns if this query type is not live nor kill
	fn is_other(&self) -> bool {
		matches!(self, Self::Other)
	}

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
#[non_exhaustive]
pub struct Response {
	pub time: Duration,
	pub result: Result<CoreValue>,
	// Record the query type in case processing the response is necessary (such as tracking live queries).
	pub query_type: QueryType,
}

impl Response {
	/// Return the transaction duration as a string
	pub fn speed(&self) -> String {
		format!("{:?}", self.time)
	}

	/// Retrieve the response as a normal result
	pub fn output(self) -> Result<CoreValue> {
		self.result
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
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
		let includes_type = !self.query_type.is_other();
		let mut val = serializer.serialize_struct(
			TOKEN,
			if includes_type {
				3
			} else {
				4
			},
		)?;

		val.serialize_field("time", self.speed().as_str())?;
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
				val.serialize_field("result", &CoreValue::from(e.to_string()))?;
			}
		}
		val.end()
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueryMethodResponse {
	pub time: String,
	pub status: Status,
	pub result: CoreValue,
}

impl From<&Response> for QueryMethodResponse {
	fn from(res: &Response) -> Self {
		let time = res.speed();
		let (status, result) = match &res.result {
			Ok(value) => (Status::Ok, value.clone()),
			Err(error) => (Status::Err, CoreValue::from(error.to_string())),
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
