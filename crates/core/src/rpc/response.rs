use revision::revisioned;
use serde::Serialize;

use crate::dbs::Notification;
use crate::sql::Value;
use crate::{dbs, sql};

/// The data returned by the database
// The variants here should be in exactly the same order as `crate::engine::remote::ws::Data`
// In future, they will possibly be merged to avoid having to keep them in sync.
#[revisioned(revision = 1)]
#[derive(Debug, Serialize)]
#[non_exhaustive]
pub enum Data {
	/// Generally methods return a `sql::Value`
	Other(Value),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<dbs::Response>),
	/// Live queries return a notification
	Live(Notification),
	// Add new variants here
}

impl From<Value> for Data {
	fn from(v: Value) -> Self {
		Data::Other(v)
	}
}

impl From<String> for Data {
	fn from(v: String) -> Self {
		Data::Other(Value::from(v))
	}
}

impl From<Notification> for Data {
	fn from(n: Notification) -> Self {
		Data::Live(n)
	}
}

impl From<Vec<dbs::Response>> for Data {
	fn from(v: Vec<dbs::Response>) -> Self {
		Data::Query(v)
	}
}

impl TryFrom<Data> for Value {
	type Error = crate::err::Error;

	fn try_from(val: Data) -> Result<Self, Self::Error> {
		match val {
			Data::Query(v) => sql::to_value(v),
			Data::Live(v) => sql::to_value(v),
			Data::Other(v) => Ok(v),
		}
	}
}
