use crate::dbs::Notification;
use crate::val::Value;
use crate::{dbs, expr};
use anyhow::Result;
use revision::revisioned;
use serde::Serialize;

/// The data returned by the database
// The variants here should be in exactly the same order as `crate::engine::remote::ws::Data`
// In future, they will possibly be merged to avoid having to keep them in sync.
#[revisioned(revision = 1)]
#[derive(Debug, Serialize)]
pub enum Data {
	/// Generally methods return a `expr::Value`
	Other(Value),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<dbs::Response>),
	/// Live queries return a notification
	Live(Notification),
	// Add new variants here
}

impl Data {
	pub fn into_value(self) -> Result<Value> {
		match self {
			Data::Query(v) => expr::to_value(v),
			Data::Live(v) => expr::to_value(v),
			Data::Other(v) => Ok(v),
		}
	}
}
