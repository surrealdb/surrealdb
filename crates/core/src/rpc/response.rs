use revision::revisioned;
use serde::Serialize;

use crate::dbs;
use crate::dbs::Notification;
use crate::val::{Object, Strand, Value};

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
	pub fn into_value(self) -> Value {
		match self {
			Data::Query(v) => v.into_iter().map(|x| x.into_value()).collect(),
			Data::Live(v) => Value::from(Object(map! {
				"id".to_owned() => v.id.into(),
				"action".to_owned() => Strand::new(v.action.to_string()).unwrap().into(),
				"record".to_owned() => v.record,
				"result".to_owned() => v.result,

			})),
			Data::Other(v) => v,
		}
	}
}
