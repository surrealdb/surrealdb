use revision::revisioned;
use serde::Serialize;

use crate::dbs;
use crate::dbs::{Notification, executor::convert_value_to_public_value};
use crate::map;
use surrealdb_types::Value;

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
			Data::Query(v) => {
				let converted: Vec<Value> = v.into_iter().map(|x| x.into_value()).collect();
				Value::Array(surrealdb_types::Array::from_values(converted))
			},
			Data::Live(v) => Value::from(surrealdb_types::Object::from_map(map! {
				"id".to_owned() => Value::Uuid(surrealdb_types::Uuid(v.id.0)),
				"action".to_owned() => Value::String(v.action.to_string()),
				"record".to_owned() => convert_value_to_public_value(v.record).unwrap_or(Value::Null),
				"result".to_owned() => convert_value_to_public_value(v.result).unwrap_or(Value::Null),

			})),
			Data::Other(v) => v,
		}
	}
}
