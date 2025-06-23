use crate::dbs::Notification;
use crate::expr::Value;
use crate::sql::statement::Statement;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use revision::Revisioned;
use revision::revisioned;
use serde::Deserialize;
use serde::Serialize;
use serde::ser::SerializeStruct;
use std::time::Duration;


/// The data returned from a query execution.
#[derive(Debug)]
pub enum QueryResultData {
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Results(Vec<QueryResult>),
	/// Live queries return a notification
	Notification(Notification),
}

impl QueryResultData {
	pub fn new_from_value(value: Value) -> Self {
		Self::Results(vec![QueryResult {
			stats: QueryStats::default(),
			result: Ok(value),
		}])
	}
}


#[derive(Debug)]
pub struct QueryResult {
	// pub index: u32,
	pub stats: QueryStats,
	pub result: Result<Value>,
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct QueryStats {
	pub start_time: DateTime<Utc>,
	pub execution_duration: Duration,
}

impl QueryStats {
	pub fn from_start_time(start_time: DateTime<Utc>) -> Self {
		Self {
			execution_duration: Utc::now().signed_duration_since(&start_time).to_std().expect("Duration should not be negative"),
			start_time,
		}
	}
}

impl QueryResult {
	/// Return the transaction duration as a string
	pub fn speed(&self) -> String {
		format!("{:?}", self.stats.execution_duration)
	}

	/// Retrieve the response as a normal result
	pub fn output(self) -> Result<Value> {
		self.result
	}
}
