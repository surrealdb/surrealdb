use std::borrow::Cow;
use std::time::Duration;

use crate::dbs::Notification;
use crate::expr::Value;
use crate::protocol::ToFlatbuffers;
use crate::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;
use crate::rpc::RpcError;
use crate::sql::statement::Statement;
use chrono::DateTime;
use chrono::Utc;
use revision::Revisioned;
use revision::revisioned;
use serde::Deserialize;
use serde::Serialize;
use serde::ser::SerializeStruct;
use std::error::Error;
use std::fmt;

/// The data returned from a query execution.
#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseData {
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Results(Vec<QueryResult>),
	/// Live queries return a notification
	Notification(Notification),
}

impl ResponseData {
	pub fn new_from_value(value: Value) -> Self {
		Self::Results(vec![QueryResult {
			stats: QueryStats::default(),
			result: Ok(value),
		}])
	}
}

impl From<Vec<QueryResult>> for ResponseData {
	fn from(results: Vec<QueryResult>) -> Self {
		Self::Results(results)
	}
}

impl From<Notification> for ResponseData {
	fn from(notification: Notification) -> Self {
		Self::Notification(notification)
	}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Failure {
	pub(crate) code: i64,
	pub(crate) message: Cow<'static, str>,
}

impl Failure {
	pub fn new(code: i64, message: impl Into<Cow<'static, str>>) -> Self {
		Self {
			code,
			message: message.into(),
		}
	}

	pub fn code(&self) -> i64 {
		self.code
	}

	pub fn message(&self) -> &str {
		&self.message
	}

	// TODO: STU: Copy over the error codes from src/rpc/failure.rs

	pub fn query_cancelled() -> Self {
		Self::new(1000, "Query cancelled")
	}
	pub fn query_timeout() -> Self {
		Self::new(1001, "Query timed out")
	}
	pub fn query_not_executed(message: impl Into<Cow<'static, str>>) -> Self {
		Self::new(1002, message.into())
	}

	pub fn execution_failed(message: impl Into<Cow<'static, str>>) -> Self {
		Self::new(1002, message.into())
	}

	pub fn invalid_control_flow() -> Self {
		Self::new(1003, "Invalid control flow")
	}

	pub fn method_not_found() -> Self {
		Self::new(1004, "Method not found")
	}

	pub fn custom(message: impl Into<Cow<'static, str>>) -> Self {
		Self::new(-32000, message.into())
	}
}

impl Error for Failure {}

impl fmt::Display for Failure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Failure ({}): {}", self.code, self.message)
	}
}

impl From<RpcError> for Failure {
	fn from(err: RpcError) -> Self {
		Self {
			code: match err {
				RpcError::ParseError => -32700,
				RpcError::InvalidRequest(_) => -32600,
				RpcError::MethodNotFound => -32601,
				RpcError::InvalidParams => -32602,
				RpcError::InternalError(_) => -32603,
				RpcError::Thrown(_) => 1002, // Custom error code for thrown errors
				_ => 1002,                   // Default custom error code
			},
			message: Cow::Owned(err.to_string()),
		}
	}
}

impl ToFlatbuffers for Failure {
	type Output<'bldr> = flatbuffers::WIPOffset<rpc_fb::QueryResultError<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let message = builder.create_string(&self.message);
		rpc_fb::QueryResultError::create(
			builder,
			&rpc_fb::QueryResultErrorArgs {
				code: self.code,
				message: Some(message),
			},
		)
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
	// pub index: u32,
	pub stats: QueryStats,
	pub result: Result<Value, Failure>,
}

impl QueryResult {
	pub fn ok(value: Value) -> Self {
		Self {
			stats: QueryStats::default(),
			result: Ok(value),
		}
	}

	pub fn err(err: Failure) -> Self {
		Self {
			stats: QueryStats::default(),
			result: Err(err),
		}
	}

	pub fn new_from_value(value: Value) -> Self {
		Self {
			stats: QueryStats::default(),
			result: Ok(value),
		}
	}
}

impl Default for QueryResult {
	fn default() -> Self {
		Self {
			stats: QueryStats::default(),
			result: Ok(Value::default()),
		}
	}
}

impl ToFlatbuffers for QueryResult {
	type Output<'bldr> = flatbuffers::WIPOffset<rpc_fb::QueryResult<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let (result_type, result) = match &self.result {
			Ok(value) => {
				(rpc_fb::QueryResultData::Data, Some(value.to_fb(builder).as_union_value()))
			}
			Err(err) => (rpc_fb::QueryResultData::Error, Some(err.to_fb(builder).as_union_value())),
		};
		let stats = self.stats.to_fb(builder);

		rpc_fb::QueryResult::create(
			builder,
			&rpc_fb::QueryResultArgs {
				stats: Some(stats),
				result_type,
				result,
			},
		)
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueryStats {
	pub start_time: DateTime<Utc>,
	pub execution_duration: Duration,
}

impl QueryStats {
	pub fn from_start_time(start_time: DateTime<Utc>) -> Self {
		Self {
			execution_duration: Utc::now()
				.signed_duration_since(&start_time)
				.to_std()
				.expect("Duration should not be negative"),
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
	pub fn output(self) -> Result<Value, Failure> {
		self.result
	}
}

impl ToFlatbuffers for QueryStats {
	type Output<'bldr> = flatbuffers::WIPOffset<rpc_fb::QueryStats<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let start_time = self.start_time.to_fb(builder);
		let execution_duration = self.execution_duration.to_fb(builder);
		rpc_fb::QueryStats::create(
			builder,
			&rpc_fb::QueryStatsArgs {
				start_time: Some(start_time),
				execution_duration: Some(execution_duration),
			},
		)
	}
}
