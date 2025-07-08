use std::borrow::Cow;
use std::time::Duration;

use crate::dbs::Notification;
use crate::expr::Value;
use crate::protocol::ToFlatbuffers;

use crate::rpc::RpcError;
use crate::rpc::V1Value;
use anyhow::Context;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use revision::{Revisioned, revisioned};
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::fmt;
use surrealdb_protocol::proto::prost_types;
use surrealdb_protocol::proto::rpc::v1;

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
			values: Ok(value.into_vec()),
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
	pub code: i64,
	pub message: Cow<'static, str>,
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

	pub const PARSE_ERROR: Failure = Failure {
		code: -32700,
		message: Cow::Borrowed("Parse error"),
	};

	pub const INVALID_REQUEST: Failure = Failure {
		code: -32600,
		message: Cow::Borrowed("Invalid Request"),
	};

	pub const METHOD_NOT_FOUND: Failure = Failure {
		code: -32601,
		message: Cow::Borrowed("Method not found"),
	};

	pub const INVALID_PARAMS: Failure = Failure {
		code: -32602,
		message: Cow::Borrowed("Invalid params"),
	};

	pub const INTERNAL_ERROR: Failure = Failure {
		code: -32603,
		message: Cow::Borrowed("Internal error"),
	};
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

impl From<Failure> for RpcError {
	fn from(err: Failure) -> Self {
		match err.code {
			-32700 => RpcError::ParseError,
			-32600 => RpcError::InvalidRequest(err.message.to_string()),
			-32601 => RpcError::MethodNotFound,
			-32602 => RpcError::InvalidParams,
			other => {
				RpcError::InternalError(anyhow!("Error code: {}, message: {}", other, err.message))
			}
		}
	}
}

impl From<Failure> for V1Value {
	fn from(err: Failure) -> Self {
		map! {
			String::from("code") => V1Value::from(err.code),
			String::from("message") => V1Value::from(err.message.to_string()),
		}
		.into()
	}
}

impl Revisioned for Failure {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		#[revisioned(revision = 1)]
		#[derive(Clone, Debug, Serialize)]
		struct Inner {
			code: i64,
			message: String,
		}

		let inner = Inner {
			code: self.code,
			message: self.message.as_ref().to_owned(),
		};
		inner.serialize_revisioned(writer)
	}

	fn deserialize_revisioned<R: std::io::Read>(_reader: &mut R) -> Result<Self, revision::Error> {
		unreachable!("deserialization not supported for this type")
	}

	fn revision() -> u16 {
		1
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
	// pub index: u32,
	pub stats: QueryStats,
	pub values: Result<Vec<Value>, Failure>,
}

impl QueryResult {
	pub fn ok(value: Value) -> Self {
		Self {
			stats: QueryStats::default(),
			values: Ok(vec![value]),
		}
	}

	pub fn err(err: Failure) -> Self {
		Self {
			stats: QueryStats::default(),
			values: Err(err),
		}
	}

	pub fn new_from_value(value: Value) -> Self {
		Self {
			stats: QueryStats::default(),
			values: Ok(vec![value]),
		}
	}
}

impl Default for QueryResult {
	fn default() -> Self {
		Self {
			stats: QueryStats::default(),
			values: Ok(vec![]),
		}
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueryStats {
	pub start_time: DateTime<Utc>,
	pub execution_duration: Duration,
	pub num_records: i64,
}

impl QueryStats {
	pub fn from_start_time(start_time: DateTime<Utc>) -> Self {
		Self {
			num_records: 0,
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
	pub fn output(self) -> Result<Vec<Value>, Failure> {
		self.values
	}
}

impl TryFrom<v1::QueryStats> for QueryStats {
	type Error = anyhow::Error;

	fn try_from(stats: v1::QueryStats) -> Result<Self, Self::Error> {
		let start_time = stats.start_time.context("start_time is required")?;
		let execution_duration =
			stats.execution_duration.context("execution_duration is required")?;
		Ok(Self {
			start_time: DateTime::from_timestamp(start_time.seconds, start_time.nanos as u32)
				.context("failed to parse start_time")?,
			execution_duration: Duration::from_nanos(
				execution_duration.seconds as u64 * 1_000_000_000 + execution_duration.nanos as u64,
			),
			num_records: stats.records_returned,
		})
	}
}

impl From<QueryStats> for v1::QueryStats {
	fn from(stats: QueryStats) -> Self {
		Self {
			start_time: Some(prost_types::Timestamp {
				seconds: stats.start_time.timestamp() as i64,
				nanos: stats.start_time.timestamp_subsec_nanos() as i32,
			}),
			execution_duration: Some(prost_types::Duration {
				seconds: stats.execution_duration.as_secs() as i64,
				nanos: stats.execution_duration.subsec_nanos() as i32,
			}),
			records_returned: stats.num_records,
			bytes_returned: -1,
			records_scanned: -1,
			bytes_scanned: -1,
		}
	}
}
