//! WebSocket engine

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::api;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::err::Error;
use crate::api::Connect;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Status;
use crate::opt::IntoEndpoint;
use crate::sql::Array;
use crate::sql::Strand;
use crate::sql::Value;
use futures::Stream;
use serde::Deserialize;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Instant;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Interval;
#[cfg(target_arch = "wasm32")]
use wasmtimer::std::Instant;
#[cfg(target_arch = "wasm32")]
use wasmtimer::tokio::Interval;

pub(crate) const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const PING_METHOD: &str = "ping";

/// The WS scheme used to connect to `ws://` endpoints
#[derive(Debug)]
pub struct Ws;

/// The WSS scheme used to connect to `wss://` endpoints
#[derive(Debug)]
pub struct Wss;

/// A WebSocket client for communicating with the server via WebSockets
#[derive(Debug, Clone)]
pub struct Client {
	pub(crate) id: i64,
	method: Method,
}

impl Surreal<Client> {
	/// Connects to a specific database endpoint, saving the connection on the static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::ws::Client;
	/// use surrealdb::engine::remote::ws::Ws;
	///
	/// static DB: Surreal<Client> = Surreal::init();
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect::<Ws>("localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect<P>(
		&self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			router: Some(&self.router),
			address: address.into_endpoint(),
			capacity: 0,
			client: PhantomData,
			response_type: PhantomData,
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Failure {
	pub(crate) code: i64,
	pub(crate) message: String,
}

#[derive(Debug, Deserialize)]
pub(crate) enum Data {
	Other(Value),
	Query(Vec<QueryMethodResponse>),
}

type ServerResult = std::result::Result<Data, Failure>;

impl From<Failure> for Error {
	fn from(failure: Failure) -> Self {
		match failure.code {
			-32600 => Self::InvalidRequest(failure.message),
			-32602 => Self::InvalidParams(failure.message),
			-32603 => Self::InternalError(failure.message),
			-32700 => Self::ParseError(failure.message),
			_ => Self::Query(failure.message),
		}
	}
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueryMethodResponse {
	#[allow(dead_code)]
	time: String,
	status: Status,
	result: Value,
}

impl DbResponse {
	fn from(result: ServerResult) -> Result<Self> {
		match result.map_err(Error::from)? {
			Data::Other(value) => Ok(DbResponse::Other(value)),
			Data::Query(results) => Ok(DbResponse::Query(api::Response(
				results
					.into_iter()
					.map(|response| match response.status {
						Status::Ok => match response.result {
							Value::Array(Array(values)) => Ok(values),
							Value::None | Value::Null => Ok(vec![]),
							value => Ok(vec![value]),
						},
						Status::Err => match response.result {
							Value::Strand(Strand(message)) => Err(Error::Query(message).into()),
							message => Err(Error::Query(message.to_string()).into()),
						},
					})
					.enumerate()
					.collect(),
			))),
		}
	}
}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
	id: Option<Value>,
	pub(crate) result: ServerResult,
}

struct IntervalStream {
	inner: Interval,
}

impl IntervalStream {
	fn new(interval: Interval) -> Self {
		Self {
			inner: interval,
		}
	}
}

impl Stream for IntervalStream {
	type Item = Instant;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Instant>> {
		self.inner.poll_tick(cx).map(Some)
	}
}
