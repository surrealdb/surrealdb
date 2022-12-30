//! WebSocket engine

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::engines::remote::Status;
use crate::api::err::Error;
use crate::api::Connect;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use crate::opt::IntoEndpoint;
use crate::sql::Array;
use crate::sql::Value;
use serde::Deserialize;
use std::marker::PhantomData;
use std::mem;
use std::time::Duration;

pub(crate) const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const PING_METHOD: &str = "ping";
const LOG: &str = "surrealdb::engines::remote::ws";

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
	/// use surrealdb::engines::remote::ws::Client;
	/// use surrealdb::engines::remote::ws::Ws;
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
		&'static self,
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
#[serde(untagged)]
pub(crate) enum QueryMethodResponse {
	Value(Value),
	String(String),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum SuccessValue {
	Query(Vec<(String, Status, QueryMethodResponse)>),
	Other(Value),
}

#[derive(Debug, Deserialize)]
pub(crate) enum Content {
	#[serde(rename = "result")]
	Success(SuccessValue),
	#[serde(rename = "error")]
	Failure(Failure),
}

impl DbResponse {
	fn from((method, content): (Method, Content)) -> Result<Self> {
		match content {
			Content::Success(SuccessValue::Query(results)) => Ok(DbResponse::Query(QueryResponse(
				results
					.into_iter()
					.map(|(_duration, status, result)| match status {
						Status::Ok => match result {
							QueryMethodResponse::Value(value) => match value {
								Value::Array(Array(values)) => Ok(values),
								Value::None | Value::Null => Ok(vec![]),
								value => Ok(vec![value]),
							},
							QueryMethodResponse::String(string) => Ok(vec![string.into()]),
						},
						Status::Err => match result {
							QueryMethodResponse::Value(message) => {
								Err(Error::Query(message.to_string()).into())
							}
							QueryMethodResponse::String(message) => {
								Err(Error::Query(message).into())
							}
						},
					})
					.enumerate()
					.collect(),
			))),
			Content::Success(SuccessValue::Other(mut value)) => {
				if let Method::Create | Method::Delete = method {
					if let Value::Array(Array(array)) = &mut value {
						match &mut array[..] {
							[] => {
								value = Value::None;
							}
							[v] => {
								value = mem::take(v);
							}
							_ => {}
						}
					}
				}
				Ok(DbResponse::Other(value))
			}
			Content::Failure(failure) => Err(Error::from(failure).into()),
		}
	}
}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
	#[serde(skip_serializing_if = "Option::is_none")]
	id: Option<Value>,
	#[serde(flatten)]
	pub(crate) content: Content,
}
