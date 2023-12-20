//! WebSocket engine

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::api;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::engine::remote::duration_from_str;
use crate::api::err::Error;
use crate::api::method::query::QueryResult;
use crate::api::Connect;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Notification;
use crate::dbs::Status;
use crate::method::Stats;
use crate::opt::IntoEndpoint;
use crate::sql::Value;
use indexmap::IndexMap;
use serde::Deserialize;
use std::marker::PhantomData;
use std::time::Duration;

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
	/// use once_cell::sync::Lazy;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::ws::Client;
	/// use surrealdb::engine::remote::ws::Ws;
	///
	/// static DB: Lazy<Surreal<Client>> = Lazy::new(Surreal::init);
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
			router: self.router.clone(),
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
	Live(Notification),
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
	time: String,
	status: Status,
	result: Value,
}

impl DbResponse {
	fn from(result: ServerResult) -> Result<Self> {
		match result.map_err(Error::from)? {
			Data::Other(value) => Ok(DbResponse::Other(value)),
			Data::Query(responses) => {
				let mut map =
					IndexMap::<usize, (Stats, QueryResult)>::with_capacity(responses.len());

				for (index, response) in responses.into_iter().enumerate() {
					let stats = Stats {
						execution_time: duration_from_str(&response.time),
					};
					match response.status {
						Status::Ok => {
							map.insert(index, (stats, Ok(response.result)));
						}
						Status::Err => {
							map.insert(
								index,
								(stats, Err(Error::Query(response.result.as_raw_string()).into())),
							);
						}
					}
				}

				Ok(DbResponse::Query(api::Response(map)))
			}
			// Live notifications don't call this method
			Data::Live(..) => unreachable!(),
		}
	}
}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
	id: Option<Value>,
	pub(crate) result: ServerResult,
}
