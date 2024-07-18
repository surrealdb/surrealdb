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
use crate::dbs::QueryMethodResponse;
use crate::dbs::Status;
use crate::method::Stats;
use crate::opt::IntoEndpoint;
use crate::sql::Value;
use bincode::Options as _;
use flume::Sender;
use indexmap::IndexMap;
use revision::revisioned;
use revision::Revisioned;
use serde::de::DeserializeOwned;
use serde::ser::SerializeMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Read;
use std::marker::PhantomData;
use std::time::Duration;
use surrealdb_core::dbs::Notification as CoreNotification;
use trice::Instant;
use uuid::Uuid;

pub(crate) const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const PING_METHOD: &str = "ping";
const REVISION_HEADER: &str = "revision";

/// A struct which will be serialized as a map to behave like the previously used BTreeMap.
///
/// This struct serializes as if it is a surrealdb_core::sql::Value::Object.
#[derive(Debug)]
struct RouterRequest {
	id: Option<Value>,
	method: Value,
	params: Option<Value>,
}

impl Serialize for RouterRequest {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		struct InnerRequest<'a>(&'a RouterRequest);

		impl Serialize for InnerRequest<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				let size = 1 + self.0.id.is_some() as usize + self.0.params.is_some() as usize;
				let mut map = serializer.serialize_map(Some(size))?;
				if let Some(id) = self.0.id.as_ref() {
					map.serialize_entry("id", id)?;
				}
				map.serialize_entry("method", &self.0.method)?;
				if let Some(params) = self.0.params.as_ref() {
					map.serialize_entry("params", params)?;
				}
				map.end()
			}
		}

		serializer.serialize_newtype_variant("Value", 9, "Object", &InnerRequest(self))
	}
}

impl Revisioned for RouterRequest {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		// version
		Revisioned::serialize_revisioned(&1u32, w)?;
		// object variant
		Revisioned::serialize_revisioned(&9u32, w)?;
		// object wrapper version
		Revisioned::serialize_revisioned(&1u32, w)?;

		let size = 1 + self.id.is_some() as usize + self.params.is_some() as usize;
		size.serialize_revisioned(w)?;

		let serializer = bincode::options()
			.with_no_limit()
			.with_little_endian()
			.with_varint_encoding()
			.reject_trailing_bytes();

		if let Some(x) = self.id.as_ref() {
			serializer
				.serialize_into(&mut *w, "id")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;
			x.serialize_revisioned(w)?;
		}
		serializer
			.serialize_into(&mut *w, "method")
			.map_err(|err| revision::Error::Serialize(err.to_string()))?;
		self.method.serialize_revisioned(w)?;

		if let Some(x) = self.params.as_ref() {
			serializer
				.serialize_into(&mut *w, "params")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;
			x.serialize_revisioned(w)?;
		}

		Ok(())
	}

	fn deserialize_revisioned<R: Read>(_: &mut R) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		panic!("deliberately unimplemented");
	}
}

struct RouterState<Sink, Stream, Msg> {
	var_stash: IndexMap<i64, (String, Value)>,
	/// Vars currently set by the set method,
	vars: IndexMap<String, Value>,
	/// Messages which aught to be replayed on a reconnect.
	replay: IndexMap<Method, Msg>,
	/// Pending live queries
	live_queries: HashMap<Uuid, channel::Sender<CoreNotification>>,

	routes: HashMap<i64, (Method, Sender<Result<DbResponse>>)>,

	last_activity: Instant,

	sink: Sink,
	stream: Stream,
}

impl<Sink, Stream, Msg> RouterState<Sink, Stream, Msg> {
	pub fn new(sink: Sink, stream: Stream) -> Self {
		RouterState {
			var_stash: IndexMap::new(),
			vars: IndexMap::new(),
			replay: IndexMap::new(),
			live_queries: HashMap::new(),
			routes: HashMap::new(),
			last_activity: Instant::now(),
			sink,
			stream,
		}
	}
}

enum HandleResult {
	/// Socket disconnected, should continue to reconnect
	Disconnected,
	/// Nothing wrong continue as normal.
	Ok,
}

/// The WS scheme used to connect to `ws://` endpoints
#[derive(Debug)]
pub struct Ws;

/// The WSS scheme used to connect to `wss://` endpoints
#[derive(Debug)]
pub struct Wss;

/// A WebSocket client for communicating with the server via WebSockets
#[derive(Debug, Clone)]
pub struct Client(());

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
			engine: PhantomData,
			address: address.into_endpoint(),
			capacity: 0,
			waiter: self.waiter.clone(),
			response_type: PhantomData,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Failure {
	pub(crate) code: i64,
	pub(crate) message: String,
}

#[revisioned(revision = 1)]
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
						_ => unreachable!(),
					}
				}

				Ok(DbResponse::Query(api::Response {
					results: map,
					..api::Response::new()
				}))
			}
			// Live notifications don't call this method
			Data::Live(..) => unreachable!(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
pub(crate) struct Response {
	id: Option<Value>,
	pub(crate) result: ServerResult,
}

fn serialize<V>(value: &V, revisioned: bool) -> Result<Vec<u8>>
where
	V: serde::Serialize + Revisioned,
{
	if revisioned {
		let mut buf = Vec::new();
		value.serialize_revisioned(&mut buf).map_err(|error| crate::Error::Db(error.into()))?;
		return Ok(buf);
	}
	crate::sql::serde::serialize(value).map_err(|error| crate::Error::Db(error.into()))
}

fn deserialize<A, T>(bytes: &mut A, revisioned: bool) -> Result<T>
where
	A: Read,
	T: Revisioned + DeserializeOwned,
{
	if revisioned {
		return T::deserialize_revisioned(bytes).map_err(|x| crate::Error::Db(x.into()));
	}
	let mut buf = Vec::new();
	bytes.read_to_end(&mut buf).map_err(crate::err::Error::Io)?;
	crate::sql::serde::deserialize(&buf).map_err(|error| crate::Error::Db(error.into()))
}

#[cfg(test)]
mod test {
	use std::io::Cursor;

	use revision::Revisioned;
	use surrealdb_core::sql::Value;

	use super::RouterRequest;

	fn assert_converts<S, D, I>(req: &RouterRequest, s: S, d: D)
	where
		S: FnOnce(&RouterRequest) -> I,
		D: FnOnce(I) -> Value,
	{
		let ser = s(req);
		let val = d(ser);
		let Value::Object(obj) = val else {
			panic!("not an object");
		};
		assert_eq!(obj.get("id").cloned(), req.id);
		assert_eq!(obj.get("method").unwrap().clone(), req.method);
		assert_eq!(obj.get("params").cloned(), req.params);
	}

	#[test]
	fn router_request_value_conversion() {
		let request = RouterRequest {
			id: Some(Value::from(1234i64)),
			method: Value::from("request"),
			params: Some(vec![Value::from(1234i64), Value::from("request")].into()),
		};

		println!("test convert bincode");

		assert_converts(
			&request,
			|i| bincode::serialize(i).unwrap(),
			|b| bincode::deserialize(&b).unwrap(),
		);

		println!("test convert json");

		assert_converts(
			&request,
			|i| serde_json::to_string(i).unwrap(),
			|b| serde_json::from_str(&b).unwrap(),
		);

		println!("test convert revisioned");

		assert_converts(
			&request,
			|i| {
				let mut buf = Vec::new();
				i.serialize_revisioned(&mut Cursor::new(&mut buf)).unwrap();
				buf
			},
			|b| Value::deserialize_revisioned(&mut Cursor::new(b)).unwrap(),
		);

		println!("done");
	}
}
