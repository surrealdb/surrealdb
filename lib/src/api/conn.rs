use crate::api;
use crate::api::err::Error;
use crate::api::method::query::Response;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Notification;
use crate::sql::Query;
use crate::Object;
use crate::Value;
use flume::Receiver;
use flume::Sender;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub(crate) struct Route {
	pub(crate) request: (i64, Method, Param),
	pub(crate) response: Sender<Result<DbResponse>>,
}

/// Message router
#[derive(Debug)]
pub struct Router {
	pub(crate) sender: Sender<Route>,
	pub(crate) last_id: AtomicI64,
	pub(crate) features: HashSet<ExtraFeatures>,
}

impl Router {
	pub(crate) fn next_id(&self) -> i64 {
		self.last_id.fetch_add(1, Ordering::SeqCst)
	}
}

/// The query method
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Method {
	/// Sends an authentication token to the server
	Authenticate,
	/// Performs a merge update operation
	Merge,
	/// Creates a record in a table
	Create,
	/// Deletes a record from a table
	Delete,
	/// Exports a database
	Export,
	/// Checks the health of the server
	Health,
	/// Imports a database
	Import,
	/// Invalidates a session
	Invalidate,
	/// Inserts a record or records into a table
	Insert,
	/// Kills a live query
	#[doc(hidden)] // Not supported yet
	Kill,
	/// Starts a live query
	#[doc(hidden)] // Not supported yet
	Live,
	/// Performs a patch update operation
	Patch,
	/// Sends a raw query to the database
	Query,
	/// Selects a record or records from a table
	Select,
	/// Sets a parameter on the connection
	Set,
	/// Signs into the server
	Signin,
	/// Signs up on the server
	Signup,
	/// Removes a parameter from a connection
	Unset,
	/// Performs an update operation
	Update,
	/// Performs an upsert operation
	Upsert,
	/// Selects a namespace and database to use
	Use,
	/// Queries the version of the server
	Version,
}

/// The database response sent from the router to the caller
#[derive(Debug)]
pub enum DbResponse {
	/// The response sent for the `query` method
	Query(Response),
	/// The response sent for any method except `query`
	Other(Value),
}

#[derive(Debug)]
#[allow(dead_code)] // used by ML model import and export functions
pub(crate) enum MlConfig {
	Import,
	Export {
		name: String,
		version: String,
	},
}

/// Holds the parameters given to the caller
#[derive(Debug, Default)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct Param {
	pub(crate) query: Option<(Query, Object)>,
	pub(crate) other: Vec<Value>,
	pub(crate) file: Option<PathBuf>,
	pub(crate) bytes_sender: Option<channel::Sender<Result<Vec<u8>>>>,
	pub(crate) notification_sender: Option<channel::Sender<Notification>>,
	pub(crate) ml_config: Option<MlConfig>,
}

impl Param {
	pub(crate) fn new(other: Vec<Value>) -> Self {
		Self {
			other,
			..Default::default()
		}
	}

	pub(crate) fn query(query: Query, bindings: Object) -> Self {
		Self {
			query: Some((query, bindings)),
			..Default::default()
		}
	}

	pub(crate) fn file(file: PathBuf) -> Self {
		Self {
			file: Some(file),
			..Default::default()
		}
	}

	pub(crate) fn bytes_sender(send: channel::Sender<Result<Vec<u8>>>) -> Self {
		Self {
			bytes_sender: Some(send),
			..Default::default()
		}
	}

	pub(crate) fn notification_sender(send: channel::Sender<Notification>) -> Self {
		Self {
			notification_sender: Some(send),
			..Default::default()
		}
	}
}

/// Connection trait implemented by supported protocols
pub trait Connection: Sized + Send + Sync + 'static {
	/// Constructs a new client without connecting to the server
	fn new(method: Method) -> Self;

	/// Connect to the server
	fn connect(
		address: Endpoint,
		capacity: usize,
	) -> Pin<Box<dyn Future<Output = Result<Surreal<Self>>> + Send + Sync + 'static>>
	where
		Self: api::Connection;

	/// Send a query to the server
	#[allow(clippy::type_complexity)]
	fn send<'r>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Receiver<Result<DbResponse>>>> + Send + Sync + 'r>>
	where
		Self: api::Connection;

	/// Receive responses for all methods except `query`
	fn recv(
		&mut self,
		receiver: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + Sync + '_>> {
		Box::pin(async move {
			let response = receiver.into_recv_async().await?;
			match response? {
				DbResponse::Other(value) => Ok(value),
				DbResponse::Query(..) => unreachable!(),
			}
		})
	}

	/// Receive the response of the `query` method
	fn recv_query(
		&mut self,
		receiver: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<Response>> + Send + Sync + '_>> {
		Box::pin(async move {
			let response = receiver.into_recv_async().await?;
			match response? {
				DbResponse::Query(results) => Ok(results),
				DbResponse::Other(..) => unreachable!(),
			}
		})
	}

	/// Execute all methods except `query`
	fn execute<'r, R>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<R>> + Send + Sync + 'r>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			let value = self.recv(rx).await?;
			R::deserialize(value).map_err(Into::into)
		})
	}

	/// Execute methods that return an optional single response
	fn execute_opt<'r, R>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Option<R>>> + Send + Sync + 'r>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			match self.recv(rx).await? {
				Value::None => Ok(None),
				value => R::deserialize(value).map_err(Into::into).map(Some),
			}
		})
	}

	/// Execute methods that return multiple responses
	fn execute_vec<'r, R>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Vec<R>>> + Send + Sync + 'r>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			let value = match self.recv(rx).await? {
				Value::None => Value::Array(Default::default()),
				Value::Array(array) => Value::Array(array),
				value => Value::Array(vec![value]),
			};
			Deserialize::deserialize(value).map_err(Into::into)
		})
	}

	/// Execute methods that return nothing
	fn execute_unit<'r>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'r>>
	where
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			match self.recv(rx).await? {
				Value::None => Ok(()),
				Value::Array(array) if array.is_empty() => Ok(()),
				value => Err(Error::FromValue {
					value,
					error: "expected the database to return nothing".to_owned(),
				}
				.into()),
			}
		})
	}

	/// Execute methods that return a raw value
	fn execute_value<'r>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + Sync + 'r>>
	where
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			self.recv(rx).await
		})
	}

	/// Execute the `query` method
	fn execute_query<'r>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Response>> + Send + Sync + 'r>>
	where
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			self.recv_query(rx).await
		})
	}
}
