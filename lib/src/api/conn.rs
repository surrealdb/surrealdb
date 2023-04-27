use crate::api;
use crate::api::err::Error;
use crate::api::method::query::Response;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use crate::opt::from_value;
use crate::sql::Query;
use crate::sql::Value;
use flume::Receiver;
use flume::Sender;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
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
pub struct Router<C: api::Connection> {
	pub(crate) conn: PhantomData<C>,
	pub(crate) sender: Sender<Option<Route>>,
	pub(crate) last_id: AtomicI64,
	pub(crate) features: HashSet<ExtraFeatures>,
}

impl<C> Router<C>
where
	C: api::Connection,
{
	pub(crate) fn next_id(&self) -> i64 {
		self.last_id.fetch_add(1, Ordering::SeqCst)
	}
}

impl<C> Drop for Router<C>
where
	C: api::Connection,
{
	fn drop(&mut self) {
		let _res = self.sender.send(None);
	}
}

/// The query method
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Method {
	/// Sends an authentication token to the server
	Authenticate,
	/// Perfoms a merge update operation
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
	/// Kills a live query
	#[doc(hidden)] // Not supported yet
	Kill,
	/// Starts a live query
	#[doc(hidden)] // Not supported yet
	Live,
	/// Perfoms a patch update operation
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
	/// Perfoms an update operation
	Update,
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

/// Holds the parameters given to the caller
#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct Param {
	pub(crate) query: Option<(Query, BTreeMap<String, Value>)>,
	pub(crate) other: Vec<Value>,
	pub(crate) file: Option<PathBuf>,
}

impl Param {
	pub(crate) fn new(other: Vec<Value>) -> Self {
		Self {
			other,
			query: None,
			file: None,
		}
	}

	pub(crate) fn query(query: Query, bindings: BTreeMap<String, Value>) -> Self {
		Self {
			query: Some((query, bindings)),
			other: Vec::new(),
			file: None,
		}
	}

	pub(crate) fn file(file: PathBuf) -> Self {
		Self {
			query: None,
			other: Vec::new(),
			file: Some(file),
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
		router: &'r Router<Self>,
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
		router: &'r Router<Self>,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<R>> + Send + Sync + 'r>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			let value = self.recv(rx).await?;
			from_value(value).map_err(Into::into)
		})
	}

	/// Execute methods that return an optional single response
	fn execute_opt<'r, R>(
		&'r mut self,
		router: &'r Router<Self>,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Option<R>>> + Send + Sync + 'r>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			match self.recv(rx).await? {
				Value::None | Value::Null => Ok(None),
				value => from_value(value).map_err(Into::into),
			}
		})
	}

	/// Execute methods that return multiple responses
	fn execute_vec<'r, R>(
		&'r mut self,
		router: &'r Router<Self>,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Vec<R>>> + Send + Sync + 'r>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			let value = match self.recv(rx).await? {
				Value::None | Value::Null => Value::Array(Default::default()),
				Value::Array(array) => Value::Array(array),
				value => vec![value].into(),
			};
			from_value(value).map_err(Into::into)
		})
	}

	/// Execute methods that return nothing
	fn execute_unit<'r>(
		&'r mut self,
		router: &'r Router<Self>,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'r>>
	where
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, param).await?;
			match self.recv(rx).await? {
				Value::None | Value::Null => Ok(()),
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
		router: &'r Router<Self>,
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
		router: &'r Router<Self>,
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
