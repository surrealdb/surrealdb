use crate::api;
use crate::api::err::Error;
use crate::api::method::query::Response;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use flume::{Receiver, Sender};
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use surrealdb_core::sql::{from_value, Value};

mod cmd;

pub use cmd::Command;

#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct RequestData {
	pub(crate) id: i64,
	pub(crate) command: Command,
}

#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub(crate) struct Route {
	pub(crate) request: RequestData,
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

#[derive(Debug, Clone)]
#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
pub(crate) struct MlExportConfig {
	pub(crate) name: String,
	pub(crate) version: String,
}

/// Connection trait implemented by supported protocols
pub trait Connection: Sized + Send + Sync + 'static {
	/// Constructs a new client without connecting to the server
	fn new(method: Method) -> Self;

	/// Connect to the server
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>>
	where
		Self: api::Connection;

	/// Send a query to the server
	#[allow(clippy::type_complexity)]
	fn send<'r>(
		&'r mut self,
		router: &'r Router,
		command: Command,
	) -> BoxFuture<'r, Result<Receiver<Result<DbResponse>>>>
	where
		Self: api::Connection;

	/// Receive responses for all methods except `query`
	fn recv(&mut self, receiver: Receiver<Result<DbResponse>>) -> BoxFuture<'_, Result<Value>> {
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
	) -> BoxFuture<'_, Result<Response>> {
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
		command: Command,
	) -> BoxFuture<'r, Result<R>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, command).await?;
			let value = self.recv(rx).await?;
			from_value(value).map_err(Into::into)
		})
	}

	/// Execute methods that return an optional single response
	fn execute_opt<'r, R>(
		&'r mut self,
		router: &'r Router,
		command: Command,
	) -> BoxFuture<'r, Result<Option<R>>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, command).await?;
			match self.recv(rx).await? {
				Value::None | Value::Null => Ok(None),
				value => from_value(value).map_err(Into::into),
			}
		})
	}

	/// Execute methods that return multiple responses
	fn execute_vec<'r, R>(
		&'r mut self,
		router: &'r Router,
		command: Command,
	) -> BoxFuture<'r, Result<Vec<R>>>
	where
		R: DeserializeOwned,
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, command).await?;
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
		router: &'r Router,
		command: Command,
	) -> BoxFuture<'r, Result<()>>
	where
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, command).await?;
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
		router: &'r Router,
		command: Command,
	) -> BoxFuture<'r, Result<Value>>
	where
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, command).await?;
			self.recv(rx).await
		})
	}

	/// Execute the `query` method
	fn execute_query<'r>(
		&'r mut self,
		router: &'r Router,
		command: Command,
	) -> BoxFuture<'r, Result<Response>>
	where
		Self: api::Connection,
	{
		Box::pin(async move {
			let rx = self.send(router, command).await?;
			self.recv_query(rx).await
		})
	}
}
