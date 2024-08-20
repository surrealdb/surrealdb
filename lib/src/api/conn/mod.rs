use crate::api;
use crate::api::err::Error;
use crate::api::method::query::Response;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use channel::Receiver;
use channel::Sender;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use surrealdb_core::sql::{from_value, Value};

mod cmd;

pub(crate) use cmd::Command;
#[cfg(feature = "protocol-http")]
pub(crate) use cmd::RouterRequest;

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

	pub(crate) fn send(
		&self,
		command: Command,
	) -> BoxFuture<'_, Result<Receiver<Result<DbResponse>>>> {
		Box::pin(async move {
			let id = self.next_id();
			let (sender, receiver) = channel::bounded(1);
			let route = Route {
				request: RequestData {
					id,
					command,
				},
				response: sender,
			};
			self.sender.send(route).await?;
			Ok(receiver)
		})
	}

	/// Receive responses for all methods except `query`
	pub(crate) fn recv(
		&self,
		receiver: Receiver<Result<DbResponse>>,
	) -> BoxFuture<'_, Result<Value>> {
		Box::pin(async move {
			let response = receiver.recv().await?;
			match response? {
				DbResponse::Other(value) => Ok(value),
				DbResponse::Query(..) => unreachable!(),
			}
		})
	}

	/// Receive the response of the `query` method
	pub(crate) fn recv_query(
		&self,
		receiver: Receiver<Result<DbResponse>>,
	) -> BoxFuture<'_, Result<Response>> {
		Box::pin(async move {
			let response = receiver.recv().await?;
			match response? {
				DbResponse::Query(results) => Ok(results),
				DbResponse::Other(..) => unreachable!(),
			}
		})
	}

	/// Execute all methods except `query`
	pub(crate) fn execute<R>(&self, command: Command) -> BoxFuture<'_, Result<R>>
	where
		R: DeserializeOwned,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;
			let value = self.recv(rx).await?;
			from_value(value).map_err(Into::into)
		})
	}

	/// Execute methods that return an optional single response
	pub(crate) fn execute_opt<R>(&self, command: Command) -> BoxFuture<'_, Result<Option<R>>>
	where
		R: DeserializeOwned,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;
			match self.recv(rx).await? {
				Value::None | Value::Null => Ok(None),
				value => from_value(value).map_err(Into::into),
			}
		})
	}

	/// Execute methods that return multiple responses
	pub(crate) fn execute_vec<R>(&self, command: Command) -> BoxFuture<'_, Result<Vec<R>>>
	where
		R: DeserializeOwned,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;
			let value = match self.recv(rx).await? {
				Value::None | Value::Null => Value::Array(Default::default()),
				Value::Array(array) => Value::Array(array),
				value => vec![value].into(),
			};
			from_value(value).map_err(Into::into)
		})
	}

	/// Execute methods that return nothing
	pub(crate) fn execute_unit(&self, command: Command) -> BoxFuture<'_, Result<()>> {
		Box::pin(async move {
			let rx = self.send(command).await?;
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
	pub(crate) fn execute_value(&self, command: Command) -> BoxFuture<'_, Result<Value>> {
		Box::pin(async move {
			let rx = self.send(command).await?;
			self.recv(rx).await
		})
	}

	/// Execute the `query` method
	pub(crate) fn execute_query(&self, command: Command) -> BoxFuture<'_, Result<Response>> {
		Box::pin(async move {
			let rx = self.send(command).await?;
			self.recv_query(rx).await
		})
	}
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
pub(crate) struct MlExportConfig {
	// fields are used in http and local non-wasm with ml features
	#[allow(dead_code)]
	pub(crate) name: String,
	#[allow(dead_code)]
	pub(crate) version: String,
}

/// Connection trait implemented by supported protocols
pub trait Connection: Sized + Send + Sync + 'static {
	/// Connect to the server
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>>
	where
		Self: api::Connection;
}
