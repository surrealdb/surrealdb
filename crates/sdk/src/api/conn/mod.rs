use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};

use async_channel::{Receiver, Sender};
use indexmap::IndexMap;
use surrealdb_core::rpc::{DbResult, DbResultError, DbResultStats};
use surrealdb_types::{SurrealValue, Value};

use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::method::query::IndexedResults;
use crate::api::opt::Endpoint;
use crate::api::{ExtraFeatures, Result, Surreal};

mod cmd;
pub(crate) use cmd::Command;
#[cfg(feature = "protocol-http")]
pub(crate) use cmd::RouterRequest;

use super::opt::Config;

#[derive(Debug)]
#[allow(dead_code, reason = "Used by the embedded and remote connections.")]
pub struct RequestData {
	pub(crate) id: i64,
	pub(crate) command: Command,
}

#[derive(Debug)]
#[allow(dead_code, reason = "Used by the embedded and remote connections.")]
pub(crate) struct Route {
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) request: RequestData,
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) response: Sender<Result<IndexedDbResults>>,
}

/// Message router
#[derive(Debug)]
pub struct Router {
	pub(crate) sender: Sender<Route>,
	#[allow(dead_code)]
	pub(crate) config: Config,
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
	) -> BoxFuture<'_, Result<Receiver<Result<IndexedDbResults>>>> {
		Box::pin(async move {
			let id = self.next_id();
			let (sender, receiver) = async_channel::bounded(1);
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
		receiver: Receiver<Result<IndexedDbResults>>,
	) -> BoxFuture<'_, Result<Value>> {
		Box::pin(async move {
			let response = receiver.recv().await?;
			match response? {
				IndexedDbResults::Other(value) => Ok(value),
				IndexedDbResults::Query(..) => unreachable!(),
			}
		})
	}

	/// Receive the response of the `query` method
	pub(crate) fn recv_query(
		&self,
		receiver: Receiver<Result<IndexedDbResults>>,
	) -> BoxFuture<'_, Result<IndexedResults>> {
		Box::pin(async move {
			let response = receiver.recv().await?;
			match response? {
				IndexedDbResults::Query(results) => Ok(results),
				IndexedDbResults::Other(..) => unreachable!(),
			}
		})
	}

	/// Execute all methods except `query`
	pub(crate) fn execute<R>(&self, command: Command) -> BoxFuture<'_, Result<R>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;
			let value = self.recv(rx).await?;
			R::from_value(value)
		})
	}

	/// Execute methods that return an optional single response
	pub(crate) fn execute_opt<R>(&self, command: Command) -> BoxFuture<'_, Result<Option<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;
			match self.recv(rx).await? {
				Value::None | Value::Null => Ok(None),
				value => Ok(Some(R::from_value(value)?)),
			}
		})
	}

	/// Execute methods that return multiple responses
	pub(crate) fn execute_vec<R>(&self, command: Command) -> BoxFuture<'_, Result<Vec<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;
			match self.recv(rx).await? {
				Value::None | Value::Null => Ok(Vec::new()),
				Value::Array(array) => {
					array.into_iter().map(R::from_value).collect::<Result<Vec<R>>>()
				}
				value => Ok(vec![R::from_value(value)?]),
			}
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
	pub(crate) fn execute_query(&self, command: Command) -> BoxFuture<'_, Result<IndexedResults>> {
		Box::pin(async move {
			let rx = self.send(command).await?;
			self.recv_query(rx).await
		})
	}
}

/// The database response sent from the router to the caller
#[derive(Debug)]
pub enum IndexedDbResults {
	/// The response sent for the `query` method
	Query(IndexedResults),
	/// The response sent for any method except `query`
	Other(Value),
}

impl IndexedDbResults {
	pub(crate) fn from_server_result(result: DbResult) -> Result<Self> {
		match result {
			DbResult::Other(value) => Ok(Self::Other(value)),
			DbResult::Query(responses) => {
				let mut results = IndexMap::<
					usize,
					(DbResultStats, std::result::Result<Value, DbResultError>),
				>::with_capacity(responses.len());

				for (index, response) in responses.into_iter().enumerate() {
					let stats = DbResultStats::default().with_execution_time(response.time);
					results.insert(index, (stats, response.result));
				}

				Ok(Self::Query(IndexedResults {
					results,
					live_queries: IndexMap::default(),
				}))
			}
			// Live notifications don't call this method
			DbResult::Live(..) => unreachable!(),
		}
	}
}

#[derive(Debug, Clone)]
pub(crate) struct MlExportConfig {
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) name: String,
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) version: String,
}

/// Connection trait implemented by supported protocols
pub trait Sealed: Sized + Send + Sync + 'static {
	/// Connect to the server
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>>
	where
		Self: crate::api::Connection;
}
