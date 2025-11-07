use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};

use async_channel::{Receiver, Sender};
use surrealdb_core::dbs::QueryResult;
use surrealdb_core::rpc::DbResultError;
use surrealdb_types::{SurrealValue, Value};
use uuid::Uuid;

use crate::err::Error;
use crate::method::BoxFuture;
use crate::opt::Endpoint;
use crate::{ExtraFeatures, Result, Surreal};

pub(crate) mod cmd;
pub(crate) use cmd::Command;

use super::opt::Config;

#[derive(Debug)]
#[allow(dead_code, reason = "Used by the embedded and remote connections.")]
pub struct RequestData {
	pub(crate) id: i64,
	pub(crate) command: Command,
	pub(crate) session_id: Option<Uuid>,
}

#[derive(Debug)]
#[allow(dead_code, reason = "Used by the embedded and remote connections.")]
pub(crate) struct Route {
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) request: RequestData,
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) response: Sender<std::result::Result<Vec<QueryResult>, DbResultError>>,
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

	#[allow(clippy::type_complexity)]
	pub(crate) fn send_command(
		&self,
		command: Command,
		session_id: Option<Uuid>,
	) -> BoxFuture<'_, Result<Receiver<std::result::Result<Vec<QueryResult>, DbResultError>>>> {
		Box::pin(async move {
			let id = self.next_id();
			let (sender, receiver) = async_channel::bounded(1);
			let route = Route {
				request: RequestData {
					id,
					command,
					session_id,
				},
				response: sender,
			};
			self.sender
				.send(route)
				.await
				.map_err(|e| Error::InternalError(format!("Failed to send command: {}", e)))?;
			Ok(receiver)
		})
	}

	/// Receive responses for all methods except `query`
	pub(crate) fn recv_value(
		&self,
		receiver: Receiver<std::result::Result<Vec<QueryResult>, DbResultError>>,
	) -> BoxFuture<'_, std::result::Result<Value, Error>> {
		Box::pin(async move {
			let response = receiver.recv().await.map_err(|_| Error::ConnectionUninitialised)?;
			// The response already uses DbResultError, so we just convert directly
			let mut results = response.map_err(Error::from)?;

			match results.len() {
				0 => Ok(Value::None),
				1 => {
					let result = results.remove(0);
					result.result.map_err(Error::from)
				}
				_ => Err(Error::InternalError(
					"expected the database to return one or no results".to_string(),
				)),
			}
		})
	}

	/// Receive the response of the `query` method
	pub(crate) fn recv_results(
		&self,
		receiver: Receiver<std::result::Result<Vec<QueryResult>, DbResultError>>,
	) -> BoxFuture<'_, Result<Vec<QueryResult>>> {
		Box::pin(async move {
			let results = receiver.recv().await.map_err(|_| Error::ConnectionUninitialised)?;
			results.map_err(Error::from)
		})
	}

	/// Execute all methods except `query`
	pub(crate) fn execute<R>(&self, command: Command, session_id: Uuid) -> BoxFuture<'_, Result<R>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send_command(command, Some(session_id)).await?;
			let value = self.recv_value(rx).await?;
			// Handle single-element arrays that might be returned from operations like
			// signup/signin
			let result = match value {
				Value::Array(array) if array.len() == 1 => {
					R::from_value(array.into_iter().next().expect("array has exactly one element"))
				}
				v => R::from_value(v),
			};
			Ok(result?)
		})
	}

	/// Execute methods that return an optional single response
	pub(crate) fn execute_opt<R>(
		&self,
		command: Command,
		session_id: Uuid,
	) -> BoxFuture<'_, Result<Option<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send_command(command, Some(session_id)).await?;
			match self.recv_value(rx).await? {
				Value::None | Value::Null => Ok(None),
				Value::Array(array) => match array.len() {
					// Empty array means no results
					0 => Ok(None),
					// Single-element array: extract and return the element
					// This happens when operating on a record ID
					1 => Ok(Some(R::from_value(
						array.into_iter().next().expect("array has exactly one element"),
					)?)),
					// Multiple elements should not happen for operations expecting Option<T>
					_ => Ok(Some(R::from_value(Value::Array(array))?)),
				},
				value => Ok(Some(R::from_value(value)?)),
			}
		})
	}

	/// Execute methods that return multiple responses
	pub(crate) fn execute_vec<R>(
		&self,
		command: Command,
		session_id: Uuid,
	) -> BoxFuture<'_, Result<Vec<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send_command(command, Some(session_id)).await?;
			match self.recv_value(rx).await? {
				Value::None | Value::Null => Ok(Vec::new()),
				Value::Array(array) => array
					.into_iter()
					.map(|v| R::from_value(v).map_err(Into::into))
					.collect::<Result<Vec<R>>>(),
				value => Ok(vec![R::from_value(value)?]),
			}
		})
	}

	/// Execute methods that return nothing
	pub(crate) fn execute_unit(
		&self,
		command: Command,
		session_id: Uuid,
	) -> BoxFuture<'_, Result<()>> {
		Box::pin(async move {
			let rx = self.send_command(command, Some(session_id)).await?;
			match self.recv_value(rx).await? {
				Value::None | Value::Null => Ok(()),
				Value::Array(array) if array.is_empty() => Ok(()),
				value => Err(Error::FromValue {
					value,
					error: "expected the database to return nothing".to_owned(),
				}),
			}
		})
	}

	/// Execute methods that return a raw value
	pub(crate) fn execute_value(
		&self,
		command: Command,
		session_id: Uuid,
	) -> BoxFuture<'_, Result<Value>> {
		Box::pin(async move {
			let rx = self.send_command(command, Some(session_id)).await?;
			self.recv_value(rx).await
		})
	}

	/// Execute the `query` method
	pub(crate) fn execute_query(
		&self,
		command: Command,
		session_id: Uuid,
	) -> BoxFuture<'_, Result<Vec<QueryResult>>> {
		Box::pin(async move {
			let rx = self.send_command(command, Some(session_id)).await?;
			self.recv_results(rx).await
		})
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
	#[allow(private_interfaces)]
	fn connect(
		address: Endpoint,
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>>
	where
		Self: crate::Connection;
}
