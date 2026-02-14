use std::collections::HashSet;

use async_channel::{Receiver, Sender};
use surrealdb_core::dbs::QueryResult;
use uuid::Uuid;

use crate::method::BoxFuture;
use crate::opt::Endpoint;
use crate::types::{SurrealValue, Value};
use crate::{Error, ExtraFeatures, Result, Surreal};

pub(crate) mod cmd;
pub(crate) use cmd::Command;

use super::opt::Config;

#[derive(Debug)]
#[allow(dead_code, reason = "Used by the embedded and remote connections.")]
pub struct RequestData {
	pub(crate) command: Command,
	pub(crate) session_id: Uuid,
}

#[derive(Debug)]
#[allow(dead_code, reason = "Used by the embedded and remote connections.")]
pub(crate) struct Route {
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) request: RequestData,
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) response: Sender<std::result::Result<Vec<QueryResult>, surrealdb_types::Error>>,
}

/// Message router
#[derive(Debug, Clone)]
pub struct Router {
	pub(crate) sender: Sender<Route>,
	#[allow(dead_code)]
	pub(crate) config: Config,
	pub(crate) features: HashSet<ExtraFeatures>,
}

impl Router {
	#[allow(clippy::type_complexity)]
	pub(crate) fn send_command(
		&self,
		session_id: Uuid,
		command: Command,
	) -> BoxFuture<
		'_,
		Result<Receiver<std::result::Result<Vec<QueryResult>, surrealdb_types::Error>>>,
	> {
		Box::pin(async move {
			let (sender, receiver) = async_channel::bounded(1);
			let route = Route {
				request: RequestData {
					command,
					session_id,
				},
				response: sender,
			};
			self.sender
				.send(route)
				.await
				.map_err(|e| crate::Error::internal(format!("Failed to send command: {e}")))?;
			Ok(receiver)
		})
	}

	/// Receive responses for all methods except `query`
	pub(crate) fn recv_value(
		&self,
		receiver: Receiver<std::result::Result<Vec<QueryResult>, surrealdb_types::Error>>,
	) -> BoxFuture<'_, std::result::Result<Value, Error>> {
		Box::pin(async move {
			let response = receiver.recv().await.map_err(|_| {
				crate::Error::connection(
					"Connection uninitialised".to_string(),
					Some(crate::types::ConnectionError::Uninitialised),
				)
			})?;
			let mut results = response?;

			match results.len() {
				0 => Ok(Value::None),
				1 => {
					let result = results.remove(0);
					result.result
				}
				_ => Err(crate::Error::internal(
					"expected the database to return one or no results".to_string(),
				)),
			}
		})
	}

	/// Receive the response of the `query` method
	pub(crate) fn recv_results(
		&self,
		receiver: Receiver<std::result::Result<Vec<QueryResult>, surrealdb_types::Error>>,
	) -> BoxFuture<'_, Result<Vec<QueryResult>>> {
		Box::pin(async move {
			let results = receiver.recv().await.map_err(|_| {
				crate::Error::connection(
					"Connection uninitialised".to_string(),
					Some(crate::types::ConnectionError::Uninitialised),
				)
			})?;
			results
		})
	}

	/// Execute all methods except `query`
	pub(crate) fn execute<R>(&self, session_id: Uuid, command: Command) -> BoxFuture<'_, Result<R>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send_command(session_id, command).await?;
			let value = self.recv_value(rx).await?;
			// Handle single-element arrays that might be returned from operations like
			// signup/signin
			let result = match value {
				Value::Array(array) if array.len() == 1 => {
					R::from_value(array.into_iter().next().expect("array has exactly one element"))
				}
				v => R::from_value(v),
			};
			result.map_err(|e| crate::Error::internal(e.to_string()))
		})
	}

	/// Execute methods that return an optional single response
	pub(crate) fn execute_opt<R>(
		&self,
		session_id: Uuid,
		command: Command,
	) -> BoxFuture<'_, Result<Option<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send_command(session_id, command).await?;
			match self.recv_value(rx).await? {
				Value::None | Value::Null => Ok(None),
				Value::Array(array) => match array.len() {
					// Empty array means no results
					0 => Ok(None),
					// Single-element array: extract and return the element
					// This happens when operating on a record ID
					1 => Ok(Some(
						R::from_value(
							array.into_iter().next().expect("array has exactly one element"),
						)
						.map_err(|e| crate::Error::internal(e.to_string()))?,
					)),
					// Multiple elements should not happen for operations expecting Option<T>
					_ => Ok(Some(
						R::from_value(Value::Array(array))
							.map_err(|e| crate::Error::internal(e.to_string()))?,
					)),
				},
				value => Ok(Some(
					R::from_value(value).map_err(|e| crate::Error::internal(e.to_string()))?,
				)),
			}
		})
	}

	/// Execute methods that return multiple responses
	pub(crate) fn execute_vec<R>(
		&self,
		session_id: Uuid,
		command: Command,
	) -> BoxFuture<'_, Result<Vec<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			let rx = self.send_command(session_id, command).await?;
			match self.recv_value(rx).await? {
				Value::None | Value::Null => Ok(Vec::new()),
				Value::Array(array) => array
					.into_iter()
					.map(|v| R::from_value(v).map_err(|e| crate::Error::internal(e.to_string())))
					.collect::<Result<Vec<R>>>(),
				value => Ok(vec![
					R::from_value(value).map_err(|e| crate::Error::internal(e.to_string()))?,
				]),
			}
		})
	}

	/// Execute methods that return nothing
	pub(crate) fn execute_unit(
		&self,
		session_id: Uuid,
		command: Command,
	) -> BoxFuture<'_, Result<()>> {
		Box::pin(async move {
			let rx = self.send_command(session_id, command).await?;
			match self.recv_value(rx).await? {
				Value::None | Value::Null => Ok(()),
				Value::Array(array) if array.is_empty() => Ok(()),
				_value => Err(crate::Error::internal(
					"expected the database to return nothing".to_string(),
				)),
			}
		})
	}

	/// Execute methods that return a raw value
	pub(crate) fn execute_value(
		&self,
		session_id: Uuid,
		command: Command,
	) -> BoxFuture<'_, Result<Value>> {
		Box::pin(async move {
			let rx = self.send_command(session_id, command).await?;
			self.recv_value(rx).await
		})
	}

	/// Execute the `query` method
	pub(crate) fn execute_query(
		&self,
		session_id: Uuid,
		command: Command,
	) -> BoxFuture<'_, Result<Vec<QueryResult>>> {
		Box::pin(async move {
			let rx = self.send_command(session_id, command).await?;
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
