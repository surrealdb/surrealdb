use crate::QueryResults;
use crate::api;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
use crate::rpc::Response;
use async_channel::Receiver;
use async_channel::Sender;
use chrono::DateTime;
use chrono::Utc;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use surrealdb_core::dbs::ResponseData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Value, from_value as from_core_value};

pub use surrealdb_core::protocol::flatbuffers::surreal_db::protocol::rpc::Request as RequestProto;
pub use surrealdb_core::protocol::flatbuffers::surreal_db::protocol::rpc::Response as ResponseFb;

mod cmd;
#[cfg(feature = "protocol-http")]
pub(crate) use cmd::RouterRequest;
pub(crate) use cmd::{Command, LiveQueryParams, Request};

use super::opt::Config;

#[derive(Debug)]
#[allow(dead_code, reason = "Used by the embedded and remote connections.")]
pub(crate) struct Route {
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) request: Request,
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) response: Sender<Result<ResponseData>>,
}

/// Message router
#[derive(Debug)]
pub struct Router {
	pub(crate) sender: Sender<Route>,
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
	) -> BoxFuture<'_, Result<Receiver<Result<ResponseData>>>> {
		Box::pin(async move {
			let id = self.next_id();
			let (sender, receiver) = async_channel::bounded(1);
			let route = Route {
				request: Request {
					id: id.to_string(),
					command,
				},
				response: sender,
			};
			self.sender.send(route).await?;
			Ok(receiver)
		})
	}

	fn get_single_value(query_result_data: Result<ResponseData>) -> Result<Value> {
		let results = match query_result_data? {
			ResponseData::Results(results) => results,
			ResponseData::Notification(_) => {
				return Err(Error::InternalError(
					"Received a notification instead of query results".to_owned(),
				)
				.into());
			}
		};

		if results.is_empty() {
			return Err(Error::InternalError(
				"Expected at least one result, but received none".to_string(),
			)
			.into());
		}

		if results.len() > 1 {
			return Err(Error::InternalError(
				"Expected a single result, but received multiple".to_string(),
			)
			.into());
		}

		let Some(query_result) = results.into_iter().next() else {
			return Err(Error::InternalError(
				"Expected a single result, but received none".to_string(),
			)
			.into());
		};

		let result_value = query_result.result?;

		Ok(result_value)
	}

	/// Execute all methods except `query`
	pub(crate) async fn execute<R>(&self, command: Command) -> Result<R>
	where
		R: TryFromValue,
	{
		let rx = self.send(command).await?;
		let query_result_data = rx.recv().await?;
		let value = Self::get_single_value(query_result_data)?;
		R::try_from_value(value)
	}

	/// Execute methods that return an optional single response
	pub(crate) fn execute_opt<R>(&self, command: Command) -> BoxFuture<'_, Result<Option<R>>>
	where
		R: TryFromValue,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;

			let query_result_data = rx.recv().await?;

			let value = Self::get_single_value(query_result_data)?;

			match value {
				Value::None | Value::Null => Ok(None),
				value => Ok(Some(R::try_from_value(value)?)),
			}
		})
	}

	/// Execute methods that return multiple responses
	pub(crate) fn execute_vec<R>(&self, command: Command) -> BoxFuture<'_, Result<Vec<R>>>
	where
		R: TryFromValue,
	{
		Box::pin(async move {
			let rx = self.send(command).await?;
			let query_result_data = rx.recv().await?;

			let value = Self::get_single_value(query_result_data)?;

			match value {
				Value::None | Value::Null => return Ok(Vec::new()),
				Value::Array(array) => {
					Ok(array.0.into_iter().map(R::try_from_value).collect::<Result<Vec<_>>>()?)
				}
				value => Ok(vec![R::try_from_value(value)?]),
			}
		})
	}

	/// Execute methods that return nothing
	pub(crate) fn execute_unit(&self, command: Command) -> BoxFuture<'_, Result<()>> {
		Box::pin(async move {
			let rx = self.send(command).await?;
			let query_result_data = rx.recv().await?;
			let value = Self::get_single_value(query_result_data)?;

			match value {
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
			let query_result_data = rx.recv().await?;

			let value = Self::get_single_value(query_result_data)?;

			Ok(value)
		})
	}

	/// Execute the `query` method
	pub(crate) fn execute_query(&self, command: Command) -> BoxFuture<'_, Result<QueryResults>> {
		Box::pin(async move {
			let rx = self.send(command).await?;
			let query_result_data = rx.recv().await?;

			let mut query_results = QueryResults::new();
			match query_result_data? {
				ResponseData::Results(results) => {
					query_results.results = results.into_iter().enumerate().collect();
				}
				ResponseData::Notification(_) => {
					return Err(Error::InternalError(
						"Received a notification instead of query results".to_owned(),
					)
					.into());
				}
			}

			Ok(query_results)
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
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>>
	where
		Self: api::Connection;
}
