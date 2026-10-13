use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use async_channel::Sender;
// The engine interface, and the route-channel adapter that lets the WebSocket
// and HTTP engines serve it unchanged. `Command` is that adapter's vocabulary,
// not this crate's: nothing outside it constructs one.
// Which of these a build uses depends on which engines it enables.
#[allow(unused_imports)]
pub(crate) use surrealdb_engine_api::{
	Command, EngineContext, MlExportConfig, RequestData, Route, RouteChannelEngine, SurrealEngine,
	single_result,
};
use surrealdb_rpc::QueryResult;
use uuid::Uuid;

use super::opt::Config;
use crate::method::BoxFuture;
use crate::opt::Endpoint;
use crate::types::{SurrealValue, Value, Variables};
use crate::{Error, ExtraFeatures, Result, Surreal};

/// The engine a [`Surreal`] connection drives, plus what the SDK needs to
/// know about it.
#[derive(Debug, Clone)]
pub struct Router {
	pub(crate) engine: Arc<dyn SurrealEngine>,
	#[allow(dead_code)]
	pub(crate) config: Config,
	pub(crate) features: HashSet<ExtraFeatures>,
}

/// A query the SDK has compiled but not yet run.
///
/// The builder methods (`content`, `merge`, `patch`, ...) assemble their
/// SurrealQL before the caller awaits them, so they need somewhere to keep it
/// meanwhile.
#[derive(Debug)]
pub(crate) struct QueryRequest {
	pub(crate) txn: Option<Uuid>,
	pub(crate) query: Cow<'static, str>,
	pub(crate) variables: Variables,
}

impl Router {
	/// Runs an already-compiled query, deserialising an optional record.
	pub(crate) fn run_query_opt<R>(
		&self,
		session: Uuid,
		request: QueryRequest,
	) -> BoxFuture<'_, Result<Option<R>>>
	where
		R: SurrealValue,
	{
		self.query_opt(ctx_txn(session, request.txn), request.query, request.variables)
	}

	/// Runs an already-compiled query, deserialising a list of records.
	pub(crate) fn run_query_vec<R>(
		&self,
		session: Uuid,
		request: QueryRequest,
	) -> BoxFuture<'_, Result<Vec<R>>>
	where
		R: SurrealValue,
	{
		self.query_vec(ctx_txn(session, request.txn), request.query, request.variables)
	}

	/// Runs an already-compiled query, returning its raw value.
	pub(crate) fn run_query_value(
		&self,
		session: Uuid,
		request: QueryRequest,
	) -> BoxFuture<'_, Result<Value>> {
		self.query_value(ctx_txn(session, request.txn), request.query, request.variables)
	}

	/// Builds a router around a [`Route`] channel, the shape the WebSocket and
	/// HTTP engines each already produce.
	pub(crate) fn from_route_sender(
		sender: Sender<Route>,
		features: HashSet<ExtraFeatures>,
		config: Config,
	) -> Self {
		Self {
			engine: Arc::new(RouteChannelEngine::new(sender)),
			config,
			features,
		}
	}

	/// Builds a router around an engine that serves [`SurrealEngine`]
	/// directly, with no route channel behind it.
	#[cfg_attr(
		not(any(
			feature = "protocol-grpc",
			feature = "kv-mem",
			feature = "kv-tikv",
			feature = "kv-rocksdb",
			feature = "kv-indxdb",
			feature = "kv-surrealkv",
		)),
		allow(dead_code)
	)]
	pub(crate) fn from_engine(
		engine: Arc<dyn SurrealEngine>,
		features: HashSet<ExtraFeatures>,
		config: Config,
	) -> Self {
		Self {
			engine,
			config,
			features,
		}
	}

	/// Runs a query and returns the single value its one statement produced.
	///
	/// The CRUD methods (`select`, `create`, `update`, ...) each compile to
	/// exactly one statement, so anything else is a bug in the caller rather
	/// than something to surface to the user.
	pub(crate) fn query_value(
		&self,
		ctx: EngineContext,
		query: Cow<'static, str>,
		variables: Variables,
	) -> BoxFuture<'_, Result<Value>> {
		Box::pin(async move { single_result(self.engine.query(ctx, query, variables).await?) })
	}

	/// Runs a query and deserialises its result as an optional record.
	pub(crate) fn query_opt<R>(
		&self,
		ctx: EngineContext,
		query: Cow<'static, str>,
		variables: Variables,
	) -> BoxFuture<'_, Result<Option<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			match self.query_value(ctx, query, variables).await? {
				Value::None | Value::Null => Ok(None),
				Value::Array(array) => match array.len() {
					// No match is not an error for an `Option<R>` caller.
					0 => Ok(None),
					// Operating on a record id yields a one-element array.
					1 => {
						let value =
							array.into_iter().next().expect("array has exactly one element");
						Ok(Some(R::from_value(value).map_err(deserialization_error)?))
					}
					// More than one should not happen here, but deserialising
					// the whole array gives a clearer error than truncating.
					_ => {
						Ok(Some(R::from_value(Value::Array(array)).map_err(deserialization_error)?))
					}
				},
				value => Ok(Some(R::from_value(value).map_err(deserialization_error)?)),
			}
		})
	}

	/// Runs a query and deserialises its result as a list of records.
	pub(crate) fn query_vec<R>(
		&self,
		ctx: EngineContext,
		query: Cow<'static, str>,
		variables: Variables,
	) -> BoxFuture<'_, Result<Vec<R>>>
	where
		R: SurrealValue,
	{
		Box::pin(async move {
			match self.query_value(ctx, query, variables).await? {
				Value::None | Value::Null => Ok(Vec::new()),
				Value::Array(array) => array
					.into_iter()
					.map(|value| R::from_value(value).map_err(deserialization_error))
					.collect(),
				value => Ok(vec![R::from_value(value).map_err(deserialization_error)?]),
			}
		})
	}

	/// Runs a query, returning every statement's result.
	pub(crate) fn query_results(
		&self,
		ctx: EngineContext,
		query: Cow<'static, str>,
		variables: Variables,
	) -> BoxFuture<'_, Result<Vec<QueryResult>>> {
		Box::pin(async move { self.engine.query(ctx, query, variables).await })
	}
}

fn deserialization_error(error: impl std::fmt::Display) -> Error {
	Error::serialization(error.to_string(), crate::types::SerializationError::Deserialization)
}

/// A context for a request on `session`, outside any explicit transaction.
pub(crate) fn ctx(session: Uuid) -> EngineContext {
	EngineContext::new(session)
}

/// A context for a request on `session`, inside `txn` when there is one.
pub(crate) fn ctx_txn(session: Uuid, txn: Option<Uuid>) -> EngineContext {
	EngineContext::with_transaction(session, txn)
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
