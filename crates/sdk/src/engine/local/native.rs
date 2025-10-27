use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::task::Poll;

use async_channel::{Receiver, Sender};
use futures::StreamExt;
use futures::stream::poll_fn;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::Level;
use surrealdb_core::kvs::{Datastore, Transaction};
use surrealdb_core::options::EngineOptions;
use surrealdb_types::Variables;
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::conn::{self, Route, Router};
use crate::engine::local::{Db, LiveQueryMap};
use crate::engine::tasks;
use crate::method::BoxFuture;
use crate::opt::auth::Root;
use crate::opt::{Endpoint, EndpointKind, WaitFor};
use crate::{ExtraFeatures, Result, Surreal};

impl crate::Connection for Db {}
impl conn::Sealed for Db {
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded(1);
			let config = address.config.clone();

			tokio::spawn(run_router(address, conn_tx, route_rx));

			conn_rx.recv().await??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features,
				config,
				sender: route_tx,
				last_id: AtomicI64::new(0),
			};

			Ok((router, waiter).into())
		})
	}
}

#[derive(Clone)]
pub(crate) struct RouterState {
	pub(crate) kvs: Arc<Datastore>,
	pub(crate) vars: Arc<RwLock<Variables>>,
	pub(crate) live_queries: Arc<RwLock<LiveQueryMap>>,
	pub(crate) session: Arc<RwLock<Session>>,
	pub(crate) transactions: Arc<RwLock<HashMap<Uuid, Transaction>>>,
}

pub(crate) async fn run_router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
) {
	let configured_root = match address.config.auth {
		Level::Root => Some(Root {
			username: address.config.username,
			password: address.config.password,
		}),
		_ => None,
	};

	let endpoint = match EndpointKind::from(address.url.scheme()) {
		EndpointKind::TiKv => address.url.as_str(),
		_ => &address.path,
	};

	let kvs = match Datastore::new(endpoint).await {
		Ok(kvs) => {
			if let Err(error) = kvs.check_version().await {
				conn_tx.send(Err(crate::Error::InternalError(error.to_string()))).await.ok();
				return;
			};
			if let Err(error) = kvs.bootstrap().await {
				conn_tx.send(Err(crate::Error::InternalError(error.to_string()))).await.ok();
				return;
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = &configured_root {
				if let Err(error) = kvs.initialise_credentials(&root.username, &root.password).await
				{
					conn_tx.send(Err(crate::Error::InternalError(error.to_string()))).await.ok();
					return;
				}
			}
			conn_tx.send(Ok(())).await.ok();
			kvs.with_auth_enabled(configured_root.is_some())
		}
		Err(error) => {
			conn_tx.send(Err(crate::Error::InternalError(error.to_string()))).await.ok();
			return;
		}
	};

	let kvs = match address.config.capabilities.allows_live_query_notifications() {
		true => kvs.with_notifications(),
		false => kvs,
	};

	let kvs = kvs
		.with_strict_mode(address.config.strict)
		.with_query_timeout(address.config.query_timeout)
		.with_transaction_timeout(address.config.transaction_timeout)
		.with_capabilities(address.config.capabilities);

	#[cfg(storage)]
	let kvs = kvs.with_temporary_directory(address.config.temporary_directory);

	let kvs = Arc::new(kvs);
	let vars = Arc::new(RwLock::new(Variables::default()));
	let live_queries = Arc::new(RwLock::new(HashMap::new()));
	let session = Arc::new(RwLock::new(Session::default().with_rt(true)));

	let router_state = RouterState {
		kvs,
		vars,
		live_queries,
		session,
		transactions: Arc::new(RwLock::new(HashMap::new())),
	};

	let canceller = CancellationToken::new();

	let mut opt = EngineOptions::default();
	if let Some(interval) = address.config.node_membership_refresh_interval {
		opt.node_membership_refresh_interval = interval;
	}
	if let Some(interval) = address.config.node_membership_check_interval {
		opt.node_membership_check_interval = interval;
	}
	if let Some(interval) = address.config.node_membership_cleanup_interval {
		opt.node_membership_cleanup_interval = interval;
	}
	if let Some(interval) = address.config.changefeed_gc_interval {
		opt.changefeed_gc_interval = interval;
	}
	let tasks = tasks::init(kvs.clone(), canceller.clone(), &opt);

	let mut notifications = kvs.notifications().map(Box::pin);
	let mut notification_stream = poll_fn(move |cx| match &mut notifications {
		Some(rx) => rx.poll_next_unpin(cx),
		// return poll pending so that this future is never woken up again and therefore not
		// constantly polled.
		None => Poll::Pending,
	});

	loop {
		let router_state = router_state.clone();

		tokio::select! {
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};
				tokio::spawn(async move {
					match super::router(route.request, router_state)
						.await
					{
						Ok(value) => {
							route.response.send(Ok(value)).await.ok();
						}
						Err(error) => {
							// Convert crate::Error to DbResultError
							let db_error = match error {
								crate::Error::Query(msg) => surrealdb_core::rpc::DbResultError::Thrown(msg),
								crate::Error::Http(msg) => surrealdb_core::rpc::DbResultError::InternalError(format!("HTTP error: {}", msg)),
								crate::Error::Ws(msg) => surrealdb_core::rpc::DbResultError::InternalError(format!("WebSocket error: {}", msg)),
								crate::Error::Scheme(msg) => surrealdb_core::rpc::DbResultError::InvalidRequest(format!("Unsupported scheme: {}", msg)),
								crate::Error::ConnectionUninitialised => surrealdb_core::rpc::DbResultError::InternalError("Connection uninitialised".to_string()),
								crate::Error::AlreadyConnected => surrealdb_core::rpc::DbResultError::InternalError("Already connected".to_string()),
								crate::Error::InvalidBindings(_) => surrealdb_core::rpc::DbResultError::InvalidParams("Invalid bindings".to_string()),
								crate::Error::RangeOnRecordId => surrealdb_core::rpc::DbResultError::InvalidParams("Range on record ID not supported".to_string()),
								crate::Error::RangeOnObject => surrealdb_core::rpc::DbResultError::InvalidParams("Range on object not supported".to_string()),
								crate::Error::RangeOnArray => surrealdb_core::rpc::DbResultError::InvalidParams("Range on array not supported".to_string()),
								crate::Error::RangeOnEdges => surrealdb_core::rpc::DbResultError::InvalidParams("Range on edges not supported".to_string()),
								crate::Error::RangeOnRange => surrealdb_core::rpc::DbResultError::InvalidParams("Range on range not supported".to_string()),
								crate::Error::TableColonId { table } => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Table name '{}' contains colon", table)),
								crate::Error::DuplicateRequestId(id) => surrealdb_core::rpc::DbResultError::InternalError(format!("Duplicate request ID: {}", id)),
								crate::Error::InvalidRequest(msg) => surrealdb_core::rpc::DbResultError::InvalidRequest(msg),
								crate::Error::InvalidParams(msg) => surrealdb_core::rpc::DbResultError::InvalidParams(msg),
								crate::Error::InternalError(msg) => surrealdb_core::rpc::DbResultError::InternalError(msg),
								crate::Error::ParseError(msg) => surrealdb_core::rpc::DbResultError::ParseError(msg),
								crate::Error::InvalidSemanticVersion(msg) => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Invalid semantic version: {}", msg)),
								crate::Error::InvalidUrl(msg) => surrealdb_core::rpc::DbResultError::InvalidRequest(format!("Invalid URL: {}", msg)),
								crate::Error::FromValue { value: _, error } => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Value conversion error: {}", error)),
								crate::Error::ResponseFromBinary { error: _, .. } => surrealdb_core::rpc::DbResultError::DeserializationError("Binary response deserialization error".to_string()),
								crate::Error::ToJsonString { value: _, error } => surrealdb_core::rpc::DbResultError::SerializationError(format!("JSON serialization error: {}", error)),
								crate::Error::FromJsonString { string: _, error } => surrealdb_core::rpc::DbResultError::DeserializationError(format!("JSON deserialization error: {}", error)),
								crate::Error::InvalidNsName(name) => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Invalid namespace name: {:?}", name)),
								crate::Error::InvalidDbName(name) => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Invalid database name: {:?}", name)),
								crate::Error::FileOpen { path, error } => surrealdb_core::rpc::DbResultError::InternalError(format!("Failed to open file {:?}: {}", path, error)),
								crate::Error::FileRead { path, error } => surrealdb_core::rpc::DbResultError::InternalError(format!("Failed to read file {:?}: {}", path, error)),
								crate::Error::LossyTake(_) => surrealdb_core::rpc::DbResultError::InvalidParams("Lossy take operation".to_string()),
								crate::Error::BackupsNotSupported => surrealdb_core::rpc::DbResultError::MethodNotAllowed("Backups not supported".to_string()),
								crate::Error::VersionMismatch { server_version, supported_versions } => surrealdb_core::rpc::DbResultError::InvalidRequest(format!("Version mismatch: server {} vs supported {}", server_version, supported_versions)),
								crate::Error::BuildMetadataMismatch { server_metadata, supported_metadata } => surrealdb_core::rpc::DbResultError::InvalidRequest(format!("Build metadata mismatch: server {} vs supported {}", server_metadata, supported_metadata)),
								crate::Error::LiveQueriesNotSupported => surrealdb_core::rpc::DbResultError::LiveQueryNotSupported,
								crate::Error::LiveOnObject => surrealdb_core::rpc::DbResultError::BadLiveQueryConfig("Live queries on objects not supported".to_string()),
								crate::Error::LiveOnArray => surrealdb_core::rpc::DbResultError::BadLiveQueryConfig("Live queries on arrays not supported".to_string()),
								crate::Error::LiveOnEdges => surrealdb_core::rpc::DbResultError::BadLiveQueryConfig("Live queries on edges not supported".to_string()),
								crate::Error::NotLiveQuery(idx) => surrealdb_core::rpc::DbResultError::BadLiveQueryConfig(format!("Query statement {} is not a live query", idx)),
								crate::Error::QueryIndexOutOfBounds(idx) => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Query statement {} is out of bounds", idx)),
								crate::Error::ResponseAlreadyTaken => surrealdb_core::rpc::DbResultError::InternalError("Response already taken".to_string()),
								crate::Error::InsertOnObject => surrealdb_core::rpc::DbResultError::InvalidParams("Insert queries on objects are not supported".to_string()),
								crate::Error::InsertOnArray => surrealdb_core::rpc::DbResultError::InvalidParams("Insert queries on arrays are not supported".to_string()),
								crate::Error::InsertOnEdges => surrealdb_core::rpc::DbResultError::InvalidParams("Insert queries on edges are not supported".to_string()),
								crate::Error::InsertOnRange => surrealdb_core::rpc::DbResultError::InvalidParams("Insert queries on ranges are not supported".to_string()),
								crate::Error::CrendentialsNotObject => surrealdb_core::rpc::DbResultError::InvalidParams("Credentials for signin and signup should be an object".to_string()),
								crate::Error::InvalidNetTarget(err) => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Invalid network target: {}", err)),
								crate::Error::InvalidFuncTarget(err) => surrealdb_core::rpc::DbResultError::InvalidParams(format!("Invalid function target: {}", err)),
								crate::Error::SerializeValue(msg) => surrealdb_core::rpc::DbResultError::SerializationError(msg),
								crate::Error::DeSerializeValue(msg) => surrealdb_core::rpc::DbResultError::DeserializationError(msg),
								crate::Error::Serializer(msg) => surrealdb_core::rpc::DbResultError::SerializationError(msg),
								crate::Error::Deserializer(msg) => surrealdb_core::rpc::DbResultError::DeserializationError(msg),
								crate::Error::InvalidResponse(msg) => surrealdb_core::rpc::DbResultError::InternalError(format!("Invalid response: {}", msg)),
								crate::Error::UnserializableValue(msg) => surrealdb_core::rpc::DbResultError::SerializationError(msg),
								crate::Error::ReceivedInvalidValue => surrealdb_core::rpc::DbResultError::InvalidParams("Received invalid value".to_string()),
								crate::Error::VersionsNotSupported(engine) => surrealdb_core::rpc::DbResultError::MethodNotAllowed(format!("The '{}' engine does not support data versioning", engine)),
								crate::Error::MethodNotFound(msg) => surrealdb_core::rpc::DbResultError::MethodNotFound(msg),
								crate::Error::MethodNotAllowed(msg) => surrealdb_core::rpc::DbResultError::MethodNotAllowed(msg),
								crate::Error::BadLiveQueryConfig(msg) => surrealdb_core::rpc::DbResultError::BadLiveQueryConfig(msg),
								crate::Error::BadGraphQLConfig(msg) => surrealdb_core::rpc::DbResultError::BadGraphQLConfig(msg),
								crate::Error::Thrown(msg) => surrealdb_core::rpc::DbResultError::Thrown(msg),
								crate::Error::MessageTooLong(len) => surrealdb_core::rpc::DbResultError::InternalError(format!("Message too long: {}", len)),
								crate::Error::MaxWriteBufferSizeTooSmall => surrealdb_core::rpc::DbResultError::InternalError("Write buffer size too small".to_string()),
							};
							route.response.send(Err(db_error)).await.ok();
						}
					}
				});
			}
			notification = notification_stream.next() => {
				let Some(notification) = notification else {
					// TODO: Maybe we should do something more then ignore a closed notifications
					// channel?
					continue
				};

				tokio::spawn(async move {
					let id = notification.id.0;
					if let Some(sender) = live_queries.read().await.get(&id) {

						if sender.send(Ok(notification)).await.is_err() {
							live_queries.write().await.remove(&id);
							if let Err(error) =
								super::kill_live_query(&kvs, id, &*session.read().await, vars.read().await.clone()).await
							{
								warn!("Failed to kill live query '{id}'; {error}");
							}
						}
					}
				});
			}
		}
	}
	// Shutdown and stop closed tasks
	canceller.cancel();
	// Wait for background tasks to finish
	tasks.resolve().await.ok();
	// Delete this node from the cluster
	kvs.shutdown().await.ok();
}
