use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::task::Poll;

use async_channel::{Receiver, Sender};
use futures::StreamExt;
use futures::stream::poll_fn;
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;

use crate::api::conn::{self, Route, Router};
use crate::api::engine::local::Db;
use crate::api::method::BoxFuture;
use crate::api::opt::{Endpoint, EndpointKind};
use crate::api::{ExtraFeatures, Result, Surreal};
use crate::core::dbs::{Session, Variables};
use crate::core::iam::Level;
use crate::core::kvs::Datastore;
use crate::core::options::EngineOptions;
use crate::engine::tasks;
use crate::opt::WaitFor;
use crate::opt::auth::Root;

impl crate::api::Connection for Db {}
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

pub(crate) async fn run_router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
) {
	let configured_root = match address.config.auth {
		Level::Root => Some(Root {
			username: &address.config.username,
			password: &address.config.password,
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
				conn_tx.send(Err(error)).await.ok();
				return;
			};
			if let Err(error) = kvs.bootstrap().await {
				conn_tx.send(Err(error)).await.ok();
				return;
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = configured_root {
				if let Err(error) = kvs.initialise_credentials(root.username, root.password).await {
					conn_tx.send(Err(error)).await.ok();
					return;
				}
			}
			conn_tx.send(Ok(())).await.ok();
			kvs.with_auth_enabled(configured_root.is_some())
		}
		Err(error) => {
			conn_tx.send(Err(error)).await.ok();
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
		let kvs = kvs.clone();
		let session = session.clone();
		let vars = vars.clone();
		let live_queries = live_queries.clone();
		tokio::select! {
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};
				tokio::spawn(async move {
					match super::router(route.request, &kvs, &session, &vars, &live_queries)
						.await
					{
						Ok(value) => {
							route.response.send(Ok(value)).await.ok();
						}
						Err(error) => {
							route.response.send(Err(error)).await.ok();
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

						if sender.send(notification).await.is_err() {
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
