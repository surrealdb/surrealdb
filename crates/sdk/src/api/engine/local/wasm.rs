use crate::api::conn::Connection;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engine::local::Db;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Session;
use crate::engine::tasks;
use crate::iam::Level;
use crate::kvs::Datastore;
use crate::opt::auth::Root;
use crate::opt::WaitFor;
use crate::options::EngineOptions;
use crate::{Action, Notification};
use channel::{Receiver, Sender};
use futures::stream::poll_fn;
use futures::FutureExt;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::Poll;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use wasm_bindgen_futures::spawn_local;

impl crate::api::Connection for Db {}

impl Connection for Db {
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => channel::unbounded(),
				capacity => channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = channel::bounded(1);

			spawn_local(run_router(address, conn_tx, route_rx));

			conn_rx.recv().await??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::LiveQueries);

			Ok(Surreal::new_from_router_waiter(
				Arc::new(OnceLock::with_value(Router {
					features,
					sender: route_tx,
					last_id: AtomicI64::new(0),
				})),
				Arc::new(watch::channel(Some(WaitFor::Connection))),
			))
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

	let kvs = match Datastore::new(&address.path).await {
		Ok(kvs) => {
			if let Err(error) = kvs.bootstrap().await {
				let _ = conn_tx.send(Err(error.into())).await;
				return;
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = configured_root {
				if let Err(error) = kvs.initialise_credentials(root.username, root.password).await {
					let _ = conn_tx.send(Err(error.into())).await;
					return;
				}
			}
			let _ = conn_tx.send(Ok(())).await;
			kvs.with_auth_enabled(configured_root.is_some())
		}
		Err(error) => {
			let _ = conn_tx.send(Err(error.into())).await;
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

	let kvs = Arc::new(kvs);
	let mut vars = BTreeMap::new();
	let mut live_queries = HashMap::new();
	let mut session = Session::default().with_rt(true);

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

	let mut notifications = kvs.notifications();
	let mut notification_stream = poll_fn(move |cx| match &mut notifications {
		Some(rx) => rx.poll_next_unpin(cx),
		None => Poll::Pending,
	});

	loop {
		// use the less ergonomic futures::select as tokio::select is not available.
		futures::select! {
			route = route_rx.recv().fuse() => {
				let Ok(route) = route else {
					// termination requested
					break
				};

				match super::router(
					route.request,
					&kvs,
					&mut session,
					&mut vars,
					&mut live_queries,
				)
				.await
				{
					Ok(value) => {
						let _ = route.response.send(Ok(value)).await;
					}
					Err(error) => {
						let _ = route.response.send(Err(error)).await;
					}
				}
			}
			notification = notification_stream.next().fuse() => {
				let Some(notification) = notification else {
					// TODO: maybe do something else then ignore a disconnected notification
					// channel.
					continue;
				};

				let id = notification.id;
				if let Some(sender) = live_queries.get(&id) {

					let notification = Notification {
						query_id: notification.id.0,
						action: Action::from_core(notification.action),
						data: notification.result,
					};

					if sender.send(notification).await.is_err() {
						live_queries.remove(&id);
						if let Err(error) =
							super::kill_live_query(&kvs, *id, &session, vars.clone()).await
						{
							warn!("Failed to kill live query '{id}'; {error}");
						}
					}
				}
			}
		}
	}
	// Shutdown and stop closed tasks
	canceller.cancel();
	// Wait for background tasks to finish
	let _ = tasks.resolve().await;
	// Delete this node from the cluster
	let _ = kvs.delete_node(kvs.id()).await;
}
