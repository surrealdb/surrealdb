use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::task::Poll;

use async_channel::{Receiver, Sender};
use futures::stream::poll_fn;
use futures::{FutureExt, StreamExt};
use surrealdb_core::iam::Level;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;
use wasm_bindgen_futures::spawn_local;

use crate::conn::{Route, Router};
use crate::engine::local::{Db, LiveQueryState};
use crate::engine::tasks;
use crate::method::BoxFuture;
use crate::opt::auth::Root;
use crate::opt::{Endpoint, WaitFor};
use crate::{ExtraFeatures, Result, SessionClone, SessionId, Surreal, conn};

impl crate::Connection for Db {}
impl conn::Sealed for Db {
	#[allow(private_interfaces)]
	fn connect(
		address: Endpoint,
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded(1);
			let config = address.config.clone();
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);

			spawn_local(run_router(address, conn_tx, route_rx, session_clone.receiver.clone()));

			conn_rx.recv().await??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features,
				config,
				sender: route_tx,
				last_id: AtomicI64::new(0),
			};

			Ok((router, waiter, session_clone).into())
		})
	}
}

pub(crate) async fn run_router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	let configured_root = match address.config.auth {
		Level::Root => Some(Root {
			username: address.config.username.clone(),
			password: address.config.password.clone(),
		}),
		_ => None,
	};

	let kvs = match Datastore::new(&address.path).await {
		Ok(kvs) => {
			if let Err(error) = kvs.bootstrap().await {
				conn_tx.send(Err(error.into())).await.ok();
				return;
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(ref root) = configured_root {
				if let Err(error) = kvs.initialise_credentials(&root.username, &root.password).await
				{
					conn_tx.send(Err(error.into())).await.ok();
					return;
				}
			}
			conn_tx.send(Ok(())).await.ok();
			kvs.with_auth_enabled(configured_root.is_some())
		}
		Err(error) => {
			conn_tx.send(Err(error.into())).await.ok();
			return;
		}
	};

	let kvs = match address.config.capabilities.allows_live_query_notifications() {
		true => kvs.with_notifications(),
		false => kvs,
	};

	let kvs = kvs
		.with_query_timeout(address.config.query_timeout)
		.with_transaction_timeout(address.config.transaction_timeout)
		.with_capabilities(address.config.capabilities);

	let kvs = Arc::new(kvs);
	let live_queries = Arc::new(RwLock::new(super::LiveQueryMap::new()));
	let sessions = Arc::new(RwLock::new(HashMap::new()));

	let router_state = super::RouterState {
		kvs: kvs.clone(),
		live_queries: live_queries.clone(),
		sessions: sessions.clone(),
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
		None => Poll::Pending,
	});

	#[allow(unreachable_code)]
	loop {
		let router_state = router_state.clone();

		// use the less ergonomic futures::select as tokio::select is not available.
		futures::select! {
			session = session_rx.recv().fuse() => {
				let Ok(session_id) = session else {
					break
				};
				match session_id {
					SessionId::Initial(session_id) => {
						router_state.sessions.write().await.insert(session_id, Default::default());
					}
					SessionId::Clone { old, new } => {
						let state = router_state.sessions.read().await.get(&old).cloned().unwrap_or_default();
						router_state.sessions.write().await.insert(new, state);
					}
				}
			}
			route = route_rx.recv().fuse() => {
				let Ok(route) = route else {
					// termination requested
					break
				};

				match super::router(route.request, router_state).await {
					Ok(value) => {
						route.response.send(Ok(value)).await.ok();
					}
					Err(error) => {
						route.response.send(Err(error.into())).await.ok();
					}
				}
			}
			notification = notification_stream.next().fuse() => {
				let Some(notification) = notification else {
					// TODO: maybe do something else then ignore a disconnected notification
					// channel.
					continue;
				};

				let id = notification.id.0;
				if let Some(LiveQueryState { session_id, sender }) = live_queries.read().await.get(&id) {
					let session_id = *session_id;
					if sender.send(Ok(notification)).await.is_err() {
						live_queries.write().await.remove(&id);
						let sessions_lock = sessions.read().await;
						let state = sessions_lock.get(&session_id).cloned().unwrap_or_default();
						if let Err(error) =
							super::kill_live_query(&kvs, id, &state.session, state.vars).await
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
	tasks.resolve().await.ok();
	// Delete this node from the cluster
	kvs.shutdown().await.ok();
}
