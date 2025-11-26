use std::collections::HashSet;
use std::sync::Arc;
use std::task::Poll;

use async_channel::{Receiver, Sender};
use futures::StreamExt;
use futures::stream::poll_fn;
use papaya::HashMap;
use surrealdb_core::iam::Level;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;

use crate::conn::{self, Route, Router};
use crate::engine::local::{Db, SessionState};
use crate::engine::tasks;
use crate::method::BoxFuture;
use crate::opt::auth::Root;
use crate::opt::{Endpoint, EndpointKind, WaitFor};
use crate::{ExtraFeatures, Result, SessionClone, SessionId, Surreal};

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

			tokio::spawn(run_router(address, conn_tx, route_rx, session_clone.receiver.clone()));

			conn_rx.recv().await??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features,
				config,
				sender: route_tx,
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
			if let Some(root) = &configured_root
				&& let Err(error) = kvs.initialise_credentials(&root.username, &root.password).await
			{
				conn_tx.send(Err(crate::Error::InternalError(error.to_string()))).await.ok();
				return;
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
		.with_query_timeout(address.config.query_timeout)
		.with_transaction_timeout(address.config.transaction_timeout)
		.with_capabilities(address.config.capabilities);

	#[cfg(storage)]
	let kvs = kvs.with_temporary_directory(address.config.temporary_directory);

	let router_state = super::RouterState {
		kvs: Arc::new(kvs),
		sessions: super::Sessions::new(),
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
	let tasks = tasks::init(router_state.kvs.clone(), canceller.clone(), &opt);

	let mut notifications = router_state.kvs.notifications().map(Box::pin);
	let mut notification_stream = poll_fn(move |cx| match &mut notifications {
		Some(rx) => rx.poll_next_unpin(cx),
		// return poll pending so that this future is never woken up again and therefore not
		// constantly polled.
		None => Poll::Pending,
	});

	loop {
		tokio::select! {
			biased;

			session = session_rx.recv() => {
				let Ok(session_id) = session else {
					break
				};
				match session_id {
					SessionId::Initial(session_id) => {
						router_state.sessions.insert(session_id, Ok(Arc::new(SessionState::new(session_id))));
					}
					SessionId::Clone { old, new } => {
						let state = match router_state.sessions.get(&Some(old)) {
							Ok(state) => {
								let mut session = state.session.read().await.clone();
								session.id = Some(new);
								Ok(Arc::new(SessionState {
									session: RwLock::new(session),
									vars: RwLock::new(state.vars.read().await.clone()),
									transactions: HashMap::new(),
									live_queries: HashMap::new()
								}))
							}
							Err(error) => Err(error),
						};
						router_state.sessions.insert(new, state);
					}
					SessionId::Drop(session_id) => {
						router_state.sessions.remove(&session_id);
					}
				}
			}
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};
				match router_state.sessions.get(&route.request.session_id) {
					Ok(state) => {
						let kvs = router_state.kvs.clone();
						tokio::spawn(async move {
							match super::router(&kvs, &state, route.request.command)
								.await
							{
								Ok(value) => {
									route.response.send(Ok(value)).await.ok();
								}
								Err(error) => {
									route.response.send(Err(error.into())).await.ok();
								}
							}
						});
					}
					Err(error) => {
						route.response.send(Err(crate::Error::from(error).into())).await.ok();
					}
				}
			}
			notification = notification_stream.next() => {
				let Some(notification) = notification else {
					continue
				};

				let session_id = notification.session.map(|x| x.0);
				let live_query_id = notification.id.0;

				match router_state.sessions.get(&session_id) {
					Ok(state) => {
						match state.get_live_query(&live_query_id) {
							Some(sender) => {
								let kvs = router_state.kvs.clone();
								let vars = state.vars.read().await.clone();
								let session = state.session.read().await.clone();
								tokio::spawn(async move {
									if sender.send(Ok(notification)).await.is_err() {
										state.remove_live_query(&live_query_id);
										if let Err(error) =
											super::kill_live_query(&kvs, live_query_id, &session, vars).await
										{
											warn!("Failed to kill live query '{live_query_id}'; {error}");
										}
									}
								});
							}
							None => {
								warn!("Failed to find live query '{live_query_id}' for session '{session_id:?}'");
							}
						}
					}
					Err(error) => {
						let error = crate::Error::from(error);
						warn!("Failed to find session '{session_id:?}' for live query '{live_query_id}'; {error}");
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
	router_state.kvs.shutdown().await.ok();
}
