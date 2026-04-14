use std::collections::HashSet;
use std::sync::Arc;
use std::task::Poll;

use async_channel::{Receiver, Sender};
use futures::StreamExt;
use futures::stream::poll_fn;
use surrealdb_core::channel::Receiver as CoreReceiver;
use surrealdb_core::iam::Level;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;
use surrealdb_types::Notification;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::conn::{self, Route, Router};
use crate::engine::local::{Db, SessionError};
use crate::engine::tasks;
use crate::method::BoxFuture;
use crate::opt::auth::Root;
use crate::opt::{Endpoint, EndpointKind, WaitFor};
use crate::types::HashMap;
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

			conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;

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

impl Surreal<Db> {
	// This function was introduced by a community PR,
	//
	// It exposes internal types in the public API so it is marked as doc(hidden).
	// This function is not stable nor subject to semver stability guarentees.
	#[doc(hidden)]
	pub async fn unstable_from_datastore(
		canceller: CancellationToken,
		datastore: Arc<Datastore>,
		notifications: Option<CoreReceiver<Notification>>,
		engine: EngineOptions,
	) -> Result<Self> {
		let (route_tx, route_rx) = async_channel::unbounded();
		let (conn_tx, conn_rx) = async_channel::bounded::<Result<()>>(1);
		let session_clone = SessionClone::new();
		let recv = session_clone.receiver.clone();

		tokio::spawn(async move {
			conn_tx.send(Ok(())).await.ok();

			let router_state = super::RouterState {
				kvs: datastore,
				sessions: HashMap::new(),
			};

			let tasks = tasks::init(router_state.kvs.clone(), canceller.clone(), &engine);

			router_loop(&router_state, canceller, tasks, route_rx, recv, notifications).await;

			router_state.kvs.shutdown().await
		});

		conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;

		let mut features = HashSet::new();
		features.insert(ExtraFeatures::Backup);
		features.insert(ExtraFeatures::LiveQueries);

		let waiter = watch::channel(Some(WaitFor::Connection));
		let router = Router {
			features,
			config: crate::opt::Config::default(),
			sender: route_tx,
		};

		Ok((router, waiter, session_clone).into())
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

	let builder = Datastore::builder()
		.with_query_timeout(address.config.query_timeout)
		.with_transaction_timeout(address.config.transaction_timeout)
		.with_auth(configured_root.is_some());

	#[cfg(storage)]
	let builder = builder.with_temporary_directory(address.config.temporary_directory);

	let (notify, builder) = if address.config.capabilities.allows_live_query_notifications() {
		// TODO: Move value to a global somewhere
		let (send, recv) = surrealdb_core::channel::bounded(15_000);
		(Some(recv), builder.with_notify(send))
	} else {
		(None, builder)
	};

	let builder = builder.with_capabilities(address.config.capabilities);

	let kvs = match builder.build_with_path(endpoint).await {
		Ok(kvs) => {
			if let Err(error) = kvs.check_version().await {
				conn_tx.send(Err(crate::Error::internal(error.to_string()))).await.ok();
				return;
			};
			if let Err(error) = kvs.bootstrap().await {
				conn_tx.send(Err(crate::Error::internal(error.to_string()))).await.ok();
				return;
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = &configured_root
				&& let Err(error) = kvs.initialise_credentials(&root.username, &root.password).await
			{
				conn_tx.send(Err(crate::Error::internal(error.to_string()))).await.ok();
				return;
			}
			conn_tx.send(Ok(())).await.ok();
			kvs
		}
		Err(error) => {
			conn_tx.send(Err(crate::Error::internal(error.to_string()))).await.ok();
			return;
		}
	};

	let router_state = super::RouterState {
		kvs: Arc::new(kvs),
		sessions: HashMap::new(),
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

	router_loop(&router_state, canceller, tasks, route_rx, session_rx, notify).await;

	router_state.kvs.shutdown().await.ok();
}

async fn router_loop(
	router_state: &super::RouterState,
	canceller: CancellationToken,
	tasks: tasks::Tasks,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
	notification: Option<Receiver<Notification>>,
) {
	let mut notifications = notification.map(Box::pin);
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
						router_state.handle_session_initial(session_id);
					}
					SessionId::Clone { old, new } => {
						router_state.handle_session_clone(old, new).await;
					}
					SessionId::Drop(session_id) => {
						router_state.handle_session_drop(session_id);
					}
				}
			}
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};
				match router_state.sessions.get(&route.request.session_id) {
					Some(Ok(state)) => {
						let kvs = router_state.kvs.clone();
						tokio::spawn(async move {
							match super::router(&kvs, &state, route.request.command)
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
					Some(Err(error)) => {
						route.response.send(Err(crate::engine::session_error_to_error(error))).await.ok();
					}
					None => {
						let error = crate::engine::session_error_to_error(SessionError::NotFound(route.request.session_id));
						route.response.send(Err(error)).await.ok();
					}
				}
			}
			notification = notification_stream.next() => {
				let Some(notification) = notification else {
					continue
				};
				let Some(session_id) = notification.session.map(|x| x.into_inner()) else {
					continue
				};

				let live_query_id = notification.id.into_inner();

				match router_state.sessions.get(&session_id) {
					Some(Ok(state)) => {
						match state.live_queries.get(&live_query_id) {
							Some(sender) => {
								let kvs = router_state.kvs.clone();
								let vars = state.vars.read().await.clone();
								let session = state.session.read().await.clone();
								tokio::spawn(async move {
									if sender.send(Ok(notification)).await.is_err() {
										state.live_queries.remove(&live_query_id);
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
					Some(Err(error)) => {
						warn!("Failed to find session '{session_id:?}' for live query '{live_query_id}'; {error:?}");
					}
					None => {
						let error = crate::engine::session_error_to_error(SessionError::NotFound(session_id));
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
}
