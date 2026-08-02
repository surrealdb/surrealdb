//! The engine's route loop on native targets.

use std::sync::Arc;
use std::task::Poll;

use async_channel::{Receiver, Sender};
use futures::StreamExt;
use futures::stream::poll_fn;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;
use surrealdb_engine_api::{Route, SessionError, SessionId, session_error_to_error};
use surrealdb_types::{Error, Notification};
use tokio_util::sync::CancellationToken;

use crate::router::RouterState;
use crate::{LocalConfig, router, tasks};

/// Open the datastore described by `config` and serve routes from it until the
/// route channel closes.
///
/// The first message on `conn_tx` reports whether the datastore opened; the
/// caller must wait for it before handing out the connection.
pub async fn run_router(
	config: LocalConfig,
	conn_tx: Sender<Result<(), Error>>,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	let opt = config.engine_options();

	let builder = Datastore::builder()
		.with_query_timeout(config.query_timeout)
		.with_transaction_timeout(config.transaction_timeout)
		.with_auth(config.root.is_some());

	#[cfg(storage)]
	let builder = builder.with_temporary_directory(config.temporary_directory);

	let (notify, builder) = if config.capabilities.allows_live_query_notifications() {
		let (send, recv) = async_channel::bounded(surrealdb_core::cnf::NOTIFICATIONS_CHANNEL_SIZE);
		(Some(recv), builder.with_notify(send))
	} else {
		(None, builder)
	};

	let builder = builder.with_capabilities(config.capabilities);

	let kvs = match builder.build_with_path(&config.path).await {
		Ok(kvs) => {
			if let Err(error) = kvs.check_version().await {
				conn_tx.send(Err(Error::internal(error.to_string()))).await.ok();
				return;
			};
			if let Err(error) = kvs.bootstrap().await {
				conn_tx.send(Err(Error::internal(error.to_string()))).await.ok();
				return;
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = &config.root
				&& let Err(error) = kvs.initialise_credentials(&root.username, &root.password).await
			{
				conn_tx.send(Err(Error::internal(error.to_string()))).await.ok();
				return;
			}
			conn_tx.send(Ok(())).await.ok();
			kvs
		}
		Err(error) => {
			conn_tx.send(Err(Error::internal(error.to_string()))).await.ok();
			return;
		}
	};

	let router_state = RouterState::new(Arc::new(kvs));

	let canceller = CancellationToken::new();

	let tasks = tasks::init(Arc::clone(&router_state.kvs), canceller.clone(), &opt);

	router_loop(&router_state, canceller, tasks, route_rx, session_rx, notify).await;

	router_state.kvs.shutdown().await.ok();
}

/// Serve routes from an already-open datastore, shutting it down when the route
/// channel closes.
///
/// The first message on `conn_tx` is sent as soon as the loop is running.
pub async fn run_datastore_router(
	canceller: CancellationToken,
	datastore: Arc<Datastore>,
	notifications: Option<Receiver<Notification>>,
	engine: EngineOptions,
	conn_tx: Sender<Result<(), Error>>,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	conn_tx.send(Ok(())).await.ok();

	let router_state = RouterState::new(datastore);

	let tasks = tasks::init(Arc::clone(&router_state.kvs), canceller.clone(), &engine);

	router_loop(&router_state, canceller, tasks, route_rx, session_rx, notifications).await;

	router_state.kvs.shutdown().await.ok();
}

async fn router_loop(
	router_state: &RouterState,
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
				router_state.handle_session(session_id).await;
			}
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};
				// `resolve_route_session` drains any session-lifecycle events enqueued
				// before this route, so a freshly registered/cloned session is applied
				// before the lookup (see its docs for the ordering guarantee).
				match router_state
					.resolve_route_session(&session_rx, route.request.session_id)
					.await
				{
					Ok(state) => {
						let kvs = Arc::clone(&router_state.kvs);
						tokio::spawn(async move {
							match router::router(&kvs, &state, route.request.command)
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
					Err(error) => {
						route.response.send(Err(session_error_to_error(error))).await.ok();
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
								let kvs = Arc::clone(&router_state.kvs);
								let vars = state.vars.read().await.clone();
								let session = state.session.read().await.clone();
								tokio::spawn(async move {
									if sender.send(Ok(notification)).await.is_err() {
										state.live_queries.remove(&live_query_id);
										if let Err(error) =
											router::kill_live_query(&kvs, live_query_id, &session, vars).await
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
						let error = session_error_to_error(SessionError::NotFound(session_id));
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
