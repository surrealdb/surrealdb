//! The engine's route loop on Wasm targets.

use std::sync::Arc;
use std::task::Poll;

use async_channel::{Receiver, Sender};
use futures::stream::poll_fn;
use futures::{FutureExt, StreamExt};
use surrealdb_core::kvs::Datastore;
use surrealdb_engine_api::{Route, SessionError, SessionId, session_error_to_error};
use surrealdb_types::Error;
use tokio_util::sync::CancellationToken;
use wasm_bindgen_futures::spawn_local;

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
		// The datastore starts its own maintenance tasks, so the cadences go to
		// the builder rather than to a `tasks::init` call here.
		.with_engine_options(opt)
		.with_query_timeout(config.query_timeout)
		.with_transaction_timeout(config.transaction_timeout)
		.with_auth(config.root.is_some());

	let (notify, builder) = if config.capabilities.allows_live_query_notifications() {
		let (send, recv) = async_channel::bounded(surrealdb_core::cnf::NOTIFICATIONS_CHANNEL_SIZE);
		(Some(recv), builder.with_notify(send))
	} else {
		(None, builder)
	};

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

	let router_state = RouterState::new(kvs);

	let mut notify = notify.map(Box::pin);
	let mut notification_stream = poll_fn(move |cx| match &mut notify {
		Some(rx) => rx.poll_next_unpin(cx),
		// return poll pending so that this future is never woken up again and therefore not
		// constantly polled.
		None => Poll::Pending,
	});

	#[allow(unreachable_code)]
	loop {
		// use the less ergonomic futures::select as tokio::select is not available.
		futures::select! {
			session = session_rx.recv().fuse() => {
				let Ok(session_id) = session else {
					break
				};
				router_state.handle_session(session_id).await;
			}
			route = route_rx.recv().fuse() => {
				let Ok(route) = route else {
					// termination requested
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
						let kvs = router_state.kvs.clone();
						spawn_local(async move {
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
						route.response
							.send(Err(session_error_to_error(error)))
							.await
							.ok();
					}
				}
			}
			notification = notification_stream.next().fuse() => {
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
								spawn_local(async move {
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
	// Stops the datastore's maintenance tasks, then deletes this node from
	// the cluster and shuts the storage engine down.
	router_state.kvs.shutdown().await.ok();
}
