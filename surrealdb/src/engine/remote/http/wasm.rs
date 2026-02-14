use std::collections::HashSet;
use std::sync::Arc;

use async_channel::{Receiver, Sender};
use futures::FutureExt;
use reqwest::ClientBuilder;
use tokio::sync::watch;
use wasm_bindgen_futures::spawn_local;

use super::{Client, RouterState};
use crate::conn::{Route, Router};
use crate::engine::{session_error_to_error, SessionError};
use crate::method::BoxFuture;
use crate::opt::{Endpoint, WaitFor};
use crate::{Error, ExtraFeatures, Result, SessionClone, SessionId, Surreal, conn};

impl crate::Connection for Client {}
impl conn::Sealed for Client {
	#[allow(private_interfaces)]
	fn connect(
		address: Endpoint,
		capacity: usize,
		session_clone: Option<SessionClone>,
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
			features.insert(ExtraFeatures::Backup);

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

async fn create_client(base_url: &url::Url) -> Result<reqwest::Client> {
	let headers = super::default_headers();
	let builder = ClientBuilder::new().default_headers(headers);
	let client = builder.build()?;
	let health = base_url.join("health")?;
	super::health(client.get(health)).await?;
	Ok(client)
}

pub(crate) async fn run_router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	let base_url = address.url;

	let client = match create_client(&base_url).await {
		Ok(client) => {
			conn_tx.send(Ok(())).await.ok();
			client
		}
		Err(error) => {
			conn_tx.send(Err(error)).await.ok();
			return;
		}
	};

	let state = Arc::new(RouterState::new(client, base_url));

	loop {
		futures::select! {
			session = session_rx.recv().fuse() => {
				let Ok(session_id) = session else {
					break
				};
				match session_id {
					SessionId::Initial(session_id) => {
						state.handle_session_initial(session_id).await;
					}
					SessionId::Clone { old, new } => {
						state.handle_session_clone(old, new).await;
					}
					SessionId::Drop(session_id) => {
						state.handle_session_drop(session_id).await;
					}
				}
			}
			route = route_rx.recv().fuse() => {
				let Ok(route) = route else {
					break
				};

				let session_id = route.request.session_id;
				let command = route.request.command.clone();

				// Get session state for this session_id
				let session_state = match state.sessions.get(&session_id) {
					Some(Ok(state)) => state,
					Some(Err(error)) => {
						route.response.send(Err(error)).await.ok();
						continue;
					}
					None => {
						let error = session_error_to_error(SessionError::NotFound(session_id));
						route.response.send(Err(error.into())).await.ok();
						continue;
					}
				};

				// Spawn the request handling in a background task
				// SessionState uses RwLock internally, so we can share the Arc directly
				let router_state = state.clone();
				spawn_local(async move {
					let result = super::router(
						route.request,
						&router_state.base_url,
						&router_state.client,
						&session_state,
					)
					.await;

					// On success, add replayable commands to the replay list
					// boxcar::Vec is lock-free, so this is safe to do concurrently
					if result.is_ok() && command.replayable() {
						session_state.replay.push(command);
					}

					match result {
						Ok(value) => {
							route.response.send(Ok(value)).await.ok();
						}
						Err(error) => {
							route.response.send(Err(error.into())).await.ok();
						}
					}
				});
			}
		}
	}
}
