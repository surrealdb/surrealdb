use std::collections::HashSet;
use std::sync::Arc;

use async_channel::Receiver;
use reqwest::ClientBuilder;
use surrealdb_core::cnf::SURREALDB_USER_AGENT;
use tokio::sync::watch;

use super::{Client, RouterState};
use crate::conn::{Route, Router};
use crate::engine::SessionError;
use crate::method::BoxFuture;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::opt::Tls;
use crate::opt::{Endpoint, WaitFor};
use crate::{conn, Error, ExtraFeatures, Result, SessionClone, SessionId, Surreal};

impl crate::Connection for Client {}
impl conn::Sealed for Client {
	#[allow(private_interfaces)]
	fn connect(
		address: Endpoint,
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let headers = super::default_headers();
			let config = address.config.clone();

			#[cfg_attr(not(any(feature = "native-tls", feature = "rustls")), expect(unused_mut))]
			let mut builder = ClientBuilder::new().default_headers(headers);

			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			if let Some(tls) = address.config.tls_config {
				builder = match tls {
					#[cfg(feature = "native-tls")]
					Tls::Native(config) => builder.use_preconfigured_tls(config),
					#[cfg(feature = "rustls")]
					Tls::Rust(config) => builder.use_preconfigured_tls(config),
				};
			}

			let client = builder.build()?;

			let base_url = address.url;

			let req = client
				.get(base_url.join("health")?)
				.header(reqwest::header::USER_AGENT, &*SURREALDB_USER_AGENT);
			super::health(req).await?;

			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);

			tokio::spawn(run_router(client, base_url, route_rx, session_clone.receiver.clone()));

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

pub(crate) async fn run_router(
	client: reqwest::Client,
	base_url: url::Url,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	let state = Arc::new(RouterState::new(client, base_url));
	loop {
		tokio::select! {
			biased;

			session = session_rx.recv() => {
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
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};

				let session_id = route.request.session_id;
				let command = route.request.command.clone();

				// Get session state for this session_id
				let session_state = match state.sessions.get(&session_id) {
					Some(Ok(state)) => state,
					Some(Err(error)) => {
						route.response.send(Err(Error::from(error).into())).await.ok();
						continue;
					}
					None => {
						let error = Error::from(SessionError::NotFound(session_id));
						route.response.send(Err(error.into())).await.ok();
						continue;
					}
				};

				// Spawn the request handling in a background task
				// SessionState uses RwLock internally, so we can share the Arc directly
				let router_state = state.clone();
				tokio::spawn(async move {
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

					// Convert api::err::Error to DbResultError
					let db_result = result.map_err(surrealdb_core::rpc::DbResultError::from);
					route.response.send(db_result).await.ok();
				});
			}
		}
	}
}
