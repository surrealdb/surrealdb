use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI64;

use async_channel::{Receiver, Sender};
use indexmap::IndexMap;
use reqwest::ClientBuilder;
use reqwest::header::HeaderMap;
use tokio::sync::watch;
use url::Url;
use uuid::Uuid;
use wasm_bindgen_futures::spawn_local;

use super::{Auth, Client};
use crate::conn::{Route, Router};
use crate::method::BoxFuture;
use crate::opt::{Endpoint, WaitFor};
use crate::types::Value;
use crate::{Result, SessionId, Surreal, conn};

/// Per-session state for HTTP connections
#[derive(Debug, Default, Clone)]
struct SessionState {
	headers: HeaderMap,
	vars: IndexMap<String, Value>,
	auth: Option<Auth>,
}

impl crate::Connection for Client {}
impl conn::Sealed for Client {
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
			let session_clone = session_clone.unwrap_or_else(crate::SessionClone::new);

			spawn_local(run_router(address, conn_tx, route_rx, session_clone.receiver.clone()));

			conn_rx.recv().await??;

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features: HashSet::new(),
				config,
				sender: route_tx,
				last_id: AtomicI64::new(0),
			};

			Ok((router, waiter, session_clone).into())
		})
	}
}

async fn client(base_url: &Url) -> Result<reqwest::Client> {
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

	let client = match client(&base_url).await {
		Ok(client) => {
			let _ = conn_tx.send(Ok(())).await;
			client
		}
		Err(error) => {
			let _ = conn_tx.send(Err(error)).await;
			return;
		}
	};

	// Store per-session state
	let mut sessions: HashMap<Option<Uuid>, SessionState> = HashMap::new();

	loop {
		use futures::FutureExt;

		futures::select! {
			biased;

			session = session_rx.recv().fuse() => {
				let Ok(session_id) = session else {
					break
				};
				match session_id {
					SessionId::Initial(session_id) => {
						sessions.entry(Some(session_id)).or_default();
					}
					SessionId::Clone { old, new } => {
						let old_state = sessions.get(&Some(old)).cloned().unwrap_or_default();
						sessions.insert(Some(new), old_state);
					}
				}
			}
			route = route_rx.recv().fuse() => {
				let Ok(route) = route else {
					break
				};

				let session_id = route.request.session_id;

				// Get or create session state for this session_id
				let session_state = sessions.entry(session_id).or_default();

				match super::router(
					route.request,
					&base_url,
					&client,
					&mut session_state.headers,
					&mut session_state.vars,
					&mut session_state.auth,
				)
				.await
				{
					Ok(value) => {
						let _ = route.response.send(Ok(value)).await;
					}
					Err(error) => {
						let _ = route.response.send(Err(error.into())).await;
					}
				}
			}
		}
	}
}
