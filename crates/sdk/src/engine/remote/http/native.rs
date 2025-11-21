use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI64;

use async_channel::Receiver;
use indexmap::IndexMap;
use reqwest::ClientBuilder;
use reqwest::header::HeaderMap;
use surrealdb_core::cnf::SURREALDB_USER_AGENT;
use tokio::sync::watch;
use url::Url;
use uuid::Uuid;

use super::{Auth, Client};
use crate::conn::{Route, Router};
use crate::method::BoxFuture;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::opt::Tls;
use crate::opt::{Endpoint, WaitFor};
use crate::types::Value;
use crate::{ExtraFeatures, Result, SessionClone, SessionId, Surreal, conn};

/// Per-session state for HTTP connections
#[derive(Debug, Default, Clone)]
struct SessionState {
	headers: HeaderMap,
	vars: IndexMap<String, Value>,
	auth: Option<Auth>,
}

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

			tokio::spawn(run_router(base_url, client, route_rx, session_clone.receiver.clone()));

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);

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

/// Send a clone_session RPC request to the server asynchronously
async fn send_clone_session_request(
	from: Option<uuid::Uuid>,
	to: uuid::Uuid,
	client: reqwest::Client,
	base_url: url::Url,
) {
	use surrealdb_types::{Array, SurrealValue, Uuid as SurrealUuid, Value};

	let from_value = match from {
		Some(id) => Value::Uuid(SurrealUuid(id)),
		None => Value::None,
	};

	let request = crate::conn::cmd::RouterRequest {
		id: None, // Fire and forget - we don't wait for response
		method: "clone_session",
		params: Some(Value::Array(Array::from(vec![from_value, Value::Uuid(SurrealUuid(to))]))),
		txn: None,
		session_id: None, // Session cloning doesn't use a session ID
	};

	let request_value = request.into_value();
	let payload = surrealdb_core::rpc::format::flatbuffers::encode(&request_value)
		.expect("router request should serialize");

	let result = client.post(base_url.join("rpc").expect("valid URL")).body(payload).send().await;

	if let Err(error) = result {
		warn!("Failed to send clone_session request to server: {error}");
	}
}

pub(crate) async fn run_router(
	base_url: Url,
	client: reqwest::Client,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	// Store per-session state
	let mut sessions: HashMap<Option<Uuid>, SessionState> = HashMap::new();

	loop {
		tokio::select! {
			biased;

			session = session_rx.recv() => {
				let Ok(session_id) = session else {
					break
				};
				match session_id {
				SessionId::Initial(session_id) => {
					sessions.entry(Some(session_id)).or_default();
					// Clone from default session
					send_clone_session_request(None, session_id, client.clone(), base_url.clone()).await;
				}
				SessionId::Clone { old, new } => {
					// Clone the local session state
					let old_state = sessions.get(&Some(old)).cloned().unwrap_or_default();
					sessions.insert(Some(new), old_state);
					// Send clone_session RPC request to server
					send_clone_session_request(Some(old), new, client.clone(), base_url.clone()).await;
				}
				}
			}
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};

				let session_id = route.request.session_id;

				// Get or create session state for this session_id
				let session_state = sessions.entry(session_id).or_default();

				let result = super::router(
					route.request,
					&base_url,
					&client,
					&mut session_state.headers,
					&mut session_state.vars,
					&mut session_state.auth,
				)
				.await;
				// Convert api::err::Error to DbResultError
				let db_result = result.map_err(surrealdb_core::rpc::DbResultError::from);
				let _ = route.response.send(db_result).await;
			}
		}
	}
}
