use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI64;

use async_channel::{Receiver, Sender};
use futures::FutureExt;
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

/// Send a clone_session RPC request to the server asynchronously
fn send_clone_session_request(from: Option<uuid::Uuid>, to: uuid::Uuid, base_url: url::Url) {
	wasm_bindgen_futures::spawn_local(async move {
		use js_sys::Uint8Array;
		use surrealdb_types::{Array, SurrealValue, Uuid as SurrealUuid, Value};
		use wasm_bindgen::JsValue;
		use wasm_bindgen_futures::JsFuture;
		use web_sys::{Request, RequestInit};

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

		let array = Uint8Array::new_with_length(payload.len() as u32);
		array.copy_from(&payload);

		let opts = RequestInit::new();
		opts.set_method("POST");
		let buffer: JsValue = array.buffer().into();
		opts.set_body(&buffer);

		let url = base_url.join("rpc").expect("valid URL").to_string();
		let request = Request::new_with_str_and_init(&url, &opts);

		if let Ok(request) = request {
			let window = web_sys::window().expect("window");
			let resp_promise = window.fetch_with_request(&request);
			if let Err(error) = JsFuture::from(resp_promise).await {
				warn!("Failed to send clone_session request to server: {error:?}");
			}
		}
	});
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
		futures::select! {
			session = session_rx.recv().fuse() => {
				let Ok(session_id) = session else {
					break
				};
				match session_id {
				SessionId::Initial(session_id) => {
					sessions.entry(Some(session_id)).or_default();
					// Clone from default session
					send_clone_session_request(None, session_id, base_url.clone());
				}
				SessionId::Clone { old, new } => {
					// Clone the local session state
					let old_state = sessions.get(&Some(old)).cloned().unwrap_or_default();
					sessions.insert(Some(new), old_state);
					// Send clone_session RPC request to server
					send_clone_session_request(Some(old), new, base_url.clone());
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
