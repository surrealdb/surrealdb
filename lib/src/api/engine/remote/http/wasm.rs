use super::Client;
use crate::api::conn::Connection;
use crate::api::conn::Method;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::opt::WaitFor;
use channel::{Receiver, Sender};
use futures::future::BoxFuture;
use futures::StreamExt;
use indexmap::IndexMap;
use reqwest::header::HeaderMap;
use reqwest::ClientBuilder;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;
use url::Url;
use wasm_bindgen_futures::spawn_local;

impl crate::api::Connection for Client {}

impl Connection for Client {
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => channel::unbounded(),
				capacity => channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = channel::bounded(1);

			spawn_local(run_router(address, conn_tx, route_rx));

			conn_rx.recv().await??;

			Ok(Surreal::new_from_router_waiter(
				Arc::new(OnceLock::with_value(Router {
					features: HashSet::new(),
					sender: route_tx,
					last_id: AtomicI64::new(0),
				})),
				Arc::new(watch::channel(Some(WaitFor::Connection))),
			))
		})
	}
}

async fn client(base_url: &Url) -> Result<reqwest::Client> {
	let headers = super::default_headers();
	let builder = ClientBuilder::new().default_headers(headers);
	let client = builder.build()?;
	let health = base_url.join(Method::Health.as_str())?;
	super::health(client.get(health)).await?;
	Ok(client)
}

pub(crate) async fn run_router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
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

	let mut headers = HeaderMap::new();
	let mut vars = IndexMap::new();
	let mut auth = None;

	while let Ok(route) = route_rx.recv().await {
		match super::router(route.request, &base_url, &client, &mut headers, &mut vars, &mut auth)
			.await
		{
			Ok(value) => {
				let _ = route.response.send(Ok(value)).await;
			}
			Err(error) => {
				let _ = route.response.send(Err(error)).await;
			}
		}
	}
}
