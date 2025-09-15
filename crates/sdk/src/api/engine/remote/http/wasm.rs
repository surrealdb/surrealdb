use std::collections::HashSet;
use std::sync::atomic::AtomicI64;

use async_channel::{Receiver, Sender};
use indexmap::IndexMap;
use reqwest::ClientBuilder;
use reqwest::header::HeaderMap;
use tokio::sync::watch;
use url::Url;
use wasm_bindgen_futures::spawn_local;

use super::Client;
use crate::api::conn::{Route, Router};
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
use crate::api::{Result, Surreal, conn};
use crate::opt::WaitFor;

impl crate::api::Connection for Client {}
impl conn::Sealed for Client {
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded(1);
			let config = address.config.clone();

			spawn_local(run_router(address, conn_tx, route_rx));

			conn_rx.recv().await??;

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features: HashSet::new(),
				config,
				sender: route_tx,
				last_id: AtomicI64::new(0),
			};

			Ok((router, waiter).into())
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
