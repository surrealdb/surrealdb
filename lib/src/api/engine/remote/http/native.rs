use super::Client;
use crate::api::conn::Connection;
use crate::api::conn::Method;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::opt::WaitFor;
use channel::Receiver;
use futures::future::BoxFuture;
use reqwest::header::HeaderMap;
use reqwest::ClientBuilder;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;
use url::Url;

impl crate::api::Connection for Client {}

impl Connection for Client {
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let headers = super::default_headers();

			#[allow(unused_mut)]
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

			super::health(client.get(base_url.join(Method::Health.as_str())?)).await?;

			let (route_tx, route_rx) = match capacity {
				0 => channel::unbounded(),
				capacity => channel::bounded(capacity),
			};

			tokio::spawn(run_router(base_url, client, route_rx));

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);

			Ok(Surreal::new_from_router_waiter(
				Arc::new(OnceLock::with_value(Router {
					features,
					sender: route_tx,
					last_id: AtomicI64::new(0),
				})),
				Arc::new(watch::channel(Some(WaitFor::Connection))),
			))
		})
	}
}

pub(crate) async fn run_router(base_url: Url, client: reqwest::Client, route_rx: Receiver<Route>) {
	let mut headers = HeaderMap::new();
	let mut vars = HashMap::new();
	let mut auth = None;

	while let Ok(route) = route_rx.recv().await {
		let result =
			super::router(route.request, &base_url, &client, &mut headers, &mut vars, &mut auth)
				.await;
		let _ = route.response.send(result).await;
	}
}
