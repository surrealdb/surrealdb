use super::Client;
use super::LOG;
use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::opt::from_value;
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::ExtraFeatures;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use flume::Receiver;
use futures::StreamExt;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::ACCEPT;
use reqwest::ClientBuilder;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use url::Url;

impl crate::api::Connection for Client {}

impl Connection for Client {
	fn new(method: Method) -> Self {
		Self {
			method,
		}
	}

	fn connect(
		address: Endpoint,
		capacity: usize,
	) -> Pin<Box<dyn Future<Output = Result<Surreal<Self>>> + Send + Sync + 'static>> {
		Box::pin(async move {
			let mut headers = HeaderMap::new();
			headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

			#[allow(unused_mut)]
			let mut builder = ClientBuilder::new().default_headers(headers);

			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			if let Some(tls) = address.tls_config {
				builder = match tls {
					#[cfg(feature = "native-tls")]
					Tls::Native(config) => builder.use_preconfigured_tls(config),
					#[cfg(feature = "rustls")]
					Tls::Rust(config) => builder.use_preconfigured_tls(config),
				};
			}

			let client = builder.build()?;

			let base_url = address.endpoint;

			super::health(client.get(base_url.join(Method::Health.as_str())?)).await?;

			let (route_tx, route_rx) = match capacity {
				0 => flume::unbounded(),
				capacity => flume::bounded(capacity),
			};

			router(base_url, client, route_rx);

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Auth);
			features.insert(ExtraFeatures::Backup);

			Ok(Surreal {
				router: OnceCell::with_value(Arc::new(Router {
					features,
					conn: PhantomData,
					sender: route_tx,
					last_id: AtomicI64::new(0),
				})),
			})
		})
	}

	fn send<'r>(
		&'r mut self,
		router: &'r Router<Self>,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Receiver<Result<DbResponse>>>> + Send + Sync + 'r>> {
		Box::pin(async move {
			let (sender, receiver) = flume::bounded(1);
			let route = Route {
				request: (0, self.method, param),
				response: sender,
			};
			router.sender.send_async(Some(route)).await?;
			Ok(receiver)
		})
	}

	fn recv<R>(
		&mut self,
		rx: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<R>> + Send + Sync + '_>>
	where
		R: DeserializeOwned,
	{
		Box::pin(async move {
			let response = rx.into_recv_async().await?;
			trace!(target: LOG, "Response {response:?}");
			match response? {
				DbResponse::Other(value) => from_value(value),
				DbResponse::Query(..) => unreachable!(),
			}
		})
	}

	fn recv_query(
		&mut self,
		rx: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<QueryResponse>> + Send + Sync + '_>> {
		Box::pin(async move {
			let response = rx.into_recv_async().await?;
			trace!(target: LOG, "Response {response:?}");
			match response? {
				DbResponse::Query(results) => Ok(results),
				DbResponse::Other(..) => unreachable!(),
			}
		})
	}
}

pub(crate) fn router(base_url: Url, client: reqwest::Client, route_rx: Receiver<Option<Route>>) {
	tokio::spawn(async move {
		let mut headers = HeaderMap::new();
		let mut vars = IndexMap::new();
		let mut auth = None;
		let mut stream = route_rx.into_stream();

		while let Some(Some(route)) = stream.next().await {
			match super::router(
				route.request,
				&base_url,
				&client,
				&mut headers,
				&mut vars,
				&mut auth,
			)
			.await
			{
				Ok(value) => {
					let _ = route.response.into_send_async(Ok(value)).await;
				}
				Err(error) => {
					let _ = route.response.into_send_async(Err(error)).await;
				}
			}
		}
	});
}
