use super::Client;
use super::LOG;
use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use flume::Receiver;
use flume::Sender;
use futures::StreamExt;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use reqwest::header::HeaderMap;
use reqwest::ClientBuilder;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use url::Url;
use wasm_bindgen_futures::spawn_local;

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
			let (route_tx, route_rx) = match capacity {
				0 => flume::unbounded(),
				capacity => flume::bounded(capacity),
			};

			let (conn_tx, conn_rx) = flume::bounded(1);

			router(address, conn_tx, route_rx);

			if let Err(error) = conn_rx.into_recv_async().await? {
				return Err(error);
			}

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Auth);

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
			trace!(target: LOG, "{param:?}");
			let route = Route {
				request: (0, self.method, param),
				response: sender,
			};
			router.sender.send_async(Some(route)).await?;
			Ok(receiver)
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

pub(crate) fn router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Option<Route>>,
) {
	spawn_local(async move {
		let base_url = address.endpoint;

		let client = match client(&base_url).await {
			Ok(client) => {
				let _ = conn_tx.into_send_async(Ok(())).await;
				client
			}
			Err(error) => {
				let _ = conn_tx.into_send_async(Err(error.into())).await;
				return;
			}
		};

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
