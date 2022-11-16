use super::Client;
use super::HttpRoute;
use crate::param::from_value;
use crate::param::DbResponse;
use crate::param::Param;
use crate::param::ServerAddrs;
use crate::Connection;
use crate::Method;
use crate::Result;
use crate::Route;
use crate::Router;
use crate::Surreal;
use async_trait::async_trait;
use flume::Receiver;
use flume::Sender;
use futures::StreamExt;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::ACCEPT;
use reqwest::ClientBuilder;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
#[cfg(feature = "ws")]
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use surrealdb::sql::Value;
use url::Url;
use wasm_bindgen_futures::spawn_local;

#[async_trait]
impl Connection for Client {
	type Request = (Method, Param);
	type Response = Result<DbResponse>;

	fn new(method: Method) -> Self {
		Self {
			method,
		}
	}

	async fn connect(address: ServerAddrs, capacity: usize) -> Result<Surreal<Self>> {
		let (route_tx, route_rx) = match capacity {
			0 => flume::unbounded(),
			capacity => flume::bounded(capacity),
		};

		let (conn_tx, conn_rx) = flume::bounded(1);

		router(address, conn_tx, route_rx);

		if let Err(error) = conn_rx.into_recv_async().await? {
			return Err(error);
		}

		Ok(Surreal {
			router: OnceCell::with_value(Arc::new(Router {
				conn: PhantomData,
				sender: route_tx,
				#[cfg(feature = "ws")]
				last_id: AtomicI64::new(0),
			})),
		})
	}

	async fn send(
		&mut self,
		router: &Router<Self>,
		param: Param,
	) -> Result<Receiver<Self::Response>> {
		let (sender, receiver) = flume::bounded(1);
		tracing::trace!("{param:?}");
		let route = Route {
			request: (self.method, param),
			response: sender,
		};
		router.sender.send_async(Some(route)).await?;
		Ok(receiver)
	}

	async fn recv<R>(&mut self, rx: Receiver<Self::Response>) -> Result<R>
	where
		R: DeserializeOwned,
	{
		let response = rx.into_recv_async().await?;
		tracing::trace!("Response {response:?}");
		match response? {
			DbResponse::Other(value) => from_value(&value),
			DbResponse::Query(..) => unreachable!(),
		}
	}

	async fn recv_query(
		&mut self,
		rx: Receiver<Self::Response>,
	) -> Result<Vec<Result<Vec<Value>>>> {
		let response = rx.into_recv_async().await?;
		tracing::trace!("Response {response:?}");
		match response? {
			DbResponse::Query(results) => Ok(results),
			DbResponse::Other(..) => unreachable!(),
		}
	}
}

async fn client(base_url: &Url) -> Result<reqwest::Client> {
	let mut headers = HeaderMap::new();
	headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
	let builder = ClientBuilder::new().default_headers(headers);
	let client = builder.build()?;
	let health = base_url.join(Method::Health.as_str())?;
	super::health(client.get(health)).await?;
	Ok(client)
}

fn router(
	address: ServerAddrs,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Option<HttpRoute>>,
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
