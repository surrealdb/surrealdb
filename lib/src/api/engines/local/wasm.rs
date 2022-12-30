use super::LOG;
use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engines::local::Db;
use crate::api::opt::from_value;
use crate::api::opt::Endpoint;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Session;
use crate::kvs::Datastore;
use flume::Receiver;
use flume::Sender;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use wasm_bindgen_futures::spawn_local;

impl crate::api::Connection for Db {}

impl Connection for Db {
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

			Ok(Surreal {
				router: OnceCell::with_value(Arc::new(Router {
					features: HashSet::new(),
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

pub(crate) fn router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Option<Route>>,
) {
	spawn_local(async move {
		let url = address.endpoint;

		let path = match url.scheme() {
			"mem" => "memory",
			_ => url.as_str(),
		};

		let kvs = match Datastore::new(path).await {
			Ok(kvs) => {
				let _ = conn_tx.into_send_async(Ok(())).await;
				kvs
			}
			Err(error) => {
				let _ = conn_tx.into_send_async(Err(error.into())).await;
				return;
			}
		};

		let mut session = Session::for_kv();
		let mut vars = BTreeMap::new();
		let mut stream = route_rx.into_stream();

		while let Some(Some(route)) = stream.next().await {
			match super::router(route.request, &kvs, &mut session, &mut vars, address.strict).await
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
