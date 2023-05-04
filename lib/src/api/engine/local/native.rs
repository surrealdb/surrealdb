use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engine::local::Db;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Session;
use crate::kvs::Datastore;
use flume::Receiver;
use flume::Sender;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

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

			conn_rx.into_recv_async().await??;

			let mut features = HashSet::new();
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
}

pub(crate) fn router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Option<Route>>,
) {
	tokio::spawn(async move {
		let url = address.endpoint;

		let kvs = {
			let path = match url.scheme() {
				"mem" => "memory".to_owned(),
				"fdb" | "rocksdb" | "file" => match url.to_file_path() {
					Ok(path) => format!("{}://{}", url.scheme(), path.display()),
					Err(_) => {
						let error = Error::InvalidUrl(url.as_str().to_owned());
						let _ = conn_tx.into_send_async(Err(error.into())).await;
						return;
					}
				},
				_ => url.as_str().to_owned(),
			};

			match Datastore::new(&path).await {
				Ok(kvs) => {
					let _ = conn_tx.into_send_async(Ok(())).await;
					kvs
				}
				Err(error) => {
					let _ = conn_tx.into_send_async(Err(error.into())).await;
					return;
				}
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
