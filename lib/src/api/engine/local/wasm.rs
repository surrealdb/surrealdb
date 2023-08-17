use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engine::local::Db;
use crate::api::opt::Endpoint;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Session;
use crate::iam::Level;
use crate::kvs::Datastore;
use crate::opt::auth::Root;
use flume::Receiver;
use flume::Sender;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
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

			conn_rx.into_recv_async().await??;

			Ok(Surreal {
				router: Arc::new(OnceLock::with_value(Router {
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
}

pub(crate) fn router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Option<Route>>,
) {
	spawn_local(async move {
		let url = address.endpoint;
		let configured_root = match address.auth {
			Level::Root => Some(Root {
				username: &address.username,
				password: &address.password,
			}),
			_ => None,
		};

		let path = match url.scheme() {
			"mem" => "memory",
			_ => url.as_str(),
		};

		let kvs = match Datastore::new(path).await {
			Ok(kvs) => {
				// If a root user is specified, setup the initial datastore credentials
				if let Some(root) = configured_root {
					if let Err(error) = kvs.setup_initial_creds(root).await {
						let _ = conn_tx.into_send_async(Err(error.into())).await;
						return;
					}
				}

				let _ = conn_tx.into_send_async(Ok(())).await;
				kvs.with_auth_enabled(configured_root.is_some())
			}
			Err(error) => {
				let _ = conn_tx.into_send_async(Err(error.into())).await;
				return;
			}
		};

		let kvs = kvs
			.with_strict_mode(address.config.strict)
			.with_query_timeout(address.config.query_timeout)
			.with_transaction_timeout(address.config.transaction_timeout);

		let kvs = match address.config.notifications {
			true => kvs.with_notifications(),
			false => kvs,
		};

		let mut vars = BTreeMap::new();
		let mut stream = route_rx.into_stream();
		let mut session = Session::default();

		while let Some(Some(route)) = stream.next().await {
			match super::router(route.request, &kvs, &mut session, &mut vars).await {
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
