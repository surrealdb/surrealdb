use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engine::local::Db;
use crate::api::engine::local::DEFAULT_TICK_INTERVAL;
use crate::api::opt::Endpoint;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Session;
use crate::engine::IntervalStream;
use crate::iam::Level;
use crate::kvs::Datastore;
use crate::opt::auth::Root;
use flume::Receiver;
use flume::Sender;
use futures::StreamExt;
use futures_concurrency::stream::Merge as _;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use wasm_bindgen_futures::spawn_local;
use wasmtimer::tokio as time;
use wasmtimer::tokio::MissedTickBehavior;

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
		let configured_root = match address.config.auth {
			Level::Root => Some(Root {
				username: &address.config.username,
				password: &address.config.password,
			}),
			_ => None,
		};

		let kvs = match Datastore::new(&address.path).await {
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

		let kvs = Arc::new(kvs);
		let mut vars = BTreeMap::new();
		let mut stream = route_rx.into_stream();
		let mut session = Session::default();

		let (maintenance_tx, maintenance_rx) = flume::bounded::<()>(1);
		let tick_interval = address.config.tick_interval.unwrap_or(DEFAULT_TICK_INTERVAL);
		run_maintenance(kvs.clone(), tick_interval, maintenance_rx);

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

		// Stop maintenance tasks
		let _ = maintenance_tx.into_send_async(()).await;
	});
}

fn run_maintenance(kvs: Arc<Datastore>, tick_interval: Duration, stop_signal: Receiver<()>) {
	spawn_local(async move {
		let mut interval = time::interval(tick_interval);
		// Don't bombard the database if we miss some ticks
		interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
		// Delay sending the first tick
		interval.tick().await;

		let ticker = IntervalStream::new(interval);

		let streams = (ticker.map(Some), stop_signal.into_stream().map(|_| None));

		let mut stream = streams.merge();

		while let Some(Some(_)) = stream.next().await {
			match kvs.tick().await {
				Ok(()) => trace!("Node agent tick ran successfully"),
				Err(error) => error!("Error running node agent tick: {error}"),
			}
		}
	});
}
