use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engine::local::Db;
use crate::api::opt::{Endpoint, EndpointKind};
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Session;
use crate::engine::tasks::start_tasks;
use crate::iam::Level;
use crate::kvs::Datastore;
use crate::opt::auth::Root;
use crate::opt::WaitFor;
use crate::options::EngineOptions;
use flume::Receiver;
use flume::Sender;
use futures::future::Either;
use futures::stream::poll_fn;
use futures::StreamExt;
use futures_concurrency::stream::Merge as _;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::Poll;
use tokio::sync::watch;

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
			features.insert(ExtraFeatures::LiveQueries);

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

	fn send<'r>(
		&'r mut self,
		router: &'r Router,
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
		let configured_root = match address.config.auth {
			Level::Root => Some(Root {
				username: &address.config.username,
				password: &address.config.password,
			}),
			_ => None,
		};

		let endpoint = match EndpointKind::from(address.url.scheme()) {
			EndpointKind::TiKv => address.url.as_str(),
			_ => &address.path,
		};

		let kvs = match Datastore::new(endpoint).await {
			Ok(kvs) => {
				if let Err(error) = kvs.bootstrap().await {
					let _ = conn_tx.into_send_async(Err(error.into())).await;
					return;
				}
				// If a root user is specified, setup the initial datastore credentials
				if let Some(root) = configured_root {
					if let Err(error) = kvs.setup_initial_creds(root.username, root.password).await
					{
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

		let kvs = match address.config.capabilities.allows_live_query_notifications() {
			true => kvs.with_notifications(),
			false => kvs,
		};

		let kvs = kvs
			.with_strict_mode(address.config.strict)
			.with_query_timeout(address.config.query_timeout)
			.with_transaction_timeout(address.config.transaction_timeout)
			.with_capabilities(address.config.capabilities);

		#[cfg(any(
			feature = "kv-mem",
			feature = "kv-surrealkv",
			feature = "kv-rocksdb",
			feature = "kv-fdb",
			feature = "kv-tikv",
		))]
		let kvs = match address.config.temporary_directory {
			Some(tmp_dir) => kvs.with_temporary_directory(tmp_dir),
			_ => kvs,
		};

		let kvs = Arc::new(kvs);
		let mut vars = BTreeMap::new();
		let mut live_queries = HashMap::new();
		let mut session = Session::default().with_rt(true);

		let opt = {
			let mut engine_options = EngineOptions::default();
			engine_options.tick_interval = address
				.config
				.tick_interval
				.unwrap_or(crate::api::engine::local::DEFAULT_TICK_INTERVAL);
			engine_options
		};
		let (tasks, task_chans) = start_tasks(&opt, kvs.clone());

		let mut notifications = kvs.notifications();
		let notification_stream = poll_fn(move |cx| match &mut notifications {
			Some(rx) => rx.poll_next_unpin(cx),
			None => Poll::Ready(None),
		});

		let streams = (route_rx.stream().map(Either::Left), notification_stream.map(Either::Right));
		let mut merged = streams.merge();

		while let Some(either) = merged.next().await {
			match either {
				Either::Left(None) => break, // Received a shutdown signal
				Either::Left(Some(route)) => {
					match super::router(
						route.request,
						&kvs,
						&mut session,
						&mut vars,
						&mut live_queries,
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
				Either::Right(notification) => {
					let id = notification.id;
					if let Some(sender) = live_queries.get(&id) {
						if sender.send(notification).await.is_err() {
							live_queries.remove(&id);
							if let Err(error) =
								super::kill_live_query(&kvs, id, &session, vars.clone()).await
							{
								warn!("Failed to kill live query '{id}'; {error}");
							}
						}
					}
				}
			}
		}

		// Stop maintenance tasks
		for chan in task_chans {
			if let Err(e) = chan.send(()) {
				error!("Error sending shutdown signal to task: {}", e);
			}
		}
		tasks.resolve().await.unwrap();
	});
}
