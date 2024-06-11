use crate::api::conn::Connection;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
#[allow(unused_imports)] // used by the DB engines
use crate::api::engine;
use crate::api::engine::any::Any;
use crate::api::err::Error;
use crate::api::opt::{Endpoint, EndpointKind};
use crate::api::DbResponse;
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::error::Db as DbError;
use crate::opt::WaitFor;
use flume::Receiver;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;

impl crate::api::Connection for Any {}

impl Connection for Any {
	fn new(method: Method) -> Self {
		Self {
			method,
			id: 0,
		}
	}

	#[allow(unused_variables, unreachable_code, unused_mut)] // these are all used depending on feature
	fn connect(
		address: Endpoint,
		capacity: usize,
	) -> Pin<Box<dyn Future<Output = Result<Surreal<Self>>> + Send + Sync + 'static>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => flume::unbounded(),
				capacity => flume::bounded(capacity),
			};

			let (conn_tx, conn_rx) = flume::bounded::<Result<()>>(1);
			let mut features = HashSet::new();

			match EndpointKind::from(address.url.scheme()) {
				EndpointKind::IndxDb => {
					#[cfg(feature = "kv-indxdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						engine::local::wasm::router(address, conn_tx, route_rx);
						conn_rx.into_recv_async().await??;
					}

					#[cfg(not(feature = "kv-indxdb"))]
					return Err(
						DbError::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()).into()
					);
				}

				EndpointKind::Memory => {
					#[cfg(feature = "kv-mem")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						engine::local::wasm::router(address, conn_tx, route_rx);
						conn_rx.into_recv_async().await??;
					}

					#[cfg(not(feature = "kv-mem"))]
					return Err(
						DbError::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()).into()
					);
				}

				EndpointKind::File | EndpointKind::RocksDb => {
					#[cfg(feature = "kv-rocksdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						engine::local::wasm::router(address, conn_tx, route_rx);
						conn_rx.into_recv_async().await??;
					}

					#[cfg(not(feature = "kv-rocksdb"))]
					return Err(DbError::Ds(
						"Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					.into());
				}

				EndpointKind::SurrealKV => {
					#[cfg(feature = "kv-surrealkv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						engine::local::wasm::router(address, conn_tx, route_rx);
						conn_rx.into_recv_async().await??;
					}

					#[cfg(not(feature = "kv-surrealkv"))]
					return Err(DbError::Ds(
						"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					.into());
				}

				EndpointKind::TiKv => {
					#[cfg(feature = "kv-tikv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						engine::local::wasm::router(address, conn_tx, route_rx);
						conn_rx.into_recv_async().await??;
					}

					#[cfg(not(feature = "kv-tikv"))]
					return Err(
						DbError::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()).into()
					);
				}

				EndpointKind::Http | EndpointKind::Https => {
					#[cfg(feature = "protocol-http")]
					{
						engine::remote::http::wasm::router(address, conn_tx, route_rx);
					}

					#[cfg(not(feature = "protocol-http"))]
					return Err(DbError::Ds(
						"Cannot connect to the `HTTP` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					.into());
				}

				EndpointKind::Ws | EndpointKind::Wss => {
					#[cfg(feature = "protocol-ws")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						let mut endpoint = address;
						endpoint.url = endpoint.url.join(engine::remote::ws::PATH)?;
						engine::remote::ws::wasm::router(endpoint, capacity, conn_tx, route_rx);
						conn_rx.into_recv_async().await??;
					}

					#[cfg(not(feature = "protocol-ws"))]
					return Err(DbError::Ds(
						"Cannot connect to the `WebSocket` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					.into());
				}

				EndpointKind::Unsupported(v) => return Err(Error::Scheme(v).into()),
			}

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
			self.id = router.next_id();
			let route = Route {
				request: (self.id, self.method, param),
				response: sender,
			};
			router.sender.send_async(Some(route)).await?;
			Ok(receiver)
		})
	}
}
