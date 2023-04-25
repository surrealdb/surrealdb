use crate::api::conn::Connection;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
#[allow(unused_imports)] // used by the DB engines
use crate::api::engine;
use crate::api::engine::any::Any;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use crate::api::DbResponse;
#[allow(unused_imports)] // used by the `ws` and `http` protocols
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use crate::error::Db as DbError;
use flume::Receiver;
use once_cell::sync::OnceCell;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

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

			match address.endpoint.scheme() {
				"fdb" => {
					#[cfg(feature = "kv-fdb")]
					{
						engine::local::wasm::router(address, conn_tx, route_rx);
						if let Err(error) = conn_rx.into_recv_async().await? {
							return Err(error);
						}
					}

					#[cfg(not(feature = "kv-fdb"))]
					return Err(
						DbError::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()).into()
					);
				}

				"indxdb" => {
					#[cfg(feature = "kv-indxdb")]
					{
						engine::local::wasm::router(address, conn_tx, route_rx);
						if let Err(error) = conn_rx.into_recv_async().await? {
							return Err(error);
						}
					}

					#[cfg(not(feature = "kv-indxdb"))]
					return Err(
						DbError::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()).into()
					);
				}

				"mem" => {
					#[cfg(feature = "kv-mem")]
					{
						engine::local::wasm::router(address, conn_tx, route_rx);
						if let Err(error) = conn_rx.into_recv_async().await? {
							return Err(error);
						}
					}

					#[cfg(not(feature = "kv-mem"))]
					return Err(
						DbError::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()).into()
					);
				}

				"file" | "rocksdb" => {
					#[cfg(feature = "kv-rocksdb")]
					{
						engine::local::wasm::router(address, conn_tx, route_rx);
						if let Err(error) = conn_rx.into_recv_async().await? {
							return Err(error);
						}
					}

					#[cfg(not(feature = "kv-rocksdb"))]
					return Err(DbError::Ds(
						"Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					.into());
				}

				"tikv" => {
					#[cfg(feature = "kv-tikv")]
					{
						engine::local::wasm::router(address, conn_tx, route_rx);
						if let Err(error) = conn_rx.into_recv_async().await? {
							return Err(error);
						}
					}

					#[cfg(not(feature = "kv-tikv"))]
					return Err(
						DbError::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()).into()
					);
				}

				"http" | "https" => {
					#[cfg(feature = "protocol-http")]
					{
						features.insert(ExtraFeatures::Auth);
						engine::remote::http::wasm::router(address, conn_tx, route_rx);
					}

					#[cfg(not(feature = "protocol-http"))]
					return Err(DbError::Ds(
						"Cannot connect to the `HTTP` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					.into());
				}

				"ws" | "wss" => {
					#[cfg(feature = "protocol-ws")]
					{
						features.insert(ExtraFeatures::Auth);
						let mut address = address;
						address.endpoint = address.endpoint.join(engine::remote::ws::PATH)?;
						engine::remote::ws::wasm::router(address, capacity, conn_tx, route_rx);
						if let Err(error) = conn_rx.into_recv_async().await? {
							return Err(error);
						}
					}

					#[cfg(not(feature = "protocol-ws"))]
					return Err(DbError::Ds(
						"Cannot connect to the `WebSocket` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					.into());
				}

				scheme => {
					return Err(Error::Scheme(scheme.to_owned()).into());
				}
			}

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
