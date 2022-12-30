use crate::api::conn::Connection;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
#[allow(unused_imports)] // used by the DB engines
use crate::api::engines;
use crate::api::engines::any::Any;
use crate::api::err::Error;
use crate::api::opt::from_value;
use crate::api::opt::Endpoint;
use crate::api::DbResponse;
#[allow(unused_imports)] // used by the `ws` and `http` protocols
use crate::api::ExtraFeatures;
use crate::api::Response;
use crate::api::Result;
use crate::api::Surreal;
use flume::Receiver;
use once_cell::sync::OnceCell;
use serde::de::DeserializeOwned;
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
				#[cfg(feature = "kv-fdb")]
				"fdb" => {
					engines::local::wasm::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-indxdb")]
				"indxdb" => {
					engines::local::wasm::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-mem")]
				"mem" => {
					engines::local::wasm::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-rocksdb")]
				"rocksdb" => {
					engines::local::wasm::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-rocksdb")]
				"file" => {
					engines::local::wasm::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-tikv")]
				"tikv" => {
					engines::local::wasm::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "protocol-http")]
				"http" | "https" => {
					features.insert(ExtraFeatures::Auth);
					engines::remote::http::wasm::router(address, conn_tx, route_rx);
				}

				#[cfg(feature = "protocol-ws")]
				"ws" | "wss" => {
					features.insert(ExtraFeatures::Auth);
					let mut address = address;
					address.endpoint = address.endpoint.join(engines::remote::ws::PATH)?;
					engines::remote::ws::wasm::router(address, capacity, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
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

	fn recv<R>(
		&mut self,
		receiver: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<R>> + Send + Sync + '_>>
	where
		R: DeserializeOwned,
	{
		Box::pin(async move {
			let response = receiver.into_recv_async().await?;
			match response? {
				DbResponse::Other(value) => from_value(value),
				DbResponse::Query(..) => unreachable!(),
			}
		})
	}

	fn recv_query(
		&mut self,
		receiver: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<Response>> + Send + Sync + '_>> {
		Box::pin(async move {
			let response = receiver.into_recv_async().await?;
			match response? {
				DbResponse::Query(results) => Ok(results),
				DbResponse::Other(..) => unreachable!(),
			}
		})
	}
}
