use std::collections::HashSet;

use tokio::sync::watch;
use wasm_bindgen_futures::spawn_local;

use crate::conn::Router;
#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::engine;
use crate::engine::any::Any;
use crate::err::Error;
use crate::method::BoxFuture;
use crate::opt::{Endpoint, EndpointKind, WaitFor};
use crate::{ExtraFeatures, Result, SessionClone, Surreal, conn};

impl crate::Connection for Any {}
impl conn::Sealed for Any {
	#[allow(
		unused_variables,
		unreachable_code,
		unused_mut,
		private_interfaces,
		reason = "Thse are all used depending on the enabled features."
	)]
	fn connect(
		address: Endpoint,
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded::<Result<()>>(1);
			let config = address.config.clone();
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);
			let mut features = HashSet::new();

			match EndpointKind::from(address.url.scheme()) {
				EndpointKind::IndxDb => {
					#[cfg(feature = "kv-indxdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-indxdb"))]
				return Err(
					Error::InternalError("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())
				);
				}

				EndpointKind::Memory => {
					#[cfg(feature = "kv-mem")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-mem"))]
				return Err(
					Error::InternalError("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned())
				);
				}

				EndpointKind::RocksDb => {
					#[cfg(feature = "kv-rocksdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-rocksdb"))]
			return Err(Error::Ws("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
				}

				EndpointKind::SurrealKv | EndpointKind::SurrealKvVersioned => {
					#[cfg(feature = "kv-surrealkv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-surrealkv"))]
				return Err(Error::Ws(
				"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
			));
				}

				EndpointKind::TiKv => {
					#[cfg(feature = "kv-tikv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-tikv"))]
				return Err(
					Error::Ws("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned())
				);
				}

				EndpointKind::Http | EndpointKind::Https => {
					#[cfg(feature = "protocol-http")]
					{
						spawn_local(engine::remote::http::wasm::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
					}

					#[cfg(not(feature = "protocol-http"))]
				return Err(Error::InternalError(
					"Cannot connect to the `HTTP` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
				}

				EndpointKind::Ws | EndpointKind::Wss => {
					#[cfg(feature = "protocol-ws")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						let mut endpoint = address;
						endpoint.url = endpoint.url.join(engine::remote::ws::PATH)?;
						spawn_local(engine::remote::ws::wasm::run_router(
							endpoint,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "protocol-ws"))]
				return Err(Error::InternalError(
					"Cannot connect to the `WebSocket` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
				}

				EndpointKind::Unsupported(v) => return Err(Error::Scheme(v).into()),
			}

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features,
				config,
				sender: route_tx,
			};

			Ok((router, waiter, session_clone).into())
		})
	}
}
