use std::collections::HashSet;

use tokio::sync::watch;
#[allow(unused_imports, reason = "Used by the DB engines.")]
use wasm_bindgen_futures::spawn_local;

#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::ExtraFeatures;
use crate::conn::Router;
#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::engine;
use crate::engine::any::Any;
use crate::method::BoxFuture;
use crate::opt::{Endpoint, EndpointKind, WaitFor};
use crate::{Error, Result, SessionClone, Surreal, conn};

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

			let endpoint_kind = EndpointKind::from(address.url.scheme());
			// An embedded engine owns its datastore and can hand results over as
			// it produces them; a remote one is bounded by what its transport
			// can carry, which for `ws` and `http` is one response per query.
			let streams = endpoint_kind.is_local();
			match endpoint_kind {
				EndpointKind::IndxDb => {
					#[cfg(feature = "kv-indxdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(surrealdb_engine_local::wasm::run_router(
							engine::local::local_config(address),
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;
					}

					#[cfg(not(feature = "kv-indxdb"))]
				return Err(
					Error::internal("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())
				);
				}

				EndpointKind::Memory => {
					#[cfg(feature = "kv-mem")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(surrealdb_engine_local::wasm::run_router(
							engine::local::local_config(address),
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;
					}

					#[cfg(not(feature = "kv-mem"))]
				return Err(
					Error::internal("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned())
				);
				}

				EndpointKind::RocksDb => {
					#[cfg(feature = "kv-rocksdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(surrealdb_engine_local::wasm::run_router(
							engine::local::local_config(address),
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;
					}

					#[cfg(not(feature = "kv-rocksdb"))]
			return Err(Error::internal("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
				}

				EndpointKind::SurrealKv => {
					#[cfg(feature = "kv-surrealkv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(surrealdb_engine_local::wasm::run_router(
							engine::local::local_config(address),
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;
					}

					#[cfg(not(feature = "kv-surrealkv"))]
				return Err(Error::internal(
				"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
			));
				}

				EndpointKind::TiKv => {
					#[cfg(feature = "kv-tikv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(surrealdb_engine_local::wasm::run_router(
							engine::local::local_config(address),
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;
					}

					#[cfg(not(feature = "kv-tikv"))]
				return Err(
					Error::internal("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned())
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
				return Err(Error::internal(
					"Cannot connect to the `HTTP` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
				}

				EndpointKind::Ws | EndpointKind::Wss => {
					#[cfg(feature = "protocol-ws")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						let mut endpoint = address;
						endpoint.url = endpoint
							.url
							.join(engine::remote::ws::PATH)
							.map_err(crate::std_error_to_types_error)?;
						spawn_local(engine::remote::ws::wasm::run_router(
							endpoint,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;
					}

					#[cfg(not(feature = "protocol-ws"))]
				return Err(Error::internal(
					"Cannot connect to the `WebSocket` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
				}

				EndpointKind::Unsupported(v) => {
					return Err(Error::configuration(format!("Unsupported scheme: {v}"), None));
				}
			}

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = if streams {
				Router::from_streaming_route_sender(route_tx, features, config)
			} else {
				Router::from_route_sender(route_tx, features, config)
			};

			Ok((router, waiter, session_clone).into())
		})
	}
}
