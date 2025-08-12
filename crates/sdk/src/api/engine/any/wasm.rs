use std::collections::HashSet;
use std::sync::atomic::AtomicI64;

use anyhow::{Result, bail};
use tokio::sync::watch;
use wasm_bindgen_futures::spawn_local;

use crate::api::conn::Router;
#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::api::engine;
use crate::api::engine::any::Any;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::{Endpoint, EndpointKind};
use crate::api::{ExtraFeatures, Surreal, conn};
use crate::error::Db as DbError;
use crate::opt::WaitFor;

impl crate::api::Connection for Any {}
impl conn::Sealed for Any {
	#[allow(
		unused_variables,
		unreachable_code,
		unused_mut,
		reason = "Thse are all used depending on the enabled features."
	)]
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded::<Result<()>>(1);
			let config = address.config.clone();
			let mut features = HashSet::new();

			match EndpointKind::from(address.url.scheme()) {
				EndpointKind::FoundationDb => {
					#[cfg(kv_fdb)]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??;
					}

					#[cfg(not(kv_fdb))]
					bail!(
						DbError::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())
					);
				}

				EndpointKind::IndxDb => {
					#[cfg(feature = "kv-indxdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-indxdb"))]
					bail!(
						DbError::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())
					);
				}

				EndpointKind::Memory => {
					#[cfg(feature = "kv-mem")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-mem"))]
					bail!(
						DbError::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned())
					);
				}

				EndpointKind::File | EndpointKind::RocksDb => {
					#[cfg(feature = "kv-rocksdb")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-rocksdb"))]
					bail!(DbError::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()))
				}

				EndpointKind::SurrealKv | EndpointKind::SurrealKvVersioned => {
					#[cfg(feature = "kv-surrealkv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-surrealkv"))]
					bail!(DbError::Ds(
						"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
					));
				}

				EndpointKind::TiKv => {
					#[cfg(feature = "kv-tikv")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						spawn_local(engine::local::wasm::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "kv-tikv"))]
					bail!(
						DbError::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned())
					);
				}

				EndpointKind::Http | EndpointKind::Https => {
					#[cfg(feature = "protocol-http")]
					{
						spawn_local(engine::remote::http::wasm::run_router(
							address, conn_tx, route_rx,
						));
					}

					#[cfg(not(feature = "protocol-http"))]
					bail!(DbError::Ds(
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
							endpoint, capacity, conn_tx, route_rx,
						));
						conn_rx.recv().await??;
					}

					#[cfg(not(feature = "protocol-ws"))]
					bail!(DbError::Ds(
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
				last_id: AtomicI64::new(0),
			};

			Ok((router, waiter).into())
		})
	}
}
