use std::collections::HashSet;
use std::sync::atomic::AtomicI64;

#[allow(unused_imports, reason = "Used when a DB engine is disabled.")]
use anyhow::bail;
#[cfg(feature = "protocol-http")]
use reqwest::ClientBuilder;
use tokio::sync::watch;
#[cfg(feature = "protocol-ws")]
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use tokio_tungstenite::Connector;
#[cfg(feature = "protocol-ws")]
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::api::ExtraFeatures;
use crate::api::conn::Router;
#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::api::engine;
use crate::api::engine::any::Any;
#[cfg(feature = "protocol-http")]
use crate::api::engine::remote::http;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
#[cfg(feature = "protocol-http")]
use crate::api::opt::Tls;
use crate::api::opt::{Endpoint, EndpointKind};
use crate::api::{Result, Surreal, conn};
#[allow(unused_imports, reason = "Used when a DB engine is disabled.")]
use crate::core::err::Error as DbError;
use crate::opt::WaitFor;

impl crate::api::Connection for Any {}
impl conn::Sealed for Any {
	#[allow(
		unused_variables,
		unreachable_code,
		unused_mut,
		reason = "These are all used depending on the enabled features."
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
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??
					}

					#[cfg(not(kv_fdb))]
					bail!(
						DbError::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())
					);
				}

				EndpointKind::Memory => {
					#[cfg(feature = "kv-mem")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-mem"))]
					bail!(
						DbError::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned())
					);
				}

				EndpointKind::File | EndpointKind::RocksDb => {
					#[cfg(feature = "kv-rocksdb")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-rocksdb"))]
					bail!(DbError::Ds(
						"Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
					))
				}

				EndpointKind::TiKv => {
					#[cfg(feature = "kv-tikv")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-tikv"))]
					bail!(
						DbError::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned())
					);
				}

				EndpointKind::SurrealKv | EndpointKind::SurrealKvVersioned => {
					#[cfg(feature = "kv-surrealkv")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-surrealkv"))]
					bail!(DbError::Ds(
						"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					);
				}

				EndpointKind::Http | EndpointKind::Https => {
					#[cfg(feature = "protocol-http")]
					{
						features.insert(ExtraFeatures::Backup);
						let headers = http::default_headers();
						#[cfg_attr(
							not(any(feature = "native-tls", feature = "rustls")),
							expect(unused_mut)
						)]
						let mut builder = ClientBuilder::new().default_headers(headers);
						#[cfg(any(feature = "native-tls", feature = "rustls"))]
						if let Some(tls) = address.config.tls_config {
							builder = match tls {
								#[cfg(feature = "native-tls")]
								Tls::Native(config) => builder.use_preconfigured_tls(config),
								#[cfg(feature = "rustls")]
								Tls::Rust(config) => builder.use_preconfigured_tls(config),
							};
						}
						let client = builder.build()?;
						let base_url = address.url;
						let req = client.get(base_url.join("health")?).header(
							reqwest::header::USER_AGENT,
							&*crate::core::cnf::SURREALDB_USER_AGENT,
						);
						http::health(req).await?;
						tokio::spawn(http::native::run_router(base_url, client, route_rx));
					}

					#[cfg(not(feature = "protocol-http"))]
					bail!(DbError::Ds(
						"Cannot connect to the `HTTP` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
					)
					);
				}

				EndpointKind::Ws | EndpointKind::Wss => {
					#[cfg(feature = "protocol-ws")]
					{
						features.insert(ExtraFeatures::LiveQueries);
						let mut endpoint = address;
						endpoint.url = endpoint.url.join(engine::remote::ws::PATH)?;
						#[cfg(any(feature = "native-tls", feature = "rustls"))]
						let maybe_connector = endpoint.config.tls_config.clone().map(Connector::from);
						#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
						let maybe_connector = None;

						let config = WebSocketConfig {
							max_message_size: Some(engine::remote::ws::native::MAX_MESSAGE_SIZE),
							max_frame_size: Some(engine::remote::ws::native::MAX_FRAME_SIZE),
							max_write_buffer_size: engine::remote::ws::native::MAX_MESSAGE_SIZE,
							..Default::default()
						};
						let socket = engine::remote::ws::native::connect(
							&endpoint,
							Some(config),
							maybe_connector.clone(),
						)
						.await?;
						tokio::spawn(engine::remote::ws::native::run_router(
							endpoint,
							maybe_connector,
							capacity,
							config,
							socket,
							route_rx,
						));
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
