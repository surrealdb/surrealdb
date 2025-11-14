use std::collections::HashSet;
use std::sync::atomic::AtomicI64;

// Removed anyhow::bail - using return Err() instead
#[cfg(feature = "protocol-http")]
use reqwest::ClientBuilder;
use tokio::sync::watch;
#[cfg(feature = "protocol-ws")]
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use tokio_tungstenite::Connector;
#[cfg(feature = "protocol-ws")]
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::ExtraFeatures;
use crate::conn::Router;
#[allow(unused_imports, reason = "Used by the DB engines.")]
use crate::engine;
use crate::engine::any::Any;
#[cfg(feature = "protocol-http")]
use crate::engine::remote::http;
use crate::err::Error;
use crate::method::BoxFuture;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
#[cfg(feature = "protocol-http")]
use crate::opt::Tls;
use crate::opt::{Endpoint, EndpointKind, WaitFor};
use crate::{Result, Surreal, conn};
impl crate::Connection for Any {}
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
				EndpointKind::Memory => {
					#[cfg(feature = "kv-mem")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-mem"))]
					return Err(Error::Scheme("memory".to_owned()));
				}

				EndpointKind::RocksDb => {
					#[cfg(feature = "kv-rocksdb")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(address, conn_tx, route_rx));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-rocksdb"))]
				return Err(Error::Scheme(
					"Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
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
				return Err(
					Error::Scheme("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned())
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
				return Err(Error::Scheme(
					"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
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
							&*surrealdb_core::cnf::SURREALDB_USER_AGENT,
						);
						http::health(req).await?;
						tokio::spawn(http::native::run_router(base_url, client, route_rx));
					}

					#[cfg(not(feature = "protocol-http"))]
				return Err(Error::Scheme(
					"Cannot connect to the `HTTP` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
				}

				EndpointKind::Ws | EndpointKind::Wss => {
					#[cfg(feature = "protocol-ws")]
					{
						let crate::opt::WebsocketConfig {
							read_buffer_size,
							max_message_size,
							max_write_buffer_size,
							write_buffer_size,
						} = address.config.websocket;

						features.insert(ExtraFeatures::LiveQueries);
						let mut endpoint = address;
						endpoint.url = endpoint.url.join(engine::remote::ws::PATH)?;
						#[cfg(any(feature = "native-tls", feature = "rustls"))]
						let maybe_connector = endpoint.config.tls_config.clone().map(Connector::from);
						#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
						let maybe_connector = None;

						let config = WebSocketConfig::default()
							.max_message_size(max_message_size)
							.max_frame_size(max_message_size)
							.max_write_buffer_size(max_write_buffer_size)
							.write_buffer_size(write_buffer_size)
							.read_buffer_size(read_buffer_size);
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
				return Err(Error::Scheme(
					"Cannot connect to the `WebSocket` remote engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
				}
				EndpointKind::Unsupported(v) => return Err(Error::Scheme(v)),
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
