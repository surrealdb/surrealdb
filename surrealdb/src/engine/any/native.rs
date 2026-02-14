use std::collections::HashSet;

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
use crate::Error;
use crate::method::BoxFuture;
use crate::opt::{Endpoint, EndpointKind, WaitFor};
use crate::{Result, SessionClone, Surreal, conn};
impl crate::Connection for Any {}
impl conn::Sealed for Any {
	#[allow(
		unused_variables,
		private_interfaces,
		unreachable_code,
		unused_mut,
		reason = "These are all used depending on the enabled features."
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
				EndpointKind::Memory => {
					#[cfg(feature = "kv-mem")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-mem"))]
					return Err(Error::configuration("Unsupported scheme: memory".to_string(), None));
				}

				EndpointKind::RocksDb => {
					#[cfg(feature = "kv-rocksdb")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-rocksdb"))]
				return Err(Error::configuration(
					"Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_string(),
					None,
				));
				}

				EndpointKind::TiKv => {
					#[cfg(feature = "kv-tikv")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-tikv"))]
				return Err(
					Error::configuration("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_string(), None)
				);
				}

				EndpointKind::SurrealKv => {
					#[cfg(feature = "kv-surrealkv")]
					{
						features.insert(ExtraFeatures::Backup);
						features.insert(ExtraFeatures::LiveQueries);
						tokio::spawn(engine::local::native::run_router(
							address,
							conn_tx,
							route_rx,
							session_clone.receiver.clone(),
						));
						conn_rx.recv().await??
					}

					#[cfg(not(feature = "kv-surrealkv"))]
				return Err(Error::configuration(
					"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_string(),
					None,
				));
				}

				EndpointKind::Http | EndpointKind::Https => {
					#[cfg(feature = "protocol-http")]
					{
						features.insert(ExtraFeatures::Backup);
						let base_url = address.url;

						#[cfg(any(feature = "native-tls", feature = "rustls"))]
						let client = http::native::create_client(&base_url, address.config.tls_config.as_ref())
							.await?;
						#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
						let client = http::native::create_client(&base_url).await?;

						tokio::spawn(http::native::run_router(
							client,
							base_url,
							route_rx,
							session_clone.receiver.clone(),
						));
					}

					#[cfg(not(feature = "protocol-http"))]
				return Err(Error::configuration(
					"Cannot connect to the `HTTP` remote engine as it is not enabled in this build of SurrealDB".to_string(),
					None,
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
						endpoint.url = endpoint
							.url
							.join(engine::remote::ws::PATH)
							.map_err(|e| Error::internal(e.to_string()))?;
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
							config,
							socket,
							route_rx,
							session_clone.receiver.clone(),
						));
					}

					#[cfg(not(feature = "protocol-ws"))]
				return Err(Error::configuration(
					"Cannot connect to the `WebSocket` remote engine as it is not enabled in this build of SurrealDB".to_string(),
					None,
				));
				}
				EndpointKind::Unsupported(v) => return Err(Error::configuration(format!("Unsupported scheme: {v}"), None)),
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
