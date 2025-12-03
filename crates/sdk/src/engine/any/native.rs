use std::collections::HashSet;

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
					return Err(Error::Scheme("memory".to_owned()));
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
				return Err(Error::Scheme(
					"Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
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
					Error::Scheme("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned())
				);
				}

				EndpointKind::SurrealKv | EndpointKind::SurrealKvVersioned => {
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
				return Err(Error::Scheme(
					"Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned(),
				));
				}

				EndpointKind::Http | EndpointKind::Https => {
					#[cfg(feature = "protocol-http")]
					{
						use std::net::SocketAddr;

						features.insert(ExtraFeatures::Backup);
						let headers = http::default_headers();
						let base_url = address.url;

						// Extract hostname and port for DNS resolution
						let hostname = base_url.host_str().unwrap_or("localhost").to_string();
						let port = base_url.port_or_known_default().unwrap_or(8000);

						// Resolve hostname to get list of addresses
						let host_port = format!("{}:{}", hostname, port);
						let addrs: Vec<SocketAddr> = tokio::net::lookup_host(&host_port)
							.await
							.map_err(|e| {
								Error::InternalError(format!(
									"DNS resolution failed for {}: {}",
									host_port, e
								))
							})?
							.collect();

						if addrs.is_empty() {
							return Err(Error::InternalError(format!(
								"DNS resolution returned no addresses for {}",
								host_port
							)));
						}

						// Try each address until one works
						let mut last_error = None;
						let mut successful_client = None;

						for addr in addrs {
							#[cfg_attr(
								not(any(feature = "native-tls", feature = "rustls")),
								expect(unused_mut)
							)]
							let mut builder = ClientBuilder::new()
								.default_headers(headers.clone())
								.resolve(&hostname, addr);

							#[cfg(any(feature = "native-tls", feature = "rustls"))]
							if let Some(ref tls) = address.config.tls_config {
								builder = match tls {
									#[cfg(feature = "native-tls")]
									Tls::Native(config) => builder.use_preconfigured_tls(config.clone()),
									#[cfg(feature = "rustls")]
									Tls::Rust(config) => builder.use_preconfigured_tls(config.clone()),
								};
							}

							let client = match builder.build() {
								Ok(c) => c,
								Err(e) => {
									last_error = Some(Error::from(e));
									continue;
								}
							};

							// Try health check with this address
							let req = client.get(base_url.join("health")?).header(
								reqwest::header::USER_AGENT,
								&*surrealdb_core::cnf::SURREALDB_USER_AGENT,
							);

							match http::health(req).await {
								Ok(()) => {
									successful_client = Some(client);
									break;
								}
								Err(e) => {
									last_error = Some(e);
									continue;
								}
							}
						}

						let client = successful_client.ok_or_else(|| {
							last_error.unwrap_or_else(|| {
								Error::InternalError("No addresses available".to_string())
							})
						})?;

						tokio::spawn(http::native::run_router(
							client,
							base_url,
							route_rx,
							session_clone.receiver.clone(),
						));
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
							config,
							socket,
							route_rx,
							session_clone.receiver.clone(),
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
			};

			Ok((router, waiter, session_clone).into())
		})
	}
}
