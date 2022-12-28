use crate::api::engines;
use crate::api::engines::any::Any;
use crate::api::err::Error;
use crate::api::opt::from_value;
use crate::api::opt::ServerAddrs;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
#[cfg(feature = "protocol-http")]
use crate::api::opt::Tls;
use crate::api::Connection;
use crate::api::DbResponse;
use crate::api::ExtraFeatures;
use crate::api::Method;
use crate::api::Param;
use crate::api::QueryResponse;
use crate::api::Result;
use crate::api::Route;
use crate::api::Router;
use crate::api::Surreal;
use flume::Receiver;
use once_cell::sync::OnceCell;
#[cfg(feature = "protocol-http")]
use reqwest::header::HeaderMap;
#[cfg(feature = "protocol-http")]
use reqwest::header::HeaderValue;
#[cfg(feature = "protocol-http")]
use reqwest::header::ACCEPT;
#[cfg(feature = "protocol-http")]
use reqwest::ClientBuilder;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
#[cfg(feature = "protocol-ws")]
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
#[cfg(feature = "protocol-ws")]
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use tokio_tungstenite::Connector;

impl Connection for Any {
	fn new(method: Method) -> Self {
		Self {
			method,
			id: 0,
		}
	}

	fn connect(
		address: ServerAddrs,
		capacity: usize,
	) -> Pin<Box<dyn Future<Output = Result<Surreal<Self>>> + Send + Sync + 'static>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => flume::unbounded(),
				capacity => flume::bounded(capacity),
			};

			#[allow(unused_variables)] // used by storage engines
			let (conn_tx, conn_rx) = flume::bounded::<Result<()>>(1);
			let mut features = HashSet::new();

			match address.endpoint.scheme() {
				#[cfg(feature = "kv-fdb")]
				"fdb" => {
					features.insert(ExtraFeatures::Backup);
					engines::local::native::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-mem")]
				"mem" => {
					features.insert(ExtraFeatures::Backup);
					engines::local::native::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-rocksdb")]
				"rocksdb" => {
					features.insert(ExtraFeatures::Backup);
					engines::local::native::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-rocksdb")]
				"file" => {
					features.insert(ExtraFeatures::Backup);
					engines::local::native::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "kv-tikv")]
				"tikv" => {
					features.insert(ExtraFeatures::Backup);
					engines::local::native::router(address, conn_tx, route_rx);
					if let Err(error) = conn_rx.into_recv_async().await? {
						return Err(error);
					}
				}

				#[cfg(feature = "protocol-http")]
				"http" | "https" => {
					features.insert(ExtraFeatures::Auth);
					features.insert(ExtraFeatures::Backup);
					let mut headers = HeaderMap::new();
					headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
					#[allow(unused_mut)]
					let mut builder = ClientBuilder::new().default_headers(headers);
					#[cfg(any(feature = "native-tls", feature = "rustls"))]
					if let Some(tls) = address.tls_config {
						builder = match tls {
							#[cfg(feature = "native-tls")]
							Tls::Native(config) => builder.use_preconfigured_tls(config),
							#[cfg(feature = "rustls")]
							Tls::Rust(config) => builder.use_preconfigured_tls(config),
						};
					}
					let client = builder.build()?;
					let base_url = address.endpoint;
					engines::remote::http::health(
						client.get(base_url.join(Method::Health.as_str())?),
					)
					.await?;
					engines::remote::http::native::router(base_url, client, route_rx);
				}

				#[cfg(feature = "protocol-ws")]
				"ws" | "wss" => {
					features.insert(ExtraFeatures::Auth);
					let url = address.endpoint.join(engines::remote::ws::PATH)?;
					#[cfg(any(feature = "native-tls", feature = "rustls"))]
					let maybe_connector = address.tls_config.map(Connector::from);
					#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
					let maybe_connector = None;
					let config = WebSocketConfig {
						max_send_queue: match capacity {
							0 => None,
							capacity => Some(capacity),
						},
						max_message_size: Some(engines::remote::ws::native::MAX_MESSAGE_SIZE),
						max_frame_size: Some(engines::remote::ws::native::MAX_FRAME_SIZE),
						accept_unmasked_frames: false,
					};
					let socket = engines::remote::ws::native::connect(
						&url,
						Some(config),
						maybe_connector.clone(),
					)
					.await?;
					engines::remote::ws::native::router(
						url,
						maybe_connector,
						capacity,
						config,
						socket,
						route_rx,
					);
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
	) -> Pin<Box<dyn Future<Output = Result<QueryResponse>> + Send + Sync + '_>> {
		Box::pin(async move {
			let response = receiver.into_recv_async().await?;
			match response? {
				DbResponse::Query(results) => Ok(results),
				DbResponse::Other(..) => unreachable!(),
			}
		})
	}
}
