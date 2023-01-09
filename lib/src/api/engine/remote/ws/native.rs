use super::LOG;
use super::PATH;
use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engine::remote::ws::Client;
use crate::api::engine::remote::ws::Response;
use crate::api::engine::remote::ws::PING_INTERVAL;
use crate::api::engine::remote::ws::PING_METHOD;
use crate::api::err::Error;
use crate::api::opt::from_value;
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::ExtraFeatures;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use crate::sql::Strand;
use crate::sql::Value;
use flume::Receiver;
use futures::stream::SplitSink;
use futures::SinkExt;
use futures::StreamExt;
use futures_concurrency::stream::Merge as _;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use serde::de::DeserializeOwned;
use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::IntervalStream;
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::Connector;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use trice::Instant;
use url::Url;

type WsResult<T> = std::result::Result<T, WsError>;

pub(crate) const MAX_MESSAGE_SIZE: usize = 64 << 20; // 64 MiB
pub(crate) const MAX_FRAME_SIZE: usize = 16 << 20; // 16 MiB

pub(crate) enum Either {
	Request(Option<Route>),
	Response(WsResult<Message>),
	Ping,
}

#[cfg(any(feature = "native-tls", feature = "rustls"))]
impl From<Tls> for Connector {
	fn from(tls: Tls) -> Self {
		match tls {
			#[cfg(feature = "native-tls")]
			Tls::Native(config) => Self::NativeTls(config),
			#[cfg(feature = "rustls")]
			Tls::Rust(config) => Self::Rustls(Arc::new(config)),
		}
	}
}

pub(crate) async fn connect(
	url: &Url,
	config: Option<WebSocketConfig>,
	#[allow(unused_variables)] maybe_connector: Option<Connector>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	let (socket, _) =
		tokio_tungstenite::connect_async_tls_with_config(url, config, maybe_connector).await?;

	#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
	let (socket, _) = tokio_tungstenite::connect_async_with_config(url, config).await?;

	Ok(socket)
}

impl crate::api::Connection for Client {}

impl Connection for Client {
	fn new(method: Method) -> Self {
		Self {
			id: 0,
			method,
		}
	}

	fn connect(
		address: Endpoint,
		capacity: usize,
	) -> Pin<Box<dyn Future<Output = Result<Surreal<Self>>> + Send + Sync + 'static>> {
		Box::pin(async move {
			let url = address.endpoint.join(PATH)?;
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			let maybe_connector = address.tls_config.map(Connector::from);
			#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
			let maybe_connector = None;

			let config = WebSocketConfig {
				max_send_queue: match capacity {
					0 => None,
					capacity => Some(capacity),
				},
				max_message_size: Some(MAX_MESSAGE_SIZE),
				max_frame_size: Some(MAX_FRAME_SIZE),
				accept_unmasked_frames: false,
			};

			let socket = connect(&url, Some(config), maybe_connector.clone()).await?;

			let (route_tx, route_rx) = match capacity {
				0 => flume::unbounded(),
				capacity => flume::bounded(capacity),
			};

			router(url, maybe_connector, capacity, config, socket, route_rx);

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Auth);

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
			self.id = router.next_id();
			let (sender, receiver) = flume::bounded(1);
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
		rx: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<R>> + Send + Sync + '_>>
	where
		R: DeserializeOwned,
	{
		Box::pin(async move {
			let response = rx.into_recv_async().await?;
			match response? {
				DbResponse::Other(value) => from_value(value),
				DbResponse::Query(..) => unreachable!(),
			}
		})
	}

	fn recv_query(
		&mut self,
		rx: Receiver<Result<DbResponse>>,
	) -> Pin<Box<dyn Future<Output = Result<QueryResponse>> + Send + Sync + '_>> {
		Box::pin(async move {
			let response = rx.into_recv_async().await?;
			match response? {
				DbResponse::Query(results) => Ok(results),
				DbResponse::Other(..) => unreachable!(),
			}
		})
	}
}

#[allow(clippy::too_many_lines)]
pub(crate) fn router(
	url: Url,
	maybe_connector: Option<Connector>,
	capacity: usize,
	config: WebSocketConfig,
	mut socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
	route_rx: Receiver<Option<Route>>,
) {
	tokio::spawn(async move {
		let ping = {
			let mut request = BTreeMap::new();
			request.insert("method".to_owned(), PING_METHOD.into());
			let value = Value::from(request);
			Message::Binary(value.into())
		};

		let mut vars = IndexMap::new();
		let mut replay = IndexMap::new();

		'router: loop {
			let (socket_sink, socket_stream) = socket.split();
			let mut socket_sink = Socket(Some(socket_sink));

			if let Socket(Some(socket_sink)) = &mut socket_sink {
				let mut routes = match capacity {
					0 => HashMap::new(),
					capacity => HashMap::with_capacity(capacity),
				};

				let mut interval = time::interval(PING_INTERVAL);
				// don't bombard the server with pings if we miss some ticks
				interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
				// Delay sending the first ping
				interval.tick().await;

				let pinger = IntervalStream::new(interval);

				let streams = (
					socket_stream.map(Either::Response),
					route_rx.stream().map(Either::Request),
					pinger.map(|_| Either::Ping),
				);

				let mut merged = streams.merge();
				let mut last_activity = Instant::now();

				while let Some(either) = merged.next().await {
					match either {
						Either::Request(Some(Route {
							request,
							response,
						})) => {
							let (id, method, param) = request;
							let params = match param.query {
								Some((query, bindings)) => {
									vec![query.to_string().into(), bindings.into()]
								}
								None => param.other,
							};
							match method {
								Method::Set => {
									if let [Value::Strand(Strand(key)), value] = &params[..2] {
										vars.insert(key.clone(), value.clone());
									}
								}
								Method::Unset => {
									if let [Value::Strand(Strand(key))] = &params[..1] {
										vars.remove(key);
									}
								}
								_ => {}
							}
							let method_str = match method {
								Method::Health => PING_METHOD,
								_ => method.as_str(),
							};
							let message = {
								let mut request = BTreeMap::new();
								request.insert("id".to_owned(), Value::from(id));
								request.insert("method".to_owned(), method_str.into());
								if !params.is_empty() {
									request.insert("params".to_owned(), params.into());
								}
								let payload = Value::from(request);
								trace!(target: LOG, "Request {payload}");
								Message::Binary(payload.into())
							};
							if let Method::Authenticate
							| Method::Invalidate
							| Method::Signin
							| Method::Signup
							| Method::Use = method
							{
								replay.insert(method, message.clone());
							}
							match socket_sink.send(message).await {
								Ok(..) => {
									last_activity = Instant::now();
									match routes.entry(id) {
										Entry::Vacant(entry) => {
											entry.insert((method, response));
										}
										Entry::Occupied(..) => {
											let error = Error::DuplicateRequestId(id);
											if response
												.into_send_async(Err(error.into()))
												.await
												.is_err()
											{
												trace!(target: LOG, "Receiver dropped");
											}
										}
									}
								}
								Err(error) => {
									let error = Error::Ws(error.to_string());
									if response.into_send_async(Err(error.into())).await.is_err() {
										trace!(target: LOG, "Receiver dropped");
									}
									break;
								}
							}
						}
						Either::Response(result) => {
							last_activity = Instant::now();
							match result {
								Ok(message) => match Response::try_from(message) {
									Ok(option) => {
										if let Some(response) = option {
											trace!(target: LOG, "{response:?}");
											if let Some(id) = response.id {
												if let Some((method, sender)) =
													routes.remove(&id.as_int())
												{
													let _res = sender
														.into_send_async(DbResponse::from((
															method,
															response.content,
														)))
														.await;
												}
											}
										}
									}
									Err(_error) => {
										trace!(target: LOG, "Failed to deserialise message");
									}
								},
								Err(error) => {
									match error {
										WsError::ConnectionClosed => {
											trace!(
												target: LOG,
												"Connection successfully closed on the server"
											);
										}
										error => {
											trace!(target: LOG, "{error}");
										}
									}
									break;
								}
							}
						}
						Either::Ping => {
							// only ping if we haven't talked to the server recently
							if last_activity.elapsed() >= PING_INTERVAL {
								trace!(target: LOG, "Pinging the server");
								if let Err(error) = socket_sink.send(ping.clone()).await {
									trace!(target: LOG, "failed to ping the server; {error:?}");
									break;
								}
							}
						}
						Either::Request(None) => {
							break 'router;
						}
					}
				}
			}

			'reconnect: loop {
				trace!(target: LOG, "Reconnecting...");
				match connect(&url, Some(config), maybe_connector.clone()).await {
					Ok(s) => {
						socket = s;
						for (_, message) in &replay {
							if let Err(error) = socket.send(message.clone()).await {
								trace!(target: LOG, "{error}");
								time::sleep(time::Duration::from_secs(1)).await;
								continue 'reconnect;
							}
						}
						#[cfg(feature = "protocol-ws")]
						for (key, value) in &vars {
							let mut request = BTreeMap::new();
							request.insert("method".to_owned(), Method::Set.as_str().into());
							request.insert(
								"params".to_owned(),
								vec![key.as_str().into(), value.clone()].into(),
							);
							let payload = Value::from(request);
							trace!(target: LOG, "Request {payload}");
							if let Err(error) = socket.send(Message::Binary(payload.into())).await {
								trace!(target: LOG, "{error}");
								time::sleep(time::Duration::from_secs(1)).await;
								continue 'reconnect;
							}
						}
						trace!(target: LOG, "Reconnected successfully");
						break;
					}
					Err(error) => {
						trace!(target: LOG, "Failed to reconnect; {error}");
						time::sleep(time::Duration::from_secs(1)).await;
					}
				}
			}
		}
	});
}

impl Response {
	fn try_from(message: Message) -> Result<Option<Self>> {
		match message {
			Message::Text(text) => {
				trace!(target: LOG, "Received an unexpected text message; {text}");
				Ok(None)
			}
			Message::Binary(binary) => msgpack::from_slice(&binary).map(Some).map_err(|error| {
				Error::ResponseFromBinary {
					binary,
					error,
				}
				.into()
			}),
			Message::Ping(..) => {
				trace!(target: LOG, "Received a ping from the server");
				Ok(None)
			}
			Message::Pong(..) => {
				trace!(target: LOG, "Received a pong from the server");
				Ok(None)
			}
			Message::Frame(..) => {
				trace!(target: LOG, "Received an unexpected raw frame");
				Ok(None)
			}
			Message::Close(..) => {
				trace!(target: LOG, "Received an unexpected close message");
				Ok(None)
			}
		}
	}
}

pub struct Socket(Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>);

impl Drop for Socket {
	fn drop(&mut self) {
		if let Some(mut conn) = mem::take(&mut self.0) {
			futures::executor::block_on(async move {
				match conn.borrow_mut().close().await {
					Ok(..) => trace!(target: LOG, "Connection closed successfully"),
					Err(error) => {
						trace!(target: LOG, "Failed to close database connection; {error}")
					}
				}
			});
		}
	}
}
