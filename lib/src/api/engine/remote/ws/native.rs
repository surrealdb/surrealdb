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
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Notification;
use crate::engine::remote::ws::Data;
use crate::engine::IntervalStream;
use crate::opt::from_value;
use crate::sql::serde::{deserialize, serialize};
use crate::sql::Object;
use crate::sql::Strand;
use crate::sql::Uuid;
use crate::sql::Value;
use flume::Receiver;
use futures::stream::SplitSink;
use futures::SinkExt;
use futures::StreamExt;
use futures_concurrency::stream::Merge as _;
use indexmap::IndexMap;
use serde::Deserialize;
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
use std::sync::OnceLock;
use tokio::net::TcpStream;
use tokio::time;
use tokio::time::MissedTickBehavior;
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
pub(crate) const WRITE_BUFFER_SIZE: usize = 128000; // tungstenite default
pub(crate) const MAX_WRITE_BUFFER_SIZE: usize = WRITE_BUFFER_SIZE + MAX_MESSAGE_SIZE; // Recommended max according to tungstenite docs
pub(crate) const NAGLE_ALG: bool = false;

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
		tokio_tungstenite::connect_async_tls_with_config(url, config, NAGLE_ALG, maybe_connector)
			.await?;

	#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
	let (socket, _) = tokio_tungstenite::connect_async_with_config(url, config, NAGLE_ALG).await?;

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
			let url = address.url.join(PATH)?;
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			let maybe_connector = address.config.tls_config.map(Connector::from);
			#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
			let maybe_connector = None;

			let config = WebSocketConfig {
				max_message_size: Some(MAX_MESSAGE_SIZE),
				max_frame_size: Some(MAX_FRAME_SIZE),
				max_write_buffer_size: MAX_WRITE_BUFFER_SIZE,
				..Default::default()
			};

			let socket = connect(&url, Some(config), maybe_connector.clone()).await?;

			let (route_tx, route_rx) = match capacity {
				0 => flume::unbounded(),
				capacity => flume::bounded(capacity),
			};

			router(url, maybe_connector, capacity, config, socket, route_rx);

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::LiveQueries);

			Ok(Surreal {
				router: Arc::new(OnceLock::with_value(Router {
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
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum LiveQueryKey {
	Int(i64),
	Uuid(Uuid),
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
			let value = serialize(&value).unwrap();
			Message::Binary(value)
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
				let mut live_queries = HashMap::new();

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
									vec![query.into(), bindings.into()]
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
								Method::Kill => {
									if let [Value::Uuid(uuid)] = &params[..1] {
										live_queries.remove(&LiveQueryKey::Uuid(*uuid));
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
								trace!("Request {payload}");
								let payload = serialize(&payload).unwrap();
								Message::Binary(payload)
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
											// Register query route
											entry.insert((method, response));
											// Register live query listener, if applicable
											if let Some(sender) = param.notification_sender {
												live_queries.insert(LiveQueryKey::Int(id), sender);
											}
										}
										Entry::Occupied(..) => {
											let error = Error::DuplicateRequestId(id);
											if response
												.into_send_async(Err(error.into()))
												.await
												.is_err()
											{
												trace!("Receiver dropped");
											}
										}
									}
								}
								Err(error) => {
									let error = Error::Ws(error.to_string());
									if response.into_send_async(Err(error.into())).await.is_err() {
										trace!("Receiver dropped");
									}
									break;
								}
							}
						}
						Either::Response(result) => {
							last_activity = Instant::now();
							match result {
								Ok(message) => {
									match Response::try_from(&message) {
										Ok(option) => {
											// We are only interested in responses that are not empty
											if let Some(response) = option {
												trace!("{response:?}");
												match response.id {
													// If `id` is set this is a normal response
													Some(id) => {
														if let Ok(id) = id.coerce_to_i64() {
															// We can only route responses with IDs
															if let Some((_method, sender)) =
																routes.remove(&id)
															{
																// If this is a live query, replace the client ID with the live query ID from the database
																if let Some(sender) = live_queries
																	.remove(&LiveQueryKey::Int(id))
																{
																	if let Ok(Data::Other(
																		Value::Uuid(uuid),
																	)) = &response.result
																	{
																		live_queries.insert(
																			LiveQueryKey::Uuid(
																				*uuid,
																			),
																			sender,
																		);
																	}
																}
																// Send the response back to the caller
																let _res = sender
																	.into_send_async(
																		DbResponse::from(
																			response.result,
																		),
																	)
																	.await;
															}
														}
													}
													// If `id` is not set, this may be a live query notification
													None => match response.result {
														Ok(Data::Live(notification)) => {
															// Check if this live query is registered
															if let Some(sender) = live_queries.get(
																&LiveQueryKey::Uuid(
																	notification.id,
																),
															) {
																// Send the notification back to the caller if it is
																let _res =
																	sender.send(notification).await;
															}
														}
														Ok(..) => { /* Ignored responses like pings */
														}
														Err(error) => error!("{error:?}"),
													},
												}
											}
										}
										Err(error) => {
											#[derive(Deserialize)]
											struct Response {
												id: Option<Value>,
											}

											// Let's try to find out the ID of the response that failed to deserialise
											if let Message::Binary(binary) = message {
												if let Ok(Response {
													id,
												}) = deserialize(&binary)
												{
													// Return an error if an ID was returned
													if let Some(Ok(id)) =
														id.map(Value::coerce_to_i64)
													{
														if let Some((_method, sender)) =
															routes.remove(&id)
														{
															let _res = sender
																.into_send_async(Err(error))
																.await;
														}
													}
												} else {
													// Unfortunately, we don't know which response failed to deserialize
													warn!(
														"Failed to deserialise message; {error:?}"
													);
												}
											}
										}
									}
								}
								Err(error) => {
									match error {
										WsError::ConnectionClosed => {
											trace!("Connection successfully closed on the server");
										}
										error => {
											trace!("{error}");
										}
									}
									break;
								}
							}
						}
						Either::Ping => {
							// only ping if we haven't talked to the server recently
							if last_activity.elapsed() >= PING_INTERVAL {
								trace!("Pinging the server");
								if let Err(error) = socket_sink.send(ping.clone()).await {
									trace!("failed to ping the server; {error:?}");
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
				trace!("Reconnecting...");
				match connect(&url, Some(config), maybe_connector.clone()).await {
					Ok(s) => {
						socket = s;
						for (_, message) in &replay {
							if let Err(error) = socket.send(message.clone()).await {
								trace!("{error}");
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
							trace!("Request {payload}");
							if let Err(error) = socket.send(Message::Binary(payload.into())).await {
								trace!("{error}");
								time::sleep(time::Duration::from_secs(1)).await;
								continue 'reconnect;
							}
						}
						trace!("Reconnected successfully");
						break;
					}
					Err(error) => {
						trace!("Failed to reconnect; {error}");
						time::sleep(time::Duration::from_secs(1)).await;
					}
				}
			}
		}
	});
}

impl Response {
	fn try_from(message: &Message) -> Result<Option<Self>> {
		match message {
			Message::Text(text) => {
				// Live queries currently don't support the binary protocol
				// This is a workaround until live queries add support to send messages over binary
				if let Ok(value) = crate::sql::json(text) {
					if let Value::Object(Object(mut response)) = value {
						if let Some(Value::Object(Object(mut map))) = response.remove("result") {
							if let Some(Value::Uuid(id)) = map.remove("id") {
								if let Some(value) = map.remove("action") {
									if let Ok(action) = from_value(value) {
										if let Some(result) = map.remove("result") {
											return Ok(Some(Self {
												id: None,
												result: Ok(Data::Live(Notification {
													id,
													action,
													result,
												})),
											}));
										}
									}
								}
							}
						}
					}
				}
				trace!("Received an unexpected text message; {text}");
				Ok(None)
			}
			Message::Binary(binary) => deserialize(binary).map(Some).map_err(|error| {
				Error::ResponseFromBinary {
					binary: binary.clone(),
					error,
				}
				.into()
			}),
			Message::Ping(..) => {
				trace!("Received a ping from the server");
				Ok(None)
			}
			Message::Pong(..) => {
				trace!("Received a pong from the server");
				Ok(None)
			}
			Message::Frame(..) => {
				trace!("Received an unexpected raw frame");
				Ok(None)
			}
			Message::Close(..) => {
				trace!("Received an unexpected close message");
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
					Ok(..) => trace!("Connection closed successfully"),
					Err(error) => {
						trace!("Failed to close database connection; {error}")
					}
				}
			});
		}
	}
}
