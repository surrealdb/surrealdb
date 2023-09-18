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
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::engine::IntervalStream;
use crate::sql::serde::{deserialize, serialize};
use crate::sql::Strand;
use crate::sql::Value;
use flume::Receiver;
use flume::Sender;
use futures::SinkExt;
use futures::StreamExt;
use futures_concurrency::stream::Merge as _;
use indexmap::IndexMap;
use pharos::Channel;
use pharos::Observable;
use pharos::ObserveConfig;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use trice::Instant;
use wasm_bindgen_futures::spawn_local;
use wasmtimer::tokio as time;
use wasmtimer::tokio::MissedTickBehavior;
use ws_stream_wasm::WsEvent;
use ws_stream_wasm::WsMessage as Message;
use ws_stream_wasm::WsMeta;

pub(crate) enum Either {
	Request(Option<Route>),
	Response(Message),
	Event(WsEvent),
	Ping,
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
		mut address: Endpoint,
		capacity: usize,
	) -> Pin<Box<dyn Future<Output = Result<Surreal<Self>>> + Send + Sync + 'static>> {
		Box::pin(async move {
			address.url = address.url.join(PATH)?;

			let (route_tx, route_rx) = match capacity {
				0 => flume::unbounded(),
				capacity => flume::bounded(capacity),
			};

			let (conn_tx, conn_rx) = flume::bounded(1);

			router(address, capacity, conn_tx, route_rx);

			conn_rx.into_recv_async().await??;

			Ok(Surreal {
				router: Arc::new(OnceLock::with_value(Router {
					features: HashSet::new(),
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

pub(crate) fn router(
	address: Endpoint,
	capacity: usize,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Option<Route>>,
) {
	spawn_local(async move {
		let (mut ws, mut socket) = match WsMeta::connect(&address.url, None).await {
			Ok(pair) => pair,
			Err(error) => {
				let _ = conn_tx.into_send_async(Err(error.into())).await;
				return;
			}
		};

		let mut events = {
			let result = match capacity {
				0 => ws.observe(ObserveConfig::default()).await,
				capacity => ws.observe(Channel::Bounded(capacity).into()).await,
			};
			match result {
				Ok(events) => events,
				Err(error) => {
					let _ = conn_tx.into_send_async(Err(error.into())).await;
					return;
				}
			}
		};

		let _ = conn_tx.into_send_async(Ok(())).await;

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
			let (mut socket_sink, socket_stream) = socket.split();

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
				events.map(Either::Event),
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
									vars.insert(key.to_owned(), value.clone());
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
										entry.insert((method, response));
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
					Either::Response(message) => {
						last_activity = Instant::now();
						match Response::try_from(&message) {
							Ok(option) => {
								if let Some(response) = option {
									trace!("{response:?}");
									if let Some(Ok(id)) = response.id.map(Value::coerce_to_i64) {
										if let Some((_method, sender)) = routes.remove(&id) {
											let _res = sender
												.into_send_async(DbResponse::from(response.result))
												.await;
										}
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
										if let Some(Ok(id)) = id.map(Value::coerce_to_i64) {
											if let Some((_method, sender)) = routes.remove(&id) {
												let _res = sender.into_send_async(Err(error)).await;
											}
										}
									} else {
										// Unfortunately, we don't know which response failed to deserialize
										warn!("Failed to deserialise message; {error:?}");
									}
								}
							}
						}
					}
					Either::Event(event) => match event {
						WsEvent::Error => {
							trace!("connection errored");
							break;
						}
						WsEvent::WsErr(error) => {
							trace!("{error}");
						}
						WsEvent::Closed(..) => {
							trace!("connection closed");
							break;
						}
						_ => {}
					},
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

			'reconnect: loop {
				trace!("Reconnecting...");
				match WsMeta::connect(&address.url, None).await {
					Ok((mut meta, stream)) => {
						socket = stream;
						events = {
							let result = match capacity {
								0 => meta.observe(ObserveConfig::default()).await,
								capacity => meta.observe(Channel::Bounded(capacity).into()).await,
							};
							match result {
								Ok(events) => events,
								Err(error) => {
									trace!("{error}");
									time::sleep(Duration::from_secs(1)).await;
									continue 'reconnect;
								}
							}
						};
						for (_, message) in &replay {
							if let Err(error) = socket.send(message.clone()).await {
								trace!("{error}");
								time::sleep(Duration::from_secs(1)).await;
								continue 'reconnect;
							}
						}
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
								time::sleep(Duration::from_secs(1)).await;
								continue 'reconnect;
							}
						}
						trace!("Reconnected successfully");
						break;
					}
					Err(error) => {
						trace!("Failed to reconnect; {error}");
						time::sleep(Duration::from_secs(1)).await;
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
		}
	}
}
