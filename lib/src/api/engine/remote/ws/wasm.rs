use super::{
	deserialize, serialize, HandleResult, PendingRequest, ReplayMethod, RequestEffect, PATH,
};
use crate::api::conn::DbResponse;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::conn::{Command, Connection, RequestData};
use crate::api::engine::remote::ws::Client;
use crate::api::engine::remote::ws::Response;
use crate::api::engine::remote::ws::PING_INTERVAL;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::engine::remote::ws::Data;
use crate::engine::IntervalStream;
use crate::opt::WaitFor;
use crate::sql::Value;
use channel::{Receiver, Sender};
use futures::stream::{SplitSink, SplitStream};
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use pharos::Channel;
use pharos::Events;
use pharos::Observable;
use pharos::ObserveConfig;
use revision::revisioned;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::watch;
use trice::Instant;
use wasm_bindgen_futures::spawn_local;
use wasmtimer::tokio as time;
use wasmtimer::tokio::MissedTickBehavior;
use ws_stream_wasm::WsMessage as Message;
use ws_stream_wasm::WsMeta;
use ws_stream_wasm::{WsEvent, WsStream};

type MessageStream = SplitStream<WsStream>;
type MessageSink = SplitSink<WsStream, Message>;
type RouterState = super::RouterState<MessageSink, MessageStream>;

impl crate::api::Connection for Client {}

impl Connection for Client {
	fn connect(
		mut address: Endpoint,
		capacity: usize,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			address.url = address.url.join(PATH)?;

			let (route_tx, route_rx) = match capacity {
				0 => channel::unbounded(),
				capacity => channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = channel::bounded(1);

			spawn_local(run_router(address, capacity, conn_tx, route_rx));

			conn_rx.recv().await??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::LiveQueries);

			Ok(Surreal::new_from_router_waiter(
				Arc::new(OnceLock::with_value(Router {
					features,
					sender: route_tx,
					last_id: AtomicI64::new(0),
				})),
				Arc::new(watch::channel(Some(WaitFor::Connection))),
			))
		})
	}
}

async fn router_handle_request(
	Route {
		request,
		response,
	}: Route,
	state: &mut RouterState,
	endpoint: &Endpoint,
) -> HandleResult {
	let RequestData {
		id,
		command,
	} = request;

	let entry = state.pending_requests.entry(id);
	// We probably shouldn't be sending duplicate id requests.
	let Entry::Vacant(entry) = entry else {
		let error = Error::DuplicateRequestId(id);
		if response.send(Err(error.into())).await.is_err() {
			trace!("Receiver dropped");
		}
		return HandleResult::Ok;
	};

	let mut effect = RequestEffect::None;

	match command {
		Command::Set {
			ref key,
			ref value,
		} => {
			effect = RequestEffect::Set {
				key: key.clone(),
				value: value.clone(),
			};
		}
		Command::Unset {
			ref key,
		} => {
			effect = RequestEffect::Clear {
				key: key.clone(),
			};
		}
		Command::Insert {
			..
		} => {
			effect = RequestEffect::Insert;
		}
		Command::SubscribeLive {
			ref uuid,
			ref notification_sender,
		} => {
			state.live_queries.insert(*uuid, notification_sender.clone());
			if response.send(Ok(DbResponse::Other(Value::None))).await.is_err() {
				trace!("Receiver dropped");
			}
			// There is nothing to send to the server here
			return HandleResult::Ok;
		}
		Command::Kill {
			ref uuid,
		} => {
			state.live_queries.remove(uuid);
		}
		Command::Use {
			..
		} => {
			state.replay.insert(ReplayMethod::Use, command.clone());
		}
		Command::Signup {
			..
		} => {
			state.replay.insert(ReplayMethod::Signup, command.clone());
		}
		Command::Signin {
			..
		} => {
			state.replay.insert(ReplayMethod::Signin, command.clone());
		}
		Command::Invalidate {
			..
		} => {
			state.replay.insert(ReplayMethod::Invalidate, command.clone());
		}
		Command::Authenticate {
			..
		} => {
			state.replay.insert(ReplayMethod::Authenticate, command.clone());
		}
		_ => {}
	}

	let message = {
		let Some(req) = command.into_router_request(Some(id)) else {
			let _ = response.send(Err(Error::BackupsNotSupported.into())).await;
			return HandleResult::Ok;
		};
		trace!("Request {:?}", req);
		let payload = serialize(&req, endpoint.supports_revision).unwrap();
		Message::Binary(payload)
	};

	match state.sink.send(message).await {
		Ok(..) => {
			state.last_activity = Instant::now();
			entry.insert(PendingRequest {
				effect,
				response_channel: response,
			});
		}
		Err(error) => {
			let error = Error::Ws(error.to_string());
			if response.send(Err(error.into())).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Disconnected;
		}
	}
	HandleResult::Ok
}

async fn router_handle_response(
	response: Message,
	state: &mut RouterState,
	endpoint: &Endpoint,
) -> HandleResult {
	match Response::try_from(&response, endpoint.supports_revision) {
		Ok(option) => {
			// We are only interested in responses that are not empty
			if let Some(response) = option {
				trace!("{response:?}");
				match response.id {
					// If `id` is set this is a normal response
					Some(id) => {
						if let Ok(id) = id.coerce_to_i64() {
							// We can only route responses with IDs
							if let Some(pending) = state.pending_requests.remove(&id) {
								match pending.effect {
									RequestEffect::None => {}
									RequestEffect::Insert => {
										// For insert, we need to flatten single responses in an array
										if let Ok(Data::Other(Value::Array(value))) =
											response.result
										{
											if value.len() == 1 {
												let _ = pending
													.response_channel
													.send(DbResponse::from(Ok(Data::Other(
														value.into_iter().next().unwrap(),
													))))
													.await;
											} else {
												let _ = pending
													.response_channel
													.send(DbResponse::from(Ok(Data::Other(
														Value::Array(value),
													))))
													.await;
											}
											return HandleResult::Ok;
										}
									}
									RequestEffect::Set {
										key,
										value,
									} => {
										state.vars.insert(key, value);
									}
									RequestEffect::Clear {
										key,
									} => {
										state.vars.shift_remove(&key);
									}
								}
								let _res = pending
									.response_channel
									.send(DbResponse::from(response.result))
									.await;
							} else {
								warn!("got response for request with id '{id}', which was not in pending requests")
							}
						}
					}
					// If `id` is not set, this may be a live query notification
					None => match response.result {
						Ok(Data::Live(notification)) => {
							let live_query_id = notification.id;
							// Check if this live query is registered
							if let Some(sender) = state.live_queries.get(&live_query_id) {
								// Send the notification back to the caller or kill live query if the receiver is already dropped
								if sender.send(notification).await.is_err() {
									state.live_queries.remove(&live_query_id);
									let kill = {
										let request = Command::Kill {
											uuid: live_query_id.0,
										}
										.into_router_request(None);
										let value = serialize(&request, endpoint.supports_revision)
											.unwrap();
										Message::Binary(value)
									};
									if let Err(error) = state.sink.send(kill).await {
										trace!(
											"failed to send kill query to the server; {error:?}"
										);
										return HandleResult::Disconnected;
									}
								}
							}
						}
						Ok(..) => { /* Ignored responses like pings */ }
						Err(error) => error!("{error:?}"),
					},
				}
			}
		}
		Err(error) => {
			#[derive(Deserialize)]
			#[revisioned(revision = 1)]
			struct Response {
				id: Option<Value>,
			}

			// Let's try to find out the ID of the response that failed to deserialise
			if let Message::Binary(binary) = response {
				if let Ok(Response {
					id,
				}) = deserialize(&mut &binary[..], endpoint.supports_revision)
				{
					// Return an error if an ID was returned
					if let Some(Ok(id)) = id.map(Value::coerce_to_i64) {
						if let Some(req) = state.pending_requests.remove(&id) {
							let _res = req.response_channel.send(Err(error)).await;
						} else {
							warn!("got response for request with id '{id}', which was not in pending requests")
						}
					}
				} else {
					// Unfortunately, we don't know which response failed to deserialize
					warn!("Failed to deserialise message; {error:?}");
				}
			}
		}
	}
	HandleResult::Ok
}

async fn router_reconnect(
	state: &mut RouterState,
	events: &mut Events<WsEvent>,
	endpoint: &Endpoint,
	capacity: usize,
) {
	loop {
		trace!("Reconnecting...");
		let connect = match endpoint.supports_revision {
			true => WsMeta::connect(&endpoint.url, vec![super::REVISION_HEADER]).await,
			false => WsMeta::connect(&endpoint.url, None).await,
		};
		match connect {
			Ok((mut meta, stream)) => {
				let (new_sink, new_stream) = stream.split();
				state.sink = new_sink;
				state.stream = new_stream;
				*events = {
					let result = match capacity {
						0 => meta.observe(ObserveConfig::default()).await,
						capacity => meta.observe(Channel::Bounded(capacity).into()).await,
					};
					match result {
						Ok(events) => events,
						Err(error) => {
							trace!("{error}");
							time::sleep(Duration::from_secs(1)).await;
							continue;
						}
					}
				};
				for (_, message) in &state.replay {
					let message = message.clone().into_router_request(None);
					let message = serialize(&message, endpoint.supports_revision).unwrap();

					if let Err(error) = state.sink.send(Message::Binary(message)).await {
						trace!("{error}");
						time::sleep(Duration::from_secs(1)).await;
						continue;
					}
				}
				for (key, value) in &state.vars {
					let request = Command::Set {
						key: key.as_str().into(),
						value: value.clone(),
					}
					.into_router_request(None);
					trace!("Request {:?}", request);
					let serialize = serialize(&request, false).unwrap();
					if let Err(error) = state.sink.send(Message::Binary(serialize)).await {
						trace!("{error}");
						time::sleep(Duration::from_secs(1)).await;
						continue;
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

pub(crate) async fn run_router(
	endpoint: Endpoint,
	capacity: usize,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
) {
	let connect = match endpoint.supports_revision {
		true => WsMeta::connect(&endpoint.url, vec![super::REVISION_HEADER]).await,
		false => WsMeta::connect(&endpoint.url, None).await,
	};
	let (mut ws, socket) = match connect {
		Ok(pair) => pair,
		Err(error) => {
			let _ = conn_tx.send(Err(error.into())).await;
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
				let _ = conn_tx.send(Err(error.into())).await;
				return;
			}
		}
	};

	let _ = conn_tx.send(Ok(())).await;

	let ping = {
		let mut request = BTreeMap::new();
		request.insert("method".to_owned(), "ping".into());
		let value = Value::from(request);
		let value = serialize(&value, endpoint.supports_revision).unwrap();
		Message::Binary(value)
	};

	let (socket_sink, socket_stream) = socket.split();

	let mut state = RouterState::new(socket_sink, socket_stream);

	'router: loop {
		let mut interval = time::interval(PING_INTERVAL);
		// don't bombard the server with pings if we miss some ticks
		interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

		let mut pinger = IntervalStream::new(interval);

		state.last_activity = Instant::now();
		state.live_queries.clear();
		state.pending_requests.clear();

		loop {
			futures::select! {
				route = route_rx.recv().fuse() => {
					let Ok(route) = route else {
						match ws.close().await {
							Ok(..) => trace!("Connection closed successfully"),
							Err(error) => {
								warn!("Failed to close database connection; {error}")
							}
						}
						break 'router;
					};

					match router_handle_request(route, &mut state,&endpoint).await {
						HandleResult::Ok => {},
						HandleResult::Disconnected => {
							router_reconnect(&mut state, &mut events, &endpoint, capacity).await;
							break
						}
					}
				}
				message = state.stream.next().fuse() => {
					let Some(message) = message else {
						// socket disconnected,
							router_reconnect(&mut state, &mut events, &endpoint, capacity).await;
							break
					};

					state.last_activity = Instant::now();
					match router_handle_response(message, &mut state,&endpoint).await {
						HandleResult::Ok => {},
						HandleResult::Disconnected => {
							router_reconnect(&mut state, &mut events, &endpoint, capacity).await;
							break
						}
					}
				}
				event = events.next().fuse() => {
					let Some(event) = event else {
						continue;
					};
					match event {
						WsEvent::Error => {
							trace!("connection errored");
							break;
						}
						WsEvent::WsErr(error) => {
							trace!("{error}");
						}
						WsEvent::Closed(..) => {
							trace!("connection closed");
							router_reconnect(&mut state, &mut events, &endpoint, capacity).await;
							break;
						}
						_ => {}
					}
				}
				_ = pinger.next().fuse() => {
					if state.last_activity.elapsed() >= PING_INTERVAL {
						trace!("Pinging the server");
						if let Err(error) = state.sink.send(ping.clone()).await {
							trace!("failed to ping the server; {error:?}");
							router_reconnect(&mut state, &mut events, &endpoint, capacity).await;
							break;
						}
					}
				}
			}
		}
	}
}

impl Response {
	fn try_from(message: &Message, supports_revision: bool) -> Result<Option<Self>> {
		match message {
			Message::Text(text) => {
				trace!("Received an unexpected text message; {text}");
				Ok(None)
			}
			Message::Binary(binary) => {
				deserialize(&mut &binary[..], supports_revision).map(Some).map_err(|error| {
					Error::ResponseFromBinary {
						binary: binary.clone(),
						error: bincode::ErrorKind::Custom(error.to_string()).into(),
					}
					.into()
				})
			}
		}
	}
}
