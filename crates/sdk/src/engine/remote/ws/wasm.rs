use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use futures::stream::{SplitSink, SplitStream};
use futures::{FutureExt, SinkExt, StreamExt};
use pharos::{Channel, Events, Observable, ObserveConfig};
use surrealdb_core::dbs::QueryResultBuilder;
use surrealdb_core::iam::Token;
use surrealdb_core::rpc::{DbResponse, DbResult};
use surrealdb_types::{Array, SurrealValue, Value, Variables, object};
use tokio::sync::watch;
use trice::Instant;
use wasm_bindgen_futures::spawn_local;
use wasmtimer::tokio as time;
use wasmtimer::tokio::MissedTickBehavior;
use ws_stream_wasm::{WsEvent, WsMessage as Message, WsMeta, WsStream};

use super::{HandleResult, PATH, PendingRequest, ReplayMethod, RequestEffect};
use crate::conn::cmd::RouterRequest;
use crate::conn::{self, Command, RequestData, Route, Router};
use crate::engine::IntervalStream;
use crate::engine::remote::ws::{Client, PING_INTERVAL};
use crate::err::Error;
use crate::method::BoxFuture;
use crate::opt::{Endpoint, WaitFor};
use crate::{ExtraFeatures, Result, Surreal};

type MessageStream = SplitStream<WsStream>;
type MessageSink = SplitSink<WsStream, Message>;
type RouterState = super::RouterState<MessageSink, MessageStream>;

impl crate::Connection for Client {}
impl conn::Sealed for Client {
	fn connect(
		mut address: Endpoint,
		capacity: usize,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			address.url = address.url.join(PATH)?;

			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded(1);
			let config = address.config.clone();

			spawn_local(run_router(address, capacity, conn_tx, route_rx));

			conn_rx.recv().await??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::LiveQueries);

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

async fn router_handle_request(
	Route {
		request,
		response,
	}: Route,
	state: &mut RouterState,
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

	// Merge stored vars with query vars for RawQuery
	let command = match command {
		Command::RawQuery {
			txn,
			query,
			variables,
		} => {
			let mut merged_vars =
				state.vars.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Variables>();
			merged_vars.extend(variables);
			Command::RawQuery {
				txn,
				query,
				variables: merged_vars,
			}
		}
		other => other,
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
		Command::SubscribeLive {
			ref uuid,
			ref notification_sender,
		} => {
			state.live_queries.insert(*uuid, notification_sender.clone());
			if response.send(Ok(vec![QueryResultBuilder::instant_none()])).await.is_err() {
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
			ref token,
		} => {
			effect = RequestEffect::Authenticate {
				token: Some(token.clone()),
			};
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
		let req_value = req.into_value();
		let payload = surrealdb_core::rpc::format::flatbuffers::encode(&req_value).unwrap();
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
	_endpoint: &Endpoint,
) -> HandleResult {
	match ws_message_to_db_response(&response) {
		Ok(option) => {
			// We are only interested in responses that are not empty
			if let Some(response) = option {
				trace!("{response:?}");
				match response.id {
					// If `id` is set this is a normal response
					Some(id) => {
						if let Ok(id) = id.into_int() {
							// We can only route responses with IDs
							if let Some(mut pending) = state.pending_requests.remove(&id) {
								match response.result {
									Ok(DbResult::Query(results)) => {
										// Apply effect only on success
										match pending.effect {
											RequestEffect::None => {}
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
											_ => {}
										}
										let _res = pending.response_channel.send(Ok(results)).await;
									}
									Ok(DbResult::Live(_notification)) => {
										// Live queries should not be handled here
										warn!("Unexpected live query result in response");
									}
									Ok(DbResult::Other(_value)) => {
										// Apply effect only on success
										match pending.effect {
											RequestEffect::None => {}
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
											RequestEffect::Authenticate {
												..
											} => {}
										}
										// Other results should be converted to a single result vec
										let _res = pending
											.response_channel
											.send(Ok(vec![QueryResultBuilder::instant_none()]))
											.await;
									}
									Err(error) => {
										// Automatic refresh token handling:
										// If a request fails with "token has expired" and we have a
										// refresh token, automatically attempt to refresh
										// the authentication and retry the request.
										if let RequestEffect::Authenticate {
											token: Some(token),
										} = pending.effect
										{
											// Check if this is a token with refresh capability
											if let Token::WithRefresh {
												..
											} = &token
											{
												// If the error is due to token expiration, attempt
												// automatic refresh
												if error.to_string().contains("token has expired") {
													// Construct a new authentication request with
													// the token (which includes the
													// refresh token for automatic renewal)
													let request = RouterRequest {
														id: Some(id),
														method: "authenticate",
														params: Some(Value::Array(Array::from(
															vec![token.into_value()],
														))),
														transaction: None,
													};
													let request_value = request.into_value();
													let value =
											surrealdb_core::rpc::format::flatbuffers::encode(
												&request_value,
											)
											.expect("router request should serialize");
													let message = Message::Binary(value.into());
													// Send the refresh request
													match state.sink.send(message).await {
														Err(send_error) => {
															trace!(
																"failed to send refresh query to the server; {send_error:?}"
															);
															// If we can't send the refresh request,
															// return the original error
															pending
																.response_channel
																.send(Err(error))
																.await
																.ok();
														}
														Ok(..) => {
															// Successfully queued the refresh
															// request.
															// Clear the token from the effect to
															// prevent infinite retry loops,
															// and keep the request pending for
															// retry after refresh succeeds.
															pending.effect =
																RequestEffect::Authenticate {
																	token: None,
																};
															state
																.pending_requests
																.insert(id, pending);
														}
													}

													return HandleResult::Ok;
												}
											}
										}

										// For all other errors, or if automatic refresh isn't
										// applicable, return the error to the caller
										pending.response_channel.send(Err(error)).await.ok();
									}
								}
							} else {
								warn!(
									"got response for request with id '{id}', which was not in pending requests"
								)
							}
						}
					}
					// If `id` is not set, this may be a live query notification
					None => match response.result {
						Ok(DbResult::Live(notification)) => {
							let live_query_id = notification.id;
							// Check if this live query is registered
							if let Some(sender) = state.live_queries.get(&live_query_id) {
								// Send the notification back to the caller or kill live query if
								// the receiver is already dropped
								if sender.send(Ok(notification)).await.is_err() {
									state.live_queries.remove(&live_query_id);
									let kill = {
										let request = Command::Kill {
											uuid: live_query_id.0,
										}
										.into_router_request(None)
										.into_value();

										let value =
											surrealdb_core::rpc::format::flatbuffers::encode(
												&request,
											)
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
			#[derive(SurrealValue)]
			struct ErrorResponse {
				id: Option<Value>,
			}

			// Let's try to find out the ID of the response that failed to deserialise
			if let Message::Binary(binary) = response {
				if let Ok(ErrorResponse {
					id,
				}) = surrealdb_core::rpc::format::flatbuffers::decode(&binary)
				{
					// Return an error if an ID was returned
					if let Some(Ok(id)) = id.map(Value::into_int) {
						if let Some(req) = state.pending_requests.remove(&id) {
							let _res = req.response_channel.send(Err(error.into())).await;
						} else {
							warn!(
								"got response for request with id '{id}', which was not in pending requests"
							)
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
		let connect = WsMeta::connect(&endpoint.url, vec!["flatbuffers"]).await;
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
					let message = message.clone().into_router_request(None).into_value();

					let message =
						surrealdb_core::rpc::format::flatbuffers::encode(&message).unwrap();

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
					.into_router_request(None)
					.into_value();

					trace!("Request {:?}", request);
					let serialize =
						surrealdb_core::rpc::format::flatbuffers::encode(&request).unwrap();
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
	let connect = WsMeta::connect(&endpoint.url, vec!["flatbuffers"]).await;
	let (mut ws, socket) = match connect {
		Ok(pair) => pair,
		Err(error) => {
			let _ = conn_tx.send(Err(Error::Ws(error.to_string()).into())).await;
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
				let _ = conn_tx.send(Err(Error::Ws(error.to_string()).into())).await;
				return;
			}
		}
	};

	let _ = conn_tx.send(Ok(())).await;

	let ping = {
		let value = Value::Object(object! {
			"method": "ping",
		});
		let value = surrealdb_core::rpc::format::flatbuffers::encode(&value).unwrap();
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
		state.reset().await;

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

					match router_handle_request(route, &mut state).await {
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
							state.reset().await;
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

fn ws_message_to_db_response(message: &Message) -> Result<Option<DbResponse>> {
	match message {
		Message::Text(text) => {
			trace!("Received an unexpected text message; {text}");
			Ok(None)
		}
		Message::Binary(binary) => surrealdb_core::rpc::format::flatbuffers::decode(&binary)
			.map(Some)
			.map_err(|error| Error::InvalidResponse(error.to_string()).into()),
	}
}
