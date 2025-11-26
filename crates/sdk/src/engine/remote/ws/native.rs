use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicI64;

use async_channel::Receiver;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use surrealdb_core::dbs::QueryResultBuilder;
use surrealdb_core::iam::token::Token;
use surrealdb_core::rpc::{DbResponse, DbResult};
use surrealdb_types::{Array, SurrealValue};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};
use trice::Instant;

use super::{HandleResult, PATH, PendingRequest, ReplayMethod, RequestEffect};
use crate::conn::cmd::RouterRequest;
use crate::conn::{self, Command, RequestData, Route, Router};
use crate::engine::IntervalStream;
use crate::engine::remote::ws::{Client, PING_INTERVAL};
use crate::err::Error;
use crate::method::BoxFuture;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::opt::Tls;
use crate::opt::{Endpoint, WaitFor};
use crate::types::{Value, Variables};
use crate::{ExtraFeatures, Result, Surreal};

pub(crate) const NAGLE_ALG: bool = false;

type MessageSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type MessageStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type RouterState = super::RouterState<MessageSink, MessageStream>;

#[cfg(any(feature = "native-tls", feature = "rustls"))]
impl From<Tls> for Connector {
	fn from(tls: Tls) -> Self {
		match tls {
			#[cfg(feature = "native-tls")]
			Tls::Native(config) => Self::NativeTls(config),
			#[cfg(feature = "rustls")]
			Tls::Rust(config) => Self::Rustls(std::sync::Arc::new(config)),
		}
	}
}

pub(crate) async fn connect(
	endpoint: &Endpoint,
	config: Option<WebSocketConfig>,
	#[cfg_attr(not(any(feature = "native-tls", feature = "rustls")), expect(unused_variables))]
	maybe_connector: Option<Connector>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
	let mut request =
		(&endpoint.url).into_client_request().map_err(|err| Error::InvalidUrl(err.to_string()))?;

	request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("flatbuffers"));

	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(
		request,
		config,
		NAGLE_ALG,
		maybe_connector,
	)
	.await
	.map_err(|err| Error::Ws(err.to_string()))?;

	#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
	let (socket, _) = tokio_tungstenite::connect_async_with_config(request, config, NAGLE_ALG)
		.await
		.map_err(|err| Error::Ws(err.to_string()))?;

	Ok(socket)
}

impl crate::Connection for Client {}
impl conn::Sealed for Client {
	fn connect(
		mut address: Endpoint,
		capacity: usize,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			address.url = address.url.join(PATH)?;
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			let maybe_connector = address.config.tls_config.clone().map(Connector::from);
			#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
			let maybe_connector = None;

			let ws_config = WebSocketConfig::default()
				.read_buffer_size(address.config.websocket.read_buffer_size)
				.max_message_size(address.config.websocket.max_message_size)
				.max_frame_size(address.config.websocket.max_message_size)
				.max_write_buffer_size(address.config.websocket.max_write_buffer_size)
				.write_buffer_size(address.config.websocket.write_buffer_size);

			let socket = connect(&address, Some(ws_config), maybe_connector.clone()).await?;

			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};
			let config = address.config.clone();

			tokio::spawn(run_router(
				address,
				maybe_connector,
				capacity,
				ws_config,
				socket,
				route_rx,
			));

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

async fn router_handle_route(
	Route {
		request,
		response,
	}: Route,
	max_message_size: Option<usize>,
	state: &mut RouterState,
) -> HandleResult {
	let RequestData {
		id,
		command,
	} = request;

	// We probably shouldn't be sending duplicate id requests.
	let entry = state.pending_requests.entry(id);
	let Entry::Vacant(entry) = entry else {
		let error = Error::DuplicateRequestId(id);
		if response.send(Err(error.into())).await.is_err() {
			trace!("Receiver dropped");
		}
		return HandleResult::Ok;
	};

	// Merge stored vars with query vars for Query
	let command = match command {
		Command::Query {
			txn,
			query,
			variables,
		} => {
			let mut merged_vars =
				state.vars.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Variables>();
			merged_vars.extend(variables);
			Command::Query {
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
			if response.clone().send(Ok(vec![QueryResultBuilder::instant_none()])).await.is_err() {
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
		Command::Invalidate => {
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
		let Some(request) = command.into_router_request(Some(id)) else {
			let _ = response.send(Err(Error::BackupsNotSupported.into())).await;
			return HandleResult::Ok;
		};

		let request_value = request.into_value();

		// Router request should not fail to serialize
		let payload = surrealdb_core::rpc::format::flatbuffers::encode(&request_value)
			.expect("router request should serialize");

		Message::Binary(payload.into())
	};

	if let Some(max_message_size) = max_message_size {
		let size = message.len();
		if size > max_message_size {
			if response.send(Err(Error::MessageTooLong(size).into())).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Ok;
		}
	}

	match state.sink.send(message).await {
		Ok(_) => {
			state.last_activity = Instant::now();
			entry.insert(PendingRequest {
				effect,
				response_channel: response,
			});
		}
		Err(error) => {
			let err = Error::Ws(error.to_string());
			if response.send(Err(err.into())).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Disconnected;
		}
	}
	HandleResult::Ok
}

async fn router_handle_response(message: Message, state: &mut RouterState) -> HandleResult {
	match db_response_from_message(&message) {
		Ok(response) => {
			let Some(response) = response else {
				return HandleResult::Ok;
			};

			match response.id {
				// If `id` is set this is a normal response
				Some(id) => {
					// Try to extract i64 from Value
					if let Value::Number(surrealdb_types::Number::Int(id_num)) = id {
						match state.pending_requests.remove(&id_num) {
							Some(mut pending) => {
								// We can only route responses with IDs
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
											RequestEffect::Authenticate {
												..
											} => { /* Authenticate responses are handled in the `DBResult::Other` variant */
											}
										}
										if let Err(err) =
											pending.response_channel.send(Ok(results)).await
										{
											tracing::error!(
												"Failed to send query results to channel: {err:?}"
											);
										}
									}
									Ok(DbResult::Live(_notification)) => {
										tracing::error!("Unexpected live query result in response");
									}
									Ok(DbResult::Other(mut value)) => {
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
												token,
											} => {
												if let Some(token) = token {
													value = token.into_value();
												}
											}
										}
										let result = QueryResultBuilder::started_now()
											.finish_with_result(Ok(value));
										if let Err(err) =
											pending.response_channel.send(Ok(vec![result])).await
										{
											tracing::error!(
												"Failed to send query results to channel: {err:?}"
											);
										}
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
														id: Some(id_num),
														method: "authenticate",
														params: Some(Value::Array(Array::from(
															vec![token.into_value()],
														))),
														txn: None,
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
																.insert(id_num, pending);
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
							}
							_ => {
								warn!(
									"got response for request with id '{id_num}', which was not in pending requests"
								)
							}
						}
					}
				}
				// If `id` is not set, this may be a live query notification
				None => {
					if let Ok(DbResult::Live(notification)) = response.result {
						let live_query_id = notification.id.0;
						// Check if this live query is registered
						if let Some(sender) = state.live_queries.get(&live_query_id) {
							// Send the notification back to the caller or kill live query
							// if the receiver is already dropped
							if sender.send(Ok(notification)).await.is_err() {
								state.live_queries.remove(&live_query_id);
								let kill = {
									let request = Command::Kill {
										uuid: live_query_id,
									}
									.into_router_request(None)
									.expect("KILL command should convert to router request");

									let request_value = request.into_value();

									let value = surrealdb_core::rpc::format::flatbuffers::encode(
										&request_value,
									)
									.expect("router request should serialize");
									Message::Binary(value.into())
								};
								if let Err(error) = state.sink.send(kill).await {
									trace!("failed to send kill query to the server; {error:?}");
									return HandleResult::Disconnected;
								}
							}
						}
					}
				}
			}
		}
		Err(error) => {
			#[derive(Deserialize, SurrealValue)]
			struct ErrorResponse {
				id: Option<Value>,
			}

			// Let's try to find out the ID of the response that failed to deserialise
			if let Message::Binary(binary) = message {
				match surrealdb_core::rpc::format::flatbuffers::decode(&binary) {
					Ok(ErrorResponse {
						id,
					}) => {
						// Return an error if an ID was returned
						if let Some(Value::Number(surrealdb_types::Number::Int(id_num))) = id {
							match state.pending_requests.remove(&id_num) {
								Some(pending) => {
									let _res =
										pending.response_channel.send(Err(error.into())).await;
								}
								_ => {
									warn!(
										"got response for request with id '{id_num}', which was not in pending requests"
									)
								}
							}
						}
					}
					_ => {
						// Unfortunately, we don't know which response failed to deserialize
						warn!("Failed to deserialise message; {error:?}");
					}
				}
			}
		}
	}
	HandleResult::Ok
}

fn db_response_from_message(message: &Message) -> Result<Option<DbResponse>> {
	match message {
		Message::Text(text) => {
			trace!("Received an unexpected text message; {text}");
			Ok(None)
		}
		Message::Binary(binary) => Ok(Some(DbResponse::from_bytes(binary)?)),
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

async fn router_reconnect(
	maybe_connector: &Option<Connector>,
	config: &WebSocketConfig,
	state: &mut RouterState,
	endpoint: &Endpoint,
) {
	loop {
		trace!("Reconnecting...");
		match connect(endpoint, Some(*config), maybe_connector.clone()).await {
			Ok(s) => {
				let (new_sink, new_stream) = s.split();
				state.sink = new_sink;
				state.stream = new_stream;
				for commands in state.replay.values() {
					let request = commands
						.clone()
						.into_router_request(None)
						.expect("replay commands should always convert to route requests");

					let request_value = request.into_value();

					let message = surrealdb_core::rpc::format::flatbuffers::encode(&request_value)
						.expect("router request should serialize");

					if let Err(error) = state.sink.send(Message::Binary(message.into())).await {
						trace!("{error}");
						time::sleep(time::Duration::from_secs(1)).await;
						continue;
					}
				}
				for (key, value) in &state.vars {
					let request = Command::Set {
						key: key.as_str().into(),
						value: value.clone(),
					}
					.into_router_request(None)
					.expect("SET command should convert to router request");
					trace!("Request {:?}", request);
					let request_value = request.into_value();
					let payload = surrealdb_core::rpc::format::flatbuffers::encode(&request_value)
						.expect("router request should serialize");

					if let Err(error) = state.sink.send(Message::Binary(payload.into())).await {
						trace!("{error}");
						time::sleep(time::Duration::from_secs(1)).await;
						continue;
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

pub(crate) async fn run_router(
	endpoint: Endpoint,
	maybe_connector: Option<Connector>,
	_capacity: usize,
	config: WebSocketConfig,
	socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
	route_rx: Receiver<Route>,
) {
	let ping = {
		let request = Command::Health
			.into_router_request(None)
			.expect("HEALTH command should convert to router request");
		let request_value = request.into_value();
		let value = surrealdb_core::rpc::format::flatbuffers::encode(&request_value)
			.expect("router request should serialize");
		Message::Binary(value.into())
	};

	let (socket_sink, socket_stream) = socket.split();
	let mut state = RouterState::new(socket_sink, socket_stream);

	'router: loop {
		let mut interval = time::interval(PING_INTERVAL);
		// don't bombard the server with pings if we miss some ticks
		interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

		let mut pinger = IntervalStream::new(interval);
		// Turn into a stream instead of calling recv_async
		// The stream seems to be able to keep some state which would otherwise need to
		// be recreated with each next.

		state.last_activity = Instant::now();
		state.reset().await;

		loop {
			tokio::select! {
				route = route_rx.recv() => {
					// handle incoming route

					let Ok(response) = route else {
						// route returned Err, frontend dropped the channel, meaning the router
						// should quit.
						match state.sink.send(Message::Close(None)).await {
							Ok(..) => trace!("Connection closed successfully"),
							Err(error) => {
								warn!("Failed to close database connection; {error}")
							}
						}
						break 'router;
					};

					match router_handle_route(response, config.max_message_size, &mut state).await {
						HandleResult::Ok => {},
						HandleResult::Disconnected => {
							router_reconnect(
								&maybe_connector,
								&config,
								&mut state,
								&endpoint,
							)
							.await;
							continue 'router;
						}
					}
				}
				result = state.stream.next() => {
					// Handle result from database.

					let Some(result) = result else {
						// stream returned none meaning the connection dropped, try to reconnect.
						router_reconnect(
							&maybe_connector,
							&config,
							&mut state,
							&endpoint,
						)
						.await;
						continue 'router;
					};

					state.last_activity = Instant::now();
					match result {
						Ok(message) => {
							match router_handle_response(message, &mut state).await {
								HandleResult::Ok => continue,
								HandleResult::Disconnected => {
									router_reconnect(
										&maybe_connector,
										&config,
										&mut state,
										&endpoint,
									)
									.await;
									continue 'router;
								}
							}
						}
						Err(error) => {
							state.reset().await;
							match error {
								WsError::ConnectionClosed => {
									trace!("Connection successfully closed on the server");
								}
								error => {
									trace!("{error}");
								}
							}
							router_reconnect(
								&maybe_connector,
								&config,
								&mut state,
								&endpoint,
							)
							.await;
							continue 'router;
						}
					}
				}
				_ = pinger.next() => {
					// only ping if we haven't talked to the server recently
					if state.last_activity.elapsed() >= PING_INTERVAL {
						trace!("Pinging the server");
						if let Err(error) = state.sink.send(ping.clone()).await {
							trace!("failed to ping the server; {error:?}");
							router_reconnect(
								&maybe_connector,
								&config,
								&mut state,
								&endpoint,
							)
							.await;
							continue 'router;
						}
					}

				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::io::Write;
	use std::time::SystemTime;

	use flate2::Compression;
	use flate2::write::GzEncoder;
	use rand::{Rng, thread_rng};
	use surrealdb_core::rpc;

	use crate::types::{Array, Value};

	#[test_log::test]
	fn large_vector_serialisation_bench() {
		//
		let timed = |func: &dyn Fn() -> Vec<u8>| {
			let start = SystemTime::now();
			let r = func();
			(start.elapsed().unwrap(), r)
		};
		//
		let compress = |v: &Vec<u8>| {
			let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
			encoder.write_all(v).unwrap();
			encoder.finish().unwrap()
		};
		// Generate a random vector
		let vector_size = if cfg!(debug_assertions) {
			200_000 // Debug is slow
		} else {
			2_000_000 // Release is fast
		};
		let mut vector: Vec<i32> = Vec::new();
		let mut rng = thread_rng();
		for _ in 0..vector_size {
			vector.push(rng.r#gen());
		}
		//	Store the results
		let mut results = vec![];
		// Calculate the reference using FlatBuffers (what we actually use in production)
		let ref_payload;
		let ref_compressed;

		// Build the Value
		let vector = Value::Array(Array::from(vector));

		const FLATBUFFERS: &str = "Flatbuffers Vec<Value>";
		const FLATBUFFERS_COMPRESSED: &str = "Flatbuffers Compressed Vec<Value>";
		{
			// FlatBuffers uncompressed - this is our reference
			let (duration, payload) =
				timed(&|| surrealdb_core::rpc::format::flatbuffers::encode(&vector).unwrap());
			ref_payload = payload.len() as f32;
			results.push((payload.len(), FLATBUFFERS, duration, 1.0));

			// FlatBuffers compressed
			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			ref_compressed = payload.len() as f32;
			results.push((payload.len(), FLATBUFFERS_COMPRESSED, duration, 1.0));
		}
		//
		const CBOR: &str = "CBor Vec<Value>";
		const COMPRESSED_CBOR: &str = "Compressed CBor Vec<Value>";
		{
			// CBor
			let (duration, payload) = timed(&|| {
				let cbor = rpc::format::cbor::encode(vector.clone()).unwrap();
				let mut res = Vec::new();
				ciborium::into_writer(&cbor, &mut res).unwrap();
				res
			});
			results.push((payload.len(), CBOR, duration, payload.len() as f32 / ref_payload));

			// Compressed Cbor
			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			results.push((
				payload.len(),
				COMPRESSED_CBOR,
				duration,
				payload.len() as f32 / ref_compressed,
			));
		}
		// Sort the results by ascending size
		results.sort_by(|(a, _, _, _), (b, _, _, _)| a.cmp(b));
		for (size, name, duration, factor) in &results {
			info!("{name} - Size: {size} - Duration: {duration:?} - Factor: {factor}");
		}

		// Check the expected sorted results
		// FlatBuffers is what we use in production for SDK-server communication
		let results: Vec<&str> = results.into_iter().map(|(_, name, _, _)| name).collect();
		assert_eq!(results, vec![COMPRESSED_CBOR, CBOR, FLATBUFFERS_COMPRESSED, FLATBUFFERS,])
	}
}
