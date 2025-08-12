use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicI64;

use async_channel::Receiver;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use revision::revisioned;
use serde::Deserialize;
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
use crate::api::conn::{self, Command, DbResponse, RequestData, Route, Router};
use crate::api::engine::remote::Response;
use crate::api::engine::remote::ws::{Client, PING_INTERVAL};
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::{ExtraFeatures, Result, Surreal};
use crate::core::val::Value as CoreValue;
use crate::engine::IntervalStream;
use crate::engine::remote::Data;
use crate::opt::WaitFor;

pub(crate) const MAX_MESSAGE_SIZE: usize = 64 << 20; // 64 MiB
pub(crate) const MAX_FRAME_SIZE: usize = 16 << 20; // 16 MiB
pub(crate) const WRITE_BUFFER_SIZE: usize = 128000; // tungstenite default
pub(crate) const MAX_WRITE_BUFFER_SIZE: usize = WRITE_BUFFER_SIZE + MAX_MESSAGE_SIZE; // Recommended max according to tungstenite docs
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
	let mut request = (&endpoint.url).into_client_request()?;

	request
		.headers_mut()
		.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(super::REVISION_HEADER));

	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(
		request,
		config,
		NAGLE_ALG,
		maybe_connector,
	)
	.await?;

	#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
	let (socket, _) = tokio_tungstenite::connect_async_with_config(request, config, NAGLE_ALG).await?;

	Ok(socket)
}

impl crate::api::Connection for Client {}
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

			let ws_config = WebSocketConfig {
				max_message_size: Some(MAX_MESSAGE_SIZE),
				max_frame_size: Some(MAX_FRAME_SIZE),
				max_write_buffer_size: MAX_WRITE_BUFFER_SIZE,
				..Default::default()
			};

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
			if response.clone().send(Ok(DbResponse::Other(CoreValue::None))).await.is_err() {
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
			token: _,
		} => {
			state.replay.insert(ReplayMethod::Authenticate, command.clone());
		}
		_ => {}
	}

	let message = {
		let Some(request) = command.into_router_request(Some(id)) else {
			let _ = response.send(Err(Error::BackupsNotSupported.into())).await;
			return HandleResult::Ok;
		};
		trace!("Request {:?}", request);

		// Unwrap because a router request cannot fail to serialize.
		let payload = crate::core::rpc::format::revision::encode(&request).unwrap();

		Message::Binary(payload)
	};

	match state.sink.send(message).await {
		Ok(_) => {
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

async fn router_handle_response(response: Message, state: &mut RouterState) -> HandleResult {
	match Response::try_from(&response) {
		Ok(option) => {
			// We are only interested in responses that are not empty
			if let Some(response) = option {
				trace!("{response:?}");
				match response.id {
					// If `id` is set this is a normal response
					Some(id) => {
						if let Ok(id) = id.coerce_to() {
							match state.pending_requests.remove(&id) {
								Some(pending) => {
									let resp = match DbResponse::from_server_result(response.result)
									{
										Ok(x) => x,
										Err(e) => {
											let _ = pending.response_channel.send(Err(e)).await;
											return HandleResult::Ok;
										}
									};
									// We can only route responses with IDs
									match pending.effect {
										RequestEffect::None => {}
										RequestEffect::Insert => {
											// For insert, we need to flatten single responses in an
											// array
											if let DbResponse::Other(CoreValue::Array(array)) = resp
											{
												if array.len() == 1 {
													let _ = pending
														.response_channel
														.send(Ok(DbResponse::Other(
															array.into_iter().next().unwrap(),
														)))
														.await;
												} else {
													let _ = pending
														.response_channel
														.send(Ok(DbResponse::Other(
															CoreValue::Array(array),
														)))
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
									let _res = pending.response_channel.send(Ok(resp)).await;
								}
								_ => {
									warn!(
										"got response for request with id '{id}', which was not in pending requests"
									)
								}
							}
						}
					}
					// If `id` is not set, this may be a live query notification
					None => {
						match response.result {
							Ok(Data::Live(notification)) => {
								let live_query_id = notification.id;
								// Check if this live query is registered
								if let Some(sender) = state.live_queries.get(&live_query_id) {
									// Send the notification back to the caller or kill live query
									// if the receiver is already dropped
									if sender.send(notification).await.is_err() {
										state.live_queries.remove(&live_query_id);
										let kill = {
											let request = Command::Kill {
												uuid: live_query_id.0,
											}
											.into_router_request(None)
											.unwrap();

											let value = crate::core::rpc::format::revision::encode(
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
						}
					}
				}
			}
		}
		Err(error) => {
			#[revisioned(revision = 1)]
			#[derive(Deserialize)]
			struct ErrorResponse {
				id: Option<CoreValue>,
			}

			// Let's try to find out the ID of the response that failed to deserialise
			if let Message::Binary(binary) = response {
				match crate::core::rpc::format::revision::decode(&binary) {
					Ok(ErrorResponse {
						id,
					}) => {
						// Return an error if an ID was returned
						if let Some(Ok(id)) = id.map(CoreValue::coerce_to) {
							match state.pending_requests.remove(&id) {
								Some(pending) => {
									let _res = pending.response_channel.send(Err(error)).await;
								}
								_ => {
									warn!(
										"got response for request with id '{id}', which was not in pending requests"
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

					let message = crate::core::rpc::format::revision::encode(&request).unwrap();

					if let Err(error) = state.sink.send(Message::Binary(message)).await {
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
					.unwrap();
					trace!("Request {:?}", request);
					let payload = crate::core::rpc::format::revision::encode(&request).unwrap();

					if let Err(error) = state.sink.send(Message::Binary(payload)).await {
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
		let request = Command::Health.into_router_request(None).unwrap();
		let value = crate::core::rpc::format::revision::encode(&request).unwrap();
		Message::Binary(value)
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
		state.live_queries.clear();
		state.pending_requests.clear();

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

					match router_handle_route(response, &mut state).await {
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

impl Response {
	fn try_from(message: &Message) -> Result<Option<Self>> {
		match message {
			Message::Text(text) => {
				trace!("Received an unexpected text message; {text}");
				Ok(None)
			}
			Message::Binary(binary) => crate::core::rpc::format::revision::decode(binary)
				.map(Some)
				.map_err(|x| format!("Failed to deserialize revision payload: {x}"))
				.map_err(crate::api::Error::InvalidResponse)
				.map_err(anyhow::Error::new),
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

#[cfg(test)]
mod tests {
	use std::io::Write;
	use std::time::SystemTime;

	use bincode::Options;
	use flate2::Compression;
	use flate2::write::GzEncoder;
	use rand::{Rng, thread_rng};

	use crate::core::{rpc, val};

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
		// Calculate the reference
		let ref_payload;
		let ref_compressed;
		//
		const BINCODE_REF: &str = "Bincode Vec<i32>";
		const COMPRESSED_BINCODE_REF: &str = "Compressed Bincode Vec<i32>";
		{
			// Bincode Vec<i32>
			let (duration, payload) = timed(&|| {
				let mut payload = Vec::new();
				bincode::options()
					.with_fixint_encoding()
					.serialize_into(&mut payload, &vector)
					.unwrap();
				payload
			});
			ref_payload = payload.len() as f32;
			results.push((payload.len(), BINCODE_REF, duration, 1.0));

			// Compressed bincode
			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			ref_compressed = payload.len() as f32;
			results.push((payload.len(), COMPRESSED_BINCODE_REF, duration, 1.0));
		}
		// Build the Value
		let vector = val::Value::Array(val::Array::from(vector));
		//
		const BINCODE: &str = "Bincode Vec<Value>";
		const COMPRESSED_BINCODE: &str = "Compressed Bincode Vec<Value>";
		{
			// Bincode Vec<i32>
			let (duration, payload) = timed(&|| {
				let mut payload = Vec::new();
				bincode::options()
					.with_varint_encoding()
					.serialize_into(&mut payload, &vector)
					.unwrap();
				payload
			});
			results.push((payload.len(), BINCODE, duration, payload.len() as f32 / ref_payload));

			// Compressed bincode
			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			results.push((
				payload.len(),
				COMPRESSED_BINCODE,
				duration,
				payload.len() as f32 / ref_compressed,
			));
		}
		const UNVERSIONED: &str = "Unversioned Vec<Value>";
		const COMPRESSED_UNVERSIONED: &str = "Compressed Unversioned Vec<Value>";
		{
			// Unversioned
			let (duration, payload) =
				timed(&|| crate::core::rpc::format::bincode::encode(&vector).unwrap());
			results.push((
				payload.len(),
				UNVERSIONED,
				duration,
				payload.len() as f32 / ref_payload,
			));

			// Compressed Versioned
			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			results.push((
				payload.len(),
				COMPRESSED_UNVERSIONED,
				duration,
				payload.len() as f32 / ref_compressed,
			));
		}
		//
		const VERSIONED: &str = "Versioned Vec<Value>";
		const COMPRESSED_VERSIONED: &str = "Compressed Versioned Vec<Value>";
		{
			// Versioned
			let (duration, payload) =
				timed(&|| crate::core::rpc::format::revision::encode(&vector).unwrap());
			results.push((payload.len(), VERSIONED, duration, payload.len() as f32 / ref_payload));

			// Compressed Versioned
			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			results.push((
				payload.len(),
				COMPRESSED_VERSIONED,
				duration,
				payload.len() as f32 / ref_compressed,
			));
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

		// TODO: Figure out what this test was supposed to track.
		//
		//	Note this test changed with the value inversion PR, below is the previous
		// check.
		//
		//	vec![
		//		BINCODE_REF,
		//		COMPRESSED_BINCODE_REF,
		//		COMPRESSED_CBOR,
		//		COMPRESSED_BINCODE,
		//		COMPRESSED_UNVERSIONED,
		//		CBOR,
		//		COMPRESSED_VERSIONED,
		//		BINCODE,
		//		UNVERSIONED,
		//		VERSIONED,
		//	]

		// Check the expected sorted results
		let results: Vec<&str> = results.into_iter().map(|(_, name, _, _)| name).collect();
		assert_eq!(
			results,
			vec![
				BINCODE_REF,
				COMPRESSED_BINCODE_REF,
				COMPRESSED_BINCODE,
				COMPRESSED_UNVERSIONED,
				COMPRESSED_VERSIONED,
				COMPRESSED_CBOR,
				BINCODE,
				UNVERSIONED,
				VERSIONED,
				CBOR,
			]
		)
	}
}
