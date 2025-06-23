use super::{HandleResult, PATH, PendingRequest, ReplayMethod, RequestEffect};
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Surreal;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::conn::{self, Command, Request};
use crate::api::engine::remote::ws::Client;
use crate::api::engine::remote::ws::PING_INTERVAL;
use crate::api::engine::remote::{deserialize_flatbuffers, serialize_flatbuffers};
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::engine::IntervalStream;
use crate::opt::WaitFor;
use async_channel::Receiver;
use futures::SinkExt;
use futures::StreamExt;
use futures::stream::{SplitSink, SplitStream};
use prost::Message as _;
use revision::revisioned;
use serde::Deserialize;
use surrealdb_core::dbs::QueryResultData;
use surrealdb_core::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicI64;
use surrealdb_core::expr::Value as Value;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::Connector;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use trice::Instant;

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
	endpoint: &Endpoint,
) -> HandleResult {
	let Request {
		id,
		command,
	} = request;

	// We probably shouldn't be sending duplicate id requests.
	let entry = state.pending_requests.entry(id.clone());
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
			if response.clone().send(Ok(QueryResultData::new_from_value(Value::None))).await.is_err() {
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
		let request = Request::new_with_id(id, command);
		trace!("Request {:?}", request);
		let payload = serialize_flatbuffers(&request).unwrap();
		Message::Binary(payload.to_vec())
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

async fn router_handle_response(bytes: Vec<u8>, state: &mut RouterState) -> HandleResult {
	let response = match flatbuffers::root::<rpc_fb::Response<'_>>(&bytes) {
		Ok(response) => response,
		Err(error) => {
			trace!("Failed to decode response; {error}");
			return HandleResult::Disconnected;
		}
	};

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
				for command in state.replay.values() {
					let request = Request::new(command.clone());

					let message = serialize_flatbuffers(&request).unwrap();

					if let Err(error) = state.sink.send(Message::Binary(message.to_vec())).await {
						trace!("{error}");
						time::sleep(time::Duration::from_secs(1)).await;
						continue;
					}
				}
				for (key, value) in &state.vars {
					let request = Request::new(Command::Set {
						key: key.as_str().into(),
						value: value.clone(),
					});
					trace!("Request {:?}", request);
					let payload = serialize_flatbuffers(&request).unwrap();
					if let Err(error) = state.sink.send(Message::Binary(payload.to_vec())).await {
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
		let request = Request::new(Command::Health);
		let value = serialize_flatbuffers(&request).unwrap();
		Message::Binary(value.to_vec())
	};

	let (socket_sink, socket_stream) = socket.split();
	let mut state = RouterState::new(socket_sink, socket_stream);

	'router: loop {
		let mut interval = time::interval(PING_INTERVAL);
		// don't bombard the server with pings if we miss some ticks
		interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

		let mut pinger = IntervalStream::new(interval);
		// Turn into a stream instead of calling recv_async
		// The stream seems to be able to keep some state which would otherwise need to be
		// recreated with each next.

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

					match router_handle_route(response, &mut state, &endpoint).await {
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
							match message {
								Message::Binary(bytes) => {
									match router_handle_response(bytes, &mut state).await {
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
								},
								Message::Text(text) => {
									trace!("Received an unexpected text message; {text}");
									continue;
								}
								_ => {
									trace!("Received an unexpected message; {message:?}");
									continue;
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

#[cfg(test)]
mod tests {
	use super::serialize_flatbuffers;
	use bincode::Options;
	use flate2::Compression;
	use flate2::write::GzEncoder;
	use rand::{Rng, thread_rng};
	use std::io::Write;
	use std::time::SystemTime;
	use surrealdb_core::expr::{Array, Value};

	// #[test_log::test]
	// fn large_vector_serialisation_bench() {
	// 	//
	// 	let timed = |func: &dyn Fn() -> Vec<u8>| {
	// 		let start = SystemTime::now();
	// 		let r = func();
	// 		(start.elapsed().unwrap(), r)
	// 	};
	// 	//
	// 	let compress = |v: &Vec<u8>| {
	// 		let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
	// 		encoder.write_all(v).unwrap();
	// 		encoder.finish().unwrap()
	// 	};
	// 	// Generate a random vector
	// 	let vector_size = if cfg!(debug_assertions) {
	// 		200_000 // Debug is slow
	// 	} else {
	// 		2_000_000 // Release is fast
	// 	};
	// 	let mut vector: Vec<i32> = Vec::new();
	// 	let mut rng = thread_rng();
	// 	for _ in 0..vector_size {
	// 		vector.push(rng.r#gen());
	// 	}
	// 	//	Store the results
	// 	let mut results = vec![];
	// 	// Calculate the reference
	// 	let ref_payload;
	// 	let ref_compressed;
	// 	//
	// 	const BINCODE_REF: &str = "Bincode Vec<i32>";
	// 	const COMPRESSED_BINCODE_REF: &str = "Compressed Bincode Vec<i32>";
	// 	{
	// 		// Bincode Vec<i32>
	// 		let (duration, payload) = timed(&|| {
	// 			let mut payload = Vec::new();
	// 			bincode::options()
	// 				.with_fixint_encoding()
	// 				.serialize_into(&mut payload, &vector)
	// 				.unwrap();
	// 			payload
	// 		});
	// 		ref_payload = payload.len() as f32;
	// 		results.push((payload.len(), BINCODE_REF, duration, 1.0));

	// 		// Compressed bincode
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		ref_compressed = payload.len() as f32;
	// 		results.push((payload.len(), COMPRESSED_BINCODE_REF, duration, 1.0));
	// 	}
	// 	// Build the Value
	// 	let vector = Value::Array(Array::from(vector));
	// 	//
	// 	const BINCODE: &str = "Bincode Vec<Value>";
	// 	const COMPRESSED_BINCODE: &str = "Compressed Bincode Vec<Value>";
	// 	{
	// 		// Bincode Vec<i32>
	// 		let (duration, payload) = timed(&|| {
	// 			let mut payload = Vec::new();
	// 			bincode::options()
	// 				.with_varint_encoding()
	// 				.serialize_into(&mut payload, &vector)
	// 				.unwrap();
	// 			payload
	// 		});
	// 		results.push((payload.len(), BINCODE, duration, payload.len() as f32 / ref_payload));

	// 		// Compressed bincode
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_BINCODE,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	const UNVERSIONED: &str = "Unversioned Vec<Value>";
	// 	const COMPRESSED_UNVERSIONED: &str = "Compressed Unversioned Vec<Value>";
	// 	{
	// 		// Unversioned
	// 		let (duration, payload) = timed(&|| serialize_proto(&vector).unwrap());
	// 		results.push((
	// 			payload.len(),
	// 			UNVERSIONED,
	// 			duration,
	// 			payload.len() as f32 / ref_payload,
	// 		));

	// 		// Compressed Versioned
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_UNVERSIONED,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	//
	// 	const VERSIONED: &str = "Versioned Vec<Value>";
	// 	const COMPRESSED_VERSIONED: &str = "Compressed Versioned Vec<Value>";
	// 	{
	// 		// Versioned
	// 		let (duration, payload) = timed(&|| serialize_proto(&vector).unwrap());
	// 		results.push((payload.len(), VERSIONED, duration, payload.len() as f32 / ref_payload));

	// 		// Compressed Versioned
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_VERSIONED,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	//
	// 	const CBOR: &str = "CBor Vec<Value>";
	// 	const COMPRESSED_CBOR: &str = "Compressed CBor Vec<Value>";
	// 	{
	// 		// CBor
	// 		let (duration, payload) = timed(&|| {
	// 			let cbor: Cbor = vector.clone().try_into().unwrap();
	// 			let mut res = Vec::new();
	// 			ciborium::into_writer(&cbor.0, &mut res).unwrap();
	// 			res
	// 		});
	// 		results.push((payload.len(), CBOR, duration, payload.len() as f32 / ref_payload));

	// 		// Compressed Cbor
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_CBOR,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	// Sort the results by ascending size
	// 	results.sort_by(|(a, _, _, _), (b, _, _, _)| a.cmp(b));
	// 	for (size, name, duration, factor) in &results {
	// 		info!("{name} - Size: {size} - Duration: {duration:?} - Factor: {factor}");
	// 	}
	// 	// Check the expected sorted results
	// 	let results: Vec<&str> = results.into_iter().map(|(_, name, _, _)| name).collect();
	// 	assert_eq!(
	// 		results,
	// 		vec![
	// 			BINCODE_REF,
	// 			COMPRESSED_BINCODE_REF,
	// 			COMPRESSED_CBOR,
	// 			COMPRESSED_BINCODE,
	// 			COMPRESSED_UNVERSIONED,
	// 			CBOR,
	// 			COMPRESSED_VERSIONED,
	// 			BINCODE,
	// 			UNVERSIONED,
	// 			VERSIONED,
	// 		]
	// 	)
	// }
}
