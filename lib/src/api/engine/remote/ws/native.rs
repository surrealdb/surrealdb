use super::PATH;
use super::{deserialize, serialize};
use super::{HandleResult, RouterRequest};
use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::engine::remote::ws::Client;
use crate::api::engine::remote::ws::Response;
use crate::api::engine::remote::ws::PING_INTERVAL;
use crate::api::engine::remote::ws::PING_METHOD;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use crate::engine::remote::ws::Data;
use crate::engine::IntervalStream;
use crate::opt::WaitFor;
use crate::sql::Value;
use channel::Receiver;
use futures::stream::{SplitSink, SplitStream};
use futures::SinkExt;
use futures::StreamExt;
use revision::revisioned;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::HashSet;
use std::mem;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::Connector;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use trice::Instant;

pub(crate) const MAX_MESSAGE_SIZE: usize = 64 << 20; // 64 MiB
pub(crate) const MAX_FRAME_SIZE: usize = 16 << 20; // 16 MiB
pub(crate) const WRITE_BUFFER_SIZE: usize = 128000; // tungstenite default
pub(crate) const MAX_WRITE_BUFFER_SIZE: usize = WRITE_BUFFER_SIZE + MAX_MESSAGE_SIZE; // Recommended max according to tungstenite docs
pub(crate) const NAGLE_ALG: bool = false;

type MessageSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type MessageStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type RouterState = super::RouterState<MessageSink, MessageStream, Message>;

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
	endpoint: &Endpoint,
	config: Option<WebSocketConfig>,
	#[allow(unused_variables)] maybe_connector: Option<Connector>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
	let mut request = (&endpoint.url).into_client_request()?;

	if endpoint.supports_revision {
		request
			.headers_mut()
			.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(super::REVISION_HEADER));
	}

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

impl Connection for Client {
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

			let config = WebSocketConfig {
				max_message_size: Some(MAX_MESSAGE_SIZE),
				max_frame_size: Some(MAX_FRAME_SIZE),
				max_write_buffer_size: MAX_WRITE_BUFFER_SIZE,
				..Default::default()
			};

			let socket = connect(&address, Some(config), maybe_connector.clone()).await?;

			let (route_tx, route_rx) = match capacity {
				0 => channel::unbounded(),
				capacity => channel::bounded(capacity),
			};

			tokio::spawn(run_router(address, maybe_connector, capacity, config, socket, route_rx));

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

async fn router_handle_route(
	Route {
		request,
		response,
	}: Route,
	state: &mut RouterState,
	endpoint: &Endpoint,
) -> HandleResult {
	let (id, method, param) = request;
	let params = match param.query {
		Some((query, bindings)) => {
			vec![query.into(), bindings.into()]
		}
		None => param.other,
	};
	match method {
		Method::Set => {
			if let [Value::Strand(key), value] = &params[..2] {
				state.var_stash.insert(id, (key.0.clone(), value.clone()));
			}
		}
		Method::Unset => {
			if let [Value::Strand(key)] = &params[..1] {
				state.vars.swap_remove(&key.0);
			}
		}
		Method::Live => {
			if let Some(sender) = param.notification_sender {
				if let [Value::Uuid(id)] = &params[..1] {
					state.live_queries.insert(id.0, sender);
				}
			}
			if response.clone().send(Ok(DbResponse::Other(Value::None))).await.is_err() {
				trace!("Receiver dropped");
			}
			// There is nothing to send to the server here
		}
		Method::Kill => {
			if let [Value::Uuid(id)] = &params[..1] {
				state.live_queries.remove(id);
			}
		}
		_ => {}
	}
	let method_str = match method {
		Method::Health => PING_METHOD,
		_ => method.as_str(),
	};
	let message = {
		let request = RouterRequest {
			id: Some(Value::from(id)),
			method: method_str.into(),
			params: (!params.is_empty()).then(|| params.into()),
		};

		trace!("Request {:?}", request);
		let payload = serialize(&request, endpoint.supports_revision).unwrap();
		Message::Binary(payload)
	};
	if let Method::Authenticate
	| Method::Invalidate
	| Method::Signin
	| Method::Signup
	| Method::Use = method
	{
		state.replay.insert(method, message.clone());
	}
	match state.sink.send(message).await {
		Ok(_) => {
			state.last_activity = Instant::now();
			match state.routes.entry(id) {
				Entry::Vacant(entry) => {
					// Register query route
					entry.insert((method, response));
				}
				Entry::Occupied(..) => {
					let error = Error::DuplicateRequestId(id);
					if response.send(Err(error.into())).await.is_err() {
						trace!("Receiver dropped");
					}
				}
			}
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
							if let Some((method, sender)) = state.routes.remove(&id) {
								if matches!(method, Method::Set) {
									if let Some((key, value)) = state.var_stash.swap_remove(&id) {
										state.vars.insert(key, value);
									}
								}
								// Send the response back to the caller
								let mut response = response.result;
								if matches!(method, Method::Insert) {
									// For insert, we need to flatten single responses in an array
									if let Ok(Data::Other(Value::Array(value))) = &mut response {
										if let [value] = &mut value.0[..] {
											response = Ok(Data::Other(mem::take(value)));
										}
									}
								}
								let _res = sender.send(DbResponse::from(response)).await;
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
									// Send the notification back to the caller or kill live query if the receiver is already dropped
									if sender.send(notification).await.is_err() {
										state.live_queries.remove(&live_query_id);
										let kill = {
											let request = RouterRequest {
												id: None,
												method: Method::Kill.as_str().into(),
												params: Some(
													vec![Value::from(live_query_id)].into(),
												),
											};
											let value =
												serialize(&request, endpoint.supports_revision)
													.unwrap();
											Message::Binary(value)
										};
										if let Err(error) = state.sink.send(kill).await {
											trace!("failed to send kill query to the server; {error:?}");
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
						if let Some((_method, sender)) = state.routes.remove(&id) {
							let _res = sender.send(Err(error)).await;
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
				for (_, message) in &state.replay {
					if let Err(error) = state.sink.send(message.clone()).await {
						trace!("{error}");
						time::sleep(time::Duration::from_secs(1)).await;
						continue;
					}
				}
				for (key, value) in &state.vars {
					let request = RouterRequest {
						id: None,
						method: Method::Set.as_str().into(),
						params: Some(vec![key.as_str().into(), value.clone()].into()),
					};
					trace!("Request {:?}", request);
					let payload = serialize(&request, endpoint.supports_revision).unwrap();
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
		let request = RouterRequest {
			id: None,
			method: PING_METHOD.into(),
			params: None,
		};
		let value = serialize(&request, endpoint.supports_revision).unwrap();
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
		// The stream seems to be able to keep some state which would otherwise need to be
		// recreated with each next.

		state.last_activity = Instant::now();
		state.live_queries.clear();
		state.routes.clear();

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
							match router_handle_response(message, &mut state, &endpoint).await {
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
	use super::serialize;
	use bincode::Options;
	use flate2::write::GzEncoder;
	use flate2::Compression;
	use rand::{thread_rng, Rng};
	use std::io::Write;
	use std::time::SystemTime;
	use surrealdb_core::rpc::format::cbor::Cbor;
	use surrealdb_core::sql::{Array, Value};

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
			vector.push(rng.gen());
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
		let vector = Value::Array(Array::from(vector));
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
			let (duration, payload) = timed(&|| serialize(&vector, false).unwrap());
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
			let (duration, payload) = timed(&|| serialize(&vector, true).unwrap());
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
				let cbor: Cbor = vector.clone().try_into().unwrap();
				let mut res = Vec::new();
				ciborium::into_writer(&cbor.0, &mut res).unwrap();
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
		let results: Vec<&str> = results.into_iter().map(|(_, name, _, _)| name).collect();
		assert_eq!(
			results,
			vec![
				BINCODE_REF,
				COMPRESSED_BINCODE_REF,
				COMPRESSED_CBOR,
				COMPRESSED_BINCODE,
				COMPRESSED_UNVERSIONED,
				CBOR,
				COMPRESSED_VERSIONED,
				BINCODE,
				UNVERSIONED,
				VERSIONED,
			]
		)
	}
}
