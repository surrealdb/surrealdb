use std::collections::HashSet;
use std::sync::Arc;

use async_channel::Receiver;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, watch};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

use super::{
	HandleResult, PATH, PING_INTERVAL, SessionState, WsMessage, create_ping_message,
	handle_response, handle_route, handle_session_clone, handle_session_drop,
	handle_session_initial, replay_session, reset_sessions,
};
use crate::conn::{self, Route, Router};
use crate::engine::{IntervalStream, SessionError};
use crate::Error;
use crate::method::BoxFuture;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::opt::Tls;
use crate::opt::{Endpoint, WaitFor};
use crate::types::HashMap;
use crate::{ExtraFeatures, SessionClone, SessionId, Surreal};

pub(crate) const NAGLE_ALG: bool = false;

type MessageSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type MessageStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type Sessions = HashMap<Uuid, Result<Arc<SessionState>, SessionError>>;

// ============================================================================
// Platform Implementation
// ============================================================================

impl WsMessage for Message {
	fn binary(payload: Vec<u8>) -> Self {
		Message::Binary(payload.into())
	}

	fn as_binary(&self) -> Option<&[u8]> {
		match self {
			Message::Binary(data) => Some(data),
			_ => None,
		}
	}

	fn should_process(&self) -> bool {
		matches!(self, Message::Binary(_))
	}

	fn log_description(&self) -> &'static str {
		match self {
			Message::Text(_) => "text message",
			Message::Binary(_) => "binary message",
			Message::Ping(_) => "ping",
			Message::Pong(_) => "pong",
			Message::Frame(_) => "raw frame",
			Message::Close(_) => "close message",
		}
	}
}

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
) -> crate::Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
	let mut request =
		(&endpoint.url).into_client_request().map_err(|err| Error::internal(format!("Invalid URL: {}", err)))?;

	request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("flatbuffers"));

	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(
		request,
		config,
		NAGLE_ALG,
		maybe_connector,
	)
	.await
	.map_err(|err| Error::internal(format!("WebSocket error: {}", err)))?;

	#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
	let (socket, _) = tokio_tungstenite::connect_async_with_config(request, config, NAGLE_ALG)
		.await
		.map_err(|err| Error::internal(format!("WebSocket error: {}", err)))?;

	Ok(socket)
}

impl crate::Connection for super::Client {}
impl conn::Sealed for super::Client {
	#[allow(private_interfaces)]
	fn connect(
		mut address: Endpoint,
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, crate::Result<Surreal<Self>>> {
		Box::pin(async move {
			address.url = address
				.url
				.join(PATH)
				.map_err(|e| Error::internal(e.to_string()))?;
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
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);

			tokio::spawn(run_router(
				address,
				maybe_connector,
				ws_config,
				socket,
				route_rx,
				session_clone.receiver.clone(),
			));

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features,
				config,
				sender: route_tx,
			};

			Ok((router, waiter, session_clone).into())
		})
	}
}

// ============================================================================
// Router State
// ============================================================================

/// Router state for native WebSocket connections.
struct RouterState {
	sessions: Sessions,
	sink: RwLock<MessageSink>,
	stream: RwLock<MessageStream>,
}

impl RouterState {
	fn new(sink: MessageSink, stream: MessageStream) -> Self {
		RouterState {
			sessions: HashMap::new(),
			sink: RwLock::new(sink),
			stream: RwLock::new(stream),
		}
	}

	async fn update_connection(&self, sink: MessageSink, stream: MessageStream) {
		*self.sink.write().await = sink;
		*self.stream.write().await = stream;
	}
}

// ============================================================================
// Router
// ============================================================================

async fn router_reconnect(
	maybe_connector: &Option<Connector>,
	config: &WebSocketConfig,
	state: &RouterState,
	endpoint: &Endpoint,
) {
	loop {
		trace!("Reconnecting...");
		match connect(endpoint, Some(*config), maybe_connector.clone()).await {
			Ok(s) => {
				let (new_sink, new_stream) = s.split();
				state.update_connection(new_sink, new_stream).await;
				// Replay state for ALL sessions
				for (session_id, session_result) in state.sessions.to_vec() {
					if let Ok(session_state) = session_result {
						replay_session::<Message, _, _>(session_id, &session_state, &state.sink)
							.await
							.ok();
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
	config: WebSocketConfig,
	socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	let ping: Message = create_ping_message();

	let (socket_sink, socket_stream) = socket.split();
	let state = Arc::new(RouterState::new(socket_sink, socket_stream));

	'router: loop {
		let mut interval = time::interval(PING_INTERVAL);
		interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
		let mut pinger = IntervalStream::new(interval);

		reset_sessions(&state.sessions).await;

		loop {
			tokio::select! {
				biased;

				session = session_rx.recv() => {
					let Ok(session_id) = session else {
						break 'router
					};
					match session_id {
						SessionId::Initial(session_id) => {
							handle_session_initial::<Message, _, _>(
								session_id, &state.sessions, &state.sink
							).await;
						}
						SessionId::Clone { old, new } => {
							handle_session_clone::<Message, _, _>(
								old, new, &state.sessions, &state.sink
							).await;
						}
						SessionId::Drop(session_id) => {
							handle_session_drop::<Message, _, _>(
								session_id, &state.sessions, &state.sink
							).await;
						}
					}
				}
				route = route_rx.recv() => {
					let Ok(route) = route else {
						match state.sink.write().await.send(Message::Close(None)).await {
							Ok(..) => trace!("Connection closed successfully"),
							Err(error) => warn!("Failed to close database connection; {error}")
						}
						break 'router;
					};

					match handle_route::<Message, _, _>(
						route, config.max_message_size, &state.sessions, &state.sink
					).await {
						HandleResult::Ok => {}
						HandleResult::Disconnected => {
							router_reconnect(&maybe_connector, &config, &state, &endpoint).await;
							continue 'router;
						}
					}
				}
				result = async { state.stream.write().await.next().await } => {
					let Some(result) = result else {
						router_reconnect(&maybe_connector, &config, &state, &endpoint).await;
						continue 'router;
					};

					match result {
						Ok(message) => {
							match handle_response::<Message, _, _>(
								&message, &state.sessions, &state.sink
							).await {
								HandleResult::Ok => continue,
								HandleResult::Disconnected => {
									router_reconnect(&maybe_connector, &config, &state, &endpoint).await;
									continue 'router;
								}
							}
						}
						Err(error) => {
							reset_sessions(&state.sessions).await;
							match error {
								WsError::ConnectionClosed => {
									trace!("Connection successfully closed on the server");
								}
								error => {
									trace!("{error}");
								}
							}
							router_reconnect(&maybe_connector, &config, &state, &endpoint).await;
							continue 'router;
						}
					}
				}
				_ = pinger.next() => {
					trace!("Pinging the server");
					if let Err(error) = state.sink.write().await.send(ping.clone()).await {
						trace!("failed to ping the server; {error:?}");
						router_reconnect(&maybe_connector, &config, &state, &endpoint).await;
						continue 'router;
					}
				}
			}
		}
	}
}

// ============================================================================
// Tests
// ============================================================================

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
		let timed = |func: &dyn Fn() -> Vec<u8>| {
			let start = SystemTime::now();
			let r = func();
			(start.elapsed().unwrap(), r)
		};
		let compress = |v: &Vec<u8>| {
			let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
			encoder.write_all(v).unwrap();
			encoder.finish().unwrap()
		};
		let vector_size = if cfg!(debug_assertions) {
			200_000
		} else {
			2_000_000
		};
		let mut vector: Vec<i32> = Vec::new();
		let mut rng = thread_rng();
		for _ in 0..vector_size {
			vector.push(rng.r#gen());
		}
		let mut results = vec![];
		let ref_payload;
		let ref_compressed;

		let vector = Value::Array(Array::from(vector));

		const FLATBUFFERS: &str = "Flatbuffers Vec<Value>";
		const FLATBUFFERS_COMPRESSED: &str = "Flatbuffers Compressed Vec<Value>";
		{
			let (duration, payload) =
				timed(&|| surrealdb_core::rpc::format::flatbuffers::encode(&vector).unwrap());
			ref_payload = payload.len() as f32;
			results.push((payload.len(), FLATBUFFERS, duration, 1.0));

			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			ref_compressed = payload.len() as f32;
			results.push((payload.len(), FLATBUFFERS_COMPRESSED, duration, 1.0));
		}

		const CBOR: &str = "CBor Vec<Value>";
		const CBOR_COMPRESSED: &str = "Compressed CBor Vec<Value>";
		{
			let (duration, payload) = timed(&|| {
				let cbor = rpc::format::cbor::encode(vector.clone()).unwrap();
				let mut res = Vec::new();
				ciborium::into_writer(&cbor, &mut res).unwrap();
				res
			});
			results.push((payload.len(), CBOR, duration, payload.len() as f32 / ref_payload));

			let (compression_duration, payload) = timed(&|| compress(&payload));
			let duration = duration + compression_duration;
			results.push((
				payload.len(),
				CBOR_COMPRESSED,
				duration,
				payload.len() as f32 / ref_compressed,
			));
		}
		results.sort_by(|(a, _, _, _), (b, _, _, _)| a.cmp(b));
		for (size, name, duration, factor) in &results {
			info!("{name} - Size: {size} - Duration: {duration:?} - Factor: {factor}");
		}

		let results: Vec<&str> = results.into_iter().map(|(_, name, _, _)| name).collect();
		assert_eq!(results, vec![CBOR_COMPRESSED, CBOR, FLATBUFFERS_COMPRESSED, FLATBUFFERS])
	}
}
