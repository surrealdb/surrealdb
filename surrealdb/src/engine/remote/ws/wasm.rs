use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use futures::stream::{SplitSink, SplitStream};
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::sync::{RwLock, watch};
use tokio_tungstenite_wasm::{Message, WebSocketStream, connect_with_protocols};
use wasm_bindgen_futures::spawn_local;
use wasmtimer::tokio as time;
use wasmtimer::tokio::MissedTickBehavior;

use super::{
	HandleResult, PATH, PING_INTERVAL, SessionState, WsMessage, create_ping_message,
	handle_response, handle_route, handle_session_clone, handle_session_drop,
	handle_session_initial, replay_session, reset_sessions,
};
use crate::conn::{self, Route, Router};
use crate::engine::{IntervalStream, SessionError};
use crate::method::BoxFuture;
use crate::opt::{Endpoint, WaitFor};
use crate::types::HashMap;
use crate::{Error, ExtraFeatures, Result, SessionClone, SessionId, Surreal};

type MessageStream = SplitStream<WebSocketStream>;
type MessageSink = SplitSink<WebSocketStream, Message>;
type Sessions = HashMap<uuid::Uuid, std::result::Result<Arc<SessionState>, SessionError>>;

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
			Message::Close(_) => "close message",
		}
	}
}

impl crate::Connection for super::Client {}
impl conn::Sealed for super::Client {
	#[allow(private_interfaces)]
	fn connect(
		mut address: Endpoint,
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			address.url = address.url.join(PATH)?;

			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded(1);
			let config = address.config.clone();
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);

			spawn_local(run_router(address, conn_tx, route_rx, session_clone.receiver.clone()));

			conn_rx.recv().await??;

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

/// Router state for WASM WebSocket connections.
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

async fn router_reconnect(state: &RouterState, endpoint: &Endpoint) {
	loop {
		trace!("Reconnecting...");

		// Build WebSocket URL with flatbuffers protocol negotiation
		let ws_url = endpoint.url.as_str();

		match connect_with_protocols(ws_url, &["flatbuffers"]).await {
			Ok(socket) => {
				let (new_sink, new_stream) = socket.split();
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
				time::sleep(Duration::from_secs(1)).await;
			}
		}
	}
}

pub(crate) async fn run_router(
	endpoint: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
	session_rx: Receiver<SessionId>,
) {
	// Connect to the WebSocket server with flatbuffers protocol negotiation
	let ws_url = endpoint.url.as_str();
	let socket = match connect_with_protocols(ws_url, &["flatbuffers"]).await {
		Ok(socket) => socket,
		Err(error) => {
			conn_tx.send(Err(Error::internal(format!("WebSocket error: {}", error)))).await.ok();
			return;
		}
	};

	let ping: Message = create_ping_message();

	// Signal successful connection
	if conn_tx.send(Ok(())).await.is_err() {
		return;
	}

	let (socket_sink, socket_stream) = socket.split();
	let state = Arc::new(RouterState::new(socket_sink, socket_stream));

	'router: loop {
		let mut interval = time::interval(PING_INTERVAL);
		interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
		let mut pinger = IntervalStream::new(interval);

		reset_sessions(&state.sessions).await;

		loop {
			futures::select! {
				session = session_rx.recv().fuse() => {
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
				route = route_rx.recv().fuse() => {
					let Ok(route) = route else {
						if let Err(error) = state.sink.write().await.close().await {
							warn!("Failed to close database connection; {error}")
						}
						break 'router;
					};

					match handle_route::<Message, _, _>(
						route, None, &state.sessions, &state.sink
					).await {
						HandleResult::Ok => {}
						HandleResult::Disconnected => {
							router_reconnect(&state, &endpoint).await;
							continue 'router;
						}
					}
				}
				result = async { state.stream.write().await.next().await }.fuse() => {
					match result {
						Some(Ok(message)) => {
							match handle_response::<Message, _, _>(
								&message, &state.sessions, &state.sink
							).await {
								HandleResult::Ok => continue,
								HandleResult::Disconnected => {
									router_reconnect(&state, &endpoint).await;
									continue 'router;
								}
							}
						}
						Some(Err(error)) => {
							trace!("WebSocket error: {error}");
							router_reconnect(&state, &endpoint).await;
							continue 'router;
						}
						None => {
							trace!("WebSocket stream ended");
							router_reconnect(&state, &endpoint).await;
							continue 'router;
						}
					}
				}
				_ = pinger.next().fuse() => {
					trace!("Pinging the server");
					if let Err(error) = state.sink.write().await.send(ping.clone()).await {
						trace!("failed to ping the server; {error:?}");
						router_reconnect(&state, &endpoint).await;
						continue 'router;
					}
				}
			}
		}
	}
}
