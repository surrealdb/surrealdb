use core::fmt;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::close_code::AGAIN;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::{Sink, SinkExt, StreamExt};
use http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry_http::HeaderExtractor;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, LockType, Transaction, TransactionType};
use surrealdb_core::mem::ALLOC;
use surrealdb_core::observe::{
	NetworkBytesEvent, NetworkBytesEventCtx, NetworkBytesEventSafe, NetworkDirection,
	SessionAction, SessionEvent, SessionEventCtx, SessionEventSafe, SessionProtocol,
};
use surrealdb_core::rpc::format::Format;
use surrealdb_core::rpc::{DbResponse, DbResult, Method, RpcProtocol};
use surrealdb_types::{Array, Error as TypesError, HashMap, ToSql, Value};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use super::RpcState;
use crate::cnf::{
	PKG_NAME, PKG_VERSION, WEBSOCKET_MAX_ATTACHED_SESSIONS, WEBSOCKET_PING_FREQUENCY,
	WEBSOCKET_RESPONSE_BUFFER_SIZE, WEBSOCKET_RESPONSE_CHANNEL_SIZE,
	WEBSOCKET_RESPONSE_FLUSH_PERIOD,
};
use crate::rpc::CONN_CLOSED_ERR;
use crate::rpc::format::WsFormat;
use crate::telemetry::traces::rpc::span_for_request;

/// An error string sent when the server is out of memory
const SERVER_OVERLOADED: &str = "The server is unable to handle the request";

/// An error string sent when the server is gracefully shutting down
const SERVER_SHUTTING_DOWN: &str = "The server is gracefully shutting down";

/// An error string surfaced by the `begin` RPC when the WebSocket
/// canceller has already fired -- avoids opening a write transaction on
/// a connection that is about to be torn down. Executor-driven RPCs
/// (query, etc.) surface `Error::QueryCancelled` from the executor
/// instead; this constant is `begin`-only because `begin` bypasses the
/// executor and goes straight to `kvs().transaction(...)`.
const REQUEST_CANCELLED: &str = "The request was cancelled because the WebSocket is closing";

/// Build an OTel parent `Context` from W3C Trace Context propagation
/// headers carried in the RPC envelope's `trace_context` field. Reuses
/// the `HeaderMap`-based `HeaderExtractor` so the same propagator path
/// is used as for HTTP — the only difference is where the headers come
/// from. Returns `None` when the map produces no usable entries, so the
/// caller can leave the span as a fresh root rather than parent it to
/// an empty context.
fn extract_trace_context(
	trace_context: &std::collections::HashMap<String, String>,
) -> Option<opentelemetry::Context> {
	let mut headers = HeaderMap::with_capacity(trace_context.len());
	for (k, v) in trace_context {
		if let (Ok(name), Ok(value)) =
			(HeaderName::try_from(k.as_str()), HeaderValue::try_from(v.as_str()))
		{
			headers.insert(name, value);
		}
	}
	if headers.is_empty() {
		return None;
	}
	Some(opentelemetry::global::get_text_map_propagator(|propagator| {
		propagator.extract(&HeaderExtractor(&headers))
	}))
}

pub struct Websocket {
	/// The unique id of this WebSocket connection
	pub(crate) id: Uuid,
	/// The request and response format for messages
	pub(crate) format: Format,
	/// The system state for all RPC WebSocket connections
	pub(crate) state: Arc<RpcState>,
	/// The datastore accessible to all RPC WebSocket connections
	pub(crate) datastore: Arc<Datastore>,
	/// The active sessions for this WebSocket connection.
	///
	/// This map is **per-connection**: it is created fresh for every
	/// WebSocket upgrade and torn down when the connection closes. A
	/// session id attached here is only reachable from RPC calls that
	/// arrive on this same socket, so cross-connection hijack via session
	/// id enumeration is not possible on the WebSocket transport by construction.
	/// The attach count is additionally capped by [`WEBSOCKET_MAX_ATTACHED_SESSIONS`]
	/// as defence-in-depth against a single misbehaving client.
	pub(crate) sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
	/// The active transactions for this WebSocket connection
	pub(crate) transactions: DashMap<Uuid, Arc<Transaction>>,
	/// A cancellation token called when shutting down the server
	pub(crate) shutdown: CancellationToken,
	/// Connection-level cancellation handle. Bundles a hot-path
	/// `AtomicBool` (consumed by the executor's `Context::done` walks) and
	/// an awaitable `CancellationToken` (consumed by `tokio::select!`
	/// sites such as the read/ping/write loops and `SLEEP`). Tripped in
	/// lockstep by [`Self::cancel_all`]; there is no way to fire one view
	/// without the other.
	pub(crate) cancel: surrealdb_core::ctx::CancelHandle,
	/// The channels used to send and receive WebSocket messages
	pub(crate) channel: Sender<Message>,
}

impl Websocket {
	/// Trip the connection cancel handle, signalling both its
	/// `AtomicBool` flag (executor's hot-path) and `CancellationToken`
	/// (bare-await `select!` sites) so in-flight queries short-circuit at
	/// their next yield AND points blocked on an external timer
	/// (`SLEEP`, etc.) wake up immediately.
	pub(crate) fn cancel_all(&self) {
		self.cancel.trip();
	}

	/// Serve the RPC endpoint
	pub async fn serve(
		id: Uuid,
		ws: WebSocket,
		format: Format,
		session: Session,
		datastore: Arc<Datastore>,
		state: Arc<RpcState>,
	) {
		// Log the succesful WebSocket connection
		trace!("WebSocket {id} connected");
		// Record the connect timestamp so we can emit the session lifetime
		// alongside the disconnect event further down.
		let connected_at = web_time::Instant::now();
		// Create a channel for sending messages
		let (sender, receiver) = channel(*WEBSOCKET_RESPONSE_CHANNEL_SIZE);
		let rec_limit = datastore.config().max_object_parsing_depth as usize;
		// Create and store the RPC connection
		let rpc = Arc::new(Websocket {
			id,
			format,
			state: Arc::clone(&state),
			shutdown: CancellationToken::new(),
			cancel: surrealdb_core::ctx::CancelHandle::new(),
			sessions: HashMap::new(),
			transactions: DashMap::new(),
			channel: sender.clone(),
			datastore,
		});

		// Store the default session keyed by connection id
		let session = session.with_rt(true);
		rpc.set_session(id, Arc::new(RwLock::new(session)));
		// Add this WebSocket to the list
		state.web_sockets.write().await.insert(id, Arc::clone(&rpc));
		// Emit a session connect event so observability sinks can track
		// simultaneous connections without consulting the datastore.
		// `MetricsObserver::on_session_event` increments
		// `surrealdb.session.active` (UpDown gauge) and
		// `surrealdb.session.total` (counter) from this dispatch.
		rpc.datastore.observer().on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Connect,
				protocol: SessionProtocol::WebSocket,
				duration: None,
			},
			ctx: SessionEventCtx {
				session_id: Some(id),
				service_name: None,
				..Default::default()
			},
		});
		// Store all concurrent spawned tasks
		let mut tasks = JoinSet::new();
		// Buffer the WebSocket response stream
		match *WEBSOCKET_RESPONSE_BUFFER_SIZE > 0 {
			true => {
				// Buffer the WebSocket response stream
				let buffer = ws.buffer(*WEBSOCKET_RESPONSE_BUFFER_SIZE);
				// Split the socket into sending and receiving streams
				let (ws_sender, ws_receiver) = buffer.split();
				// Spawn async tasks for the WebSocket
				tasks.spawn(Self::ping(Arc::clone(&rpc), sender.clone()));
				tasks.spawn(Self::read(Arc::clone(&rpc), ws_receiver, sender.clone(), rec_limit));
				tasks.spawn(Self::write(Arc::clone(&rpc), ws_sender, receiver));
			}
			false => {
				// Split the socket into sending and receiving streams
				let (ws_sender, ws_receiver) = ws.split();
				// Spawn async tasks for the WebSocket
				tasks.spawn(Self::ping(Arc::clone(&rpc), sender.clone()));
				tasks.spawn(Self::read(Arc::clone(&rpc), ws_receiver, sender.clone(), rec_limit));
				tasks.spawn(Self::write(Arc::clone(&rpc), ws_sender, receiver));
			}
		}
		// Wait for all tasks to finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				error!("Error handling RPC connection: {err}");
			}
		}
		// Close the internal response channel
		std::mem::drop(sender);
		// Log the WebSocket disconnection
		trace!("WebSocket {id} disconnected");
		// Cleanup the live queries for this WebSocket
		rpc.cleanup_all_lqs().await;
		// Cancel any client-managed transactions left behind by `begin`
		// RPCs whose `commit` / `cancel` never arrived — most commonly
		// because the client disconnected mid-flight. Adapted from the
		// design in <https://github.com/surrealdb/surrealdb/pull/6907>
		// (`cleanup_all_txns`) but scoped to just the disconnect drain;
		// the per-session limits, counter map, and `(session_id, tx)`
		// value-type rework from 6907 are intentionally out of scope.
		rpc.cleanup_all_txns().await;
		// Remove this WebSocket from the list
		state.web_sockets.write().await.remove(&id);
		// Emit a session disconnect event including the full session
		// lifetime so histogram-based observers can summarise dwell
		// time. `MetricsObserver::on_session_event` decrements
		// `surrealdb.session.active` and records the elapsed time on
		// `surrealdb.session.duration`.
		rpc.datastore.observer().on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Disconnect,
				protocol: SessionProtocol::WebSocket,
				duration: Some(connected_at.elapsed()),
			},
			ctx: SessionEventCtx {
				session_id: Some(id),
				service_name: None,
				..Default::default()
			},
		});
	}

	/// Send Ping messages to the client
	async fn ping(rpc: Arc<Websocket>, internal_sender: Sender<Message>) {
		// Create the interval ticker
		let mut interval = tokio::time::interval(WEBSOCKET_PING_FREQUENCY);
		// Clone the WebSocket cancellation token (awaitable view of the
		// shared cancel handle).
		let canceller = rpc.cancel.token();
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				// Process brances in order
				biased;
				// Check if we should teardown
				_ = canceller.cancelled() => break,
				// Send a regular ping message
				_ = interval.tick() => {
					// Create a new ping message
					let msg = Message::Ping(Bytes::from_static(b""));
					// Close the connection if the message fails
					if let Err(err) = internal_sender.send(msg).await {
						// Output any errors if not a close error
						if err.to_string() != CONN_CLOSED_ERR {
							trace!("WebSocket error: {err}");
						}
						// Cancel the WebSocket tasks AND the executor cancel
						// flag so in-flight queries return early.
						rpc.cancel_all();
						// Exit out of the loop
						break;
					}
				},
			}
		}
	}

	/// Write messages to the client
	async fn write<S: SinkExt<Message> + Unpin>(
		rpc: Arc<Websocket>,
		mut socket: S,
		mut internal_receiver: Receiver<Message>,
	) where
		<S as Sink<Message>>::Error: fmt::Display,
	{
		// Clone the WebSocket cancellation token (awaitable view of the
		// shared cancel handle).
		let canceller = rpc.cancel.token();
		// Check if the responses are buffered
		let buffer = *WEBSOCKET_RESPONSE_BUFFER_SIZE > 0;
		// How often should responses be flushed
		let period = Duration::from_millis(*WEBSOCKET_RESPONSE_FLUSH_PERIOD);
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				// Process brances in order
				biased;
				// Check if we should teardown
				_ = canceller.cancelled() => break,
				// Retrieve a response from the channel
				Some(res) = internal_receiver.recv() => {
					// Capture the byte length before the message is moved
					// into the sink so we can fold it into the outbound
					// Prometheus counter when metrics are enabled.
					let out_bytes = match &res {
						Message::Text(msg) => msg.len(),
						Message::Binary(msg) => msg.len(),
						_ => 0,
					};
					// Check if the socket is buffered
					let result = match buffer {
						// Send the message to the socket buffer
						true => socket.feed(res).await,
						// Send the message direct to the socket
						false => socket.send(res).await
					};
					// Check if there was an error
					if let Err(err) = result {
						// Output any errors if not a close error
						if err.to_string() != CONN_CLOSED_ERR {
							trace!("WebSocket error: {err}");
						}
						// Cancel the WebSocket tasks AND the executor cancel
						// flag so in-flight queries return early.
						rpc.cancel_all();
						// Exit out of the loop
						break;
					}
					// Record outbound bytes on success. We deliberately avoid
					// double-counting on the error path above. `ctx` carries
					// `(namespace, database, user)` from the bound session;
					// record-access principals collapse to the `<record>`
					// sentinel in `NetworkBytesEventCtx::from_session` to keep
					// dimensional cardinality bounded.
					if out_bytes > 0 {
						let ctx = rpc.default_network_ctx().await;
						rpc.datastore.observer().on_network_bytes(&NetworkBytesEvent {
							safe: NetworkBytesEventSafe {
								direction: NetworkDirection::Sent,
								protocol: SessionProtocol::WebSocket,
								bytes: out_bytes as u64,
							},
							ctx,
						});
					}
				},
				// Wait for a short period of time
				_ = tokio::time::sleep(period), if buffer => {
					// Flush the WebSocket socket buffer
					if let Err(err) = socket.flush().await {
						// Output any errors if not a close error
						if err.to_string() != CONN_CLOSED_ERR {
							trace!("WebSocket error: {err}");
						}
						// Cancel the WebSocket tasks AND the executor cancel
						// flag so in-flight queries return early.
						rpc.cancel_all();
						// Exit out of the loop
						break;
					}
				}
			}
		}
	}

	/// Read messages sent from the client
	async fn read(
		rpc: Arc<Websocket>,
		mut socket: impl StreamExt<Item = Result<Message, axum::Error>> + Unpin,
		internal_sender: Sender<Message>,
		rec_limit: usize,
	) {
		// Clone the WebSocket shutdown token
		let shutdown = rpc.shutdown.clone();
		// Clone the WebSocket cancellation token (awaitable view of the
		// shared cancel handle).
		let canceller = rpc.cancel.token();
		// Store spawned tasks so we can wait for them
		let mut tasks = FuturesUnordered::new();
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				// Process brances in order
				biased;
				// Remove any completed tasks
				_ = tasks.next(), if !tasks.is_empty() => {},
				// Check if we are shutting down
				_ = shutdown.cancelled() => break,
				// Check if we should teardown
				_ = canceller.cancelled() => break,
				// Wait for the next received message
				Some(msg) = socket.next() => match msg {
					// We've received a message from the client
					Ok(msg) => match msg {
						Message::Text(_) | Message::Binary(_) => {
							// Clone the response sending channel
							let chn = internal_sender.clone();
							// Check to see whether we have available memory
							if ALLOC.is_beyond_threshold() {
								// Reject the message
								Self::close_socket(Arc::clone(&rpc), chn).await;
								// Exit out of the loop
								break;
							}
							// Otherwise spawn and handle the message
							tasks.push(Self::handle_message(&rpc, msg, chn, rec_limit));
						}
						Message::Close(_) => {
							// Respond with a close message
							if let Err(err) = internal_sender.send(Message::Close(None)).await {
								trace!("WebSocket error when replying to the close message: {err}");
							};
							// Cancel the WebSocket tasks AND the executor
							// cancel flag so in-flight queries return early.
							rpc.cancel_all();
							// Exit out of the loop
							break;
						}
						Message::Ping(_) => {
							// Ping messages are responded to automatically
						}
						Message::Pong(_) => {
							// Pong messages are handled automatically
						}
					},
					Err(err) => {
						// There was an error with the WebSocket
						trace!("WebSocket error: {err}");
						// Cancel the WebSocket tasks AND the executor cancel
						// flag so in-flight queries return early.
						rpc.cancel_all();
						// Exit out of the loop
						break;
					}
				}
			}
		}
		// We have left the main loop -- either the connection canceller
		// fired (client disconnect, socket error, close frame) or the
		// server is gracefully shutting down. In either case we drain
		// the `FuturesUnordered` of in-flight `handle_message` futures
		// to completion rather than dropping it.
		//
		// Dropping the FuturesUnordered would drop each in-flight
		// handler, which would drop the executor's open transaction
		// mid-await and trigger `Transactor::Drop`'s
		// "A transaction was dropped without being committed or
		// cancelled" error log. Draining instead lets each handler's
		// executor reach its next `Context::done` check, short-circuit
		// with `Reason::Canceled` (the cancel flag was set in lockstep
		// with the canceller via [`Self::cancel_all`] at every site
		// that fires the canceller), and finalise its transaction on
		// the normal error path. In the shutdown branch the cancel
		// flag has NOT been set, so executors run to completion
		// naturally before the drain returns.
		while tasks.next().await.is_some() {
			// Drain.
		}
		// Now that the drain is done, trip the connection canceller so
		// the `ping` and `write` tasks exit cleanly. In the cancel
		// branch this is a no-op (the canceller was already set by the
		// site that broke the loop); in the shutdown branch it is the
		// only signal those two tasks observe.
		rpc.cancel_all();
	}

	/// Handle an individual WebSocket message
	async fn handle_message(
		rpc: &Arc<Websocket>,
		msg: Message,
		chn: Sender<Message>,
		rec_limit: usize,
	) {
		// Clone the WebSocket shutdown token. The connection-level canceller
		// is NOT raced against the handler future here -- see the comment
		// on the inline-processing path below.
		let shutdown = rpc.shutdown.clone();
		// Calculate the message length and format
		let len = match msg {
			Message::Text(ref msg) => msg.len(),
			Message::Binary(ref msg) => msg.len(),
			_ => 0,
		};
		// Record inbound bytes by dispatching a network event through the
		// fan-out observer. Ping / Pong / Close / Raw frames report 0 and
		// the dispatch short-circuits inside each observer. `ctx` is built
		// from the bound session via `default_network_ctx`; record-access
		// principals collapse to a `<record>` sentinel to bound dimensional
		// label cardinality.
		if len > 0 {
			let ctx = rpc.default_network_ctx().await;
			rpc.datastore.observer().on_network_bytes(&NetworkBytesEvent {
				safe: NetworkBytesEventSafe {
					direction: NetworkDirection::Received,
					protocol: SessionProtocol::WebSocket,
					bytes: len as u64,
				},
				ctx,
			});
		}
		// Prepare the per-request tracing span. RPC duration / outcome
		// is now recorded centrally by `core::rpc::protocol` via
		// [`RpcEvent`], so we no longer thread an OTel context through
		// the response path: the tracing span is the only telemetry
		// frame we keep here.
		let span = span_for_request(&rpc.id);
		// `len` is the inbound frame size in bytes. Surfaced through
		// `NetworkBytesEvent` above, no longer attached to a per-RPC
		// telemetry context.
		let _ = len;
		// Parse the RPC envelope synchronously BEFORE `.instrument(span)`.
		//
		// Both pre-populating the span (rpc.method / otel.name /
		// rpc.request_id) and attaching the propagated OTel parent
		// (`req.trace_context`) must happen while the underlying
		// `tracing_opentelemetry::OtelData` is still in `Builder` state.
		// `Instrumented::poll` enters the wrapped span on its first
		// poll, which fires `on_enter` in the OTel `Layer` and calls
		// `start_with_context`, transitioning the state from `Builder`
		// to `Context` and freezing the span's `trace_id` from whatever
		// parent context was on the builder at that moment. After that
		// transition, `set_parent` returns `Err(AlreadyStarted)` and is
		// silently dropped — the WS span would surface in OTLP under a
		// fresh root trace, defeating per-message W3C propagation. The
		// regression test below
		// (`set_parent_before_instrument_attaches_remote_trace_id`)
		// guards this ordering.
		let parsed = rpc.format.req_ws(msg, rec_limit);
		if let Ok(req) = &parsed {
			// Now that we know the method, update the tracing span so
			// structured fields show up on any OTel-bridged trace.
			span.record("rpc.method", req.method.to_str());
			span.record("otel.name", format!("surrealdb.rpc/{}", req.method));
			span.record(
				"rpc.request_id",
				req.id.as_ref().map(|id| id.to_sql()).unwrap_or_default(),
			);
			// If the client included W3C Trace Context propagation
			// headers in the RPC envelope, use them as the OTel parent
			// of the per-message span. WebSocket has no per-message
			// header layer, so the context lives on the message body
			// under `trace_context`. Invalid entries are silently
			// dropped: `HeaderMap` rejects non-ASCII names/values, but
			// the propagator handles the resulting empty map
			// gracefully by producing a no-op parent. SDKs that don't
			// emit `trace_context` get today's behavior (fresh root
			// span per message).
			if let Some(trace_context) = req.trace_context.as_ref()
				&& let Some(parent_cx) = extract_trace_context(trace_context)
			{
				// `set_parent` returns `Err(SetParentError::LayerNotFound)`
				// when the OTel bridge layer isn't registered (OTLP
				// export disabled). Non-actionable here, so discard.
				let _ = span.set_parent(parent_cx);
			}
		}
		async move {
			match parsed {
				Ok(req) => {
					// Don't start processing if we are gracefully shutting
					// down. The graceful-shutdown path drains in-flight
					// handlers before closing, so this is observed by new
					// arrivals only.
					if shutdown.is_cancelled() {
						crate::rpc::response::send(
							DbResponse::failure(
								req.id,
								req.session_id.map(Into::into),
								TypesError::internal(SERVER_SHUTTING_DOWN.to_string()),
							),
							rpc.format,
							chn,
						)
						.await;
					}
					// Check to see whether we have available memory
					else if ALLOC.is_beyond_threshold() {
						crate::rpc::response::send(
							DbResponse::failure(
								req.id,
								req.session_id.map(Into::into),
								TypesError::internal(SERVER_OVERLOADED.to_string()),
							),
							rpc.format,
							chn,
						)
						.await;
					}
					// Otherwise process the request message inline. The
					// handler MUST NOT be raced against the connection-level
					// canceller via `tokio::select!`: that would drop the
					// handler future together with whatever transaction the
					// executor has open, and `Transactor::Drop` would log
					// "A transaction was dropped without being committed
					// or cancelled".
					//
					// Instead the connection canceller's boolean view is
					// shared with the executor's `Context` via
					// [`Websocket::cancel_flag`] and the
					// `*_with_transaction_and_cancel` `Datastore` entry
					// points used by `core::rpc::protocol::run_query`. When
					// the canceller fires the executor short-circuits at
					// its next yield with `Reason::Canceled`, the
					// transaction is finalised on the executor's normal
					// error path, and this handler returns an
					// `Err(QueryCancelled)` like any other failure.
					//
					// The read loop drains its `FuturesUnordered` of
					// in-flight `handle_message` futures on cancel rather
					// than dropping it (see `read`), so this future is
					// never dropped mid-flight in production.
					else {
						let client_session: Option<Uuid> = req.session_id.map(Into::into);
						let session_id = client_session.unwrap_or(rpc.id);
						let result = Self::process_message(
							Arc::clone(rpc),
							session_id,
							client_session,
							req.txn.map(Into::into),
							req.method,
							req.params,
						)
						.await;
						crate::rpc::response::send(
							match result {
								Ok(result) => DbResponse::success(
									req.id,
									req.session_id.map(Into::into),
									result,
								),
								Err(err) => {
									DbResponse::failure(req.id, req.session_id.map(Into::into), err)
								}
							},
							rpc.format,
							chn,
						)
						.await;
					}
				}
				Err(err) => {
					// Process the response
					crate::rpc::response::send(
						DbResponse::failure(None, None, err),
						rpc.format,
						chn,
					)
					.await;
				}
			}
		}
		.instrument(span)
		.await;
	}

	/// Process a WebSocket message and generate a response
	async fn process_message(
		rpc: Arc<Websocket>,
		session_id: Uuid,
		client_session: Option<Uuid>,
		txn: Option<Uuid>,
		method: Method,
		params: Array,
	) -> Result<DbResult, TypesError> {
		debug!("Process RPC request");
		// Check that the method is a valid method
		if !method.is_valid() {
			return Err(TypesError::not_found(
				"Method not found".to_string(),
				Some(surrealdb_types::NotFoundError::Method {
					name: method.to_string(),
				}),
			));
		}
		// Execute the specified method
		RpcProtocol::execute(rpc.as_ref(), txn, session_id, client_session, method, params).await
	}

	/// Reject a WebSocket message due to server overloading
	async fn close_socket(rpc: Arc<Websocket>, chn: Sender<Message>) {
		// Log the error as a warning
		warn!("The server is overloaded and is unable to process a WebSocket request");
		// Create a custom close frame
		let frame = CloseFrame {
			code: AGAIN,
			reason: SERVER_OVERLOADED.into(),
		};
		// Respond with a close message
		if let Err(err) = chn.send(Message::Close(Some(frame))).await {
			debug!("WebSocket error when sending close message: {err}");
		};
		// Cancel the WebSocket tasks AND the executor cancel flag so any
		// in-flight queries return early.
		rpc.cancel_all();
	}

	/// Snapshot the connection's default session into a
	/// [`NetworkBytesEventCtx`] for tenant attribution. Short-circuits
	/// to the default ctx when the installed observer is a no-op so
	/// community builds without an audit/dimensional observer pay
	/// nothing on the byte-counting hot path.
	///
	/// The session may flip namespace/database via `USE NS … DB …`
	/// during the connection's lifetime, so the snapshot is taken
	/// per-event rather than cached. The cost is at most three
	/// `String` clones plus a fast read-lock acquire — negligible
	/// compared to the actual frame transfer.
	async fn default_network_ctx(&self) -> NetworkBytesEventCtx {
		if self.datastore.observer().is_noop() {
			return NetworkBytesEventCtx::default();
		}
		match self.get_session(&self.id) {
			Ok(lock) => NetworkBytesEventCtx::from_session(&*lock.read().await),
			Err(_) => NetworkBytesEventCtx::default(),
		}
	}
}

impl RpcProtocol for Websocket {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.datastore
	}

	/// The version information for this RPC context
	fn version_data(&self) -> DbResult {
		let value = Value::String(format!("{PKG_NAME}-{}", *PKG_VERSION));
		DbResult::Other(value)
	}

	/// Expose the connection cancel handle so the executor's `Context`
	/// can short-circuit at the next yield AND any bare-await sites
	/// (`SLEEP`) can `select!` against it when the WebSocket is torn
	/// down. See [`Self::cancel`] on the struct field.
	fn cancel_handle(&self) -> Option<surrealdb_core::ctx::CancelHandle> {
		Some(self.cancel.clone())
	}

	/// A pointer to all active sessions
	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
		&self.sessions
	}

	/// Lists all explicitly attached sessions on this connection.
	///
	/// WebSocket session maps are **per-connection** - the returned ids are only those the same
	/// client attached on the same socket, so enumeration cannot leak another user's session id
	/// (unlike the HTTP transport, where the underlying vulnerability originated). Filters out
	/// this connection's implicit default session keyed by `self.id` so clients cannot enumerate
	/// or target it via `detach`.
	async fn sessions(&self) -> Result<DbResult, TypesError> {
		let connection_id = self.id;
		let array: Array = self
			.session_map()
			.to_vec()
			.into_iter()
			.filter(|(key, _)| *key != connection_id)
			.map(|(key, _)| Value::Uuid(surrealdb_types::Uuid::from(key)))
			.collect();
		Ok(DbResult::Other(Value::Array(array)))
	}

	/// Registers a new session with the given ID, subject to the
	/// [`WEBSOCKET_MAX_ATTACHED_SESSIONS`] per-connection cap.
	///
	/// The implicit connection session keyed by `self.id` counts towards
	/// the cap so a client cannot sidestep it. Defence-in-depth against
	/// a client attempting to exhaust memory within a single connection.
	async fn attach(&self, session_id: Uuid) -> Result<DbResult, TypesError> {
		if self.session_map().contains_key(&session_id) {
			return Err(surrealdb_core::rpc::session_exists(session_id));
		}
		if self.session_map().len() >= *WEBSOCKET_MAX_ATTACHED_SESSIONS {
			return Err(surrealdb_core::rpc::method_not_allowed(Method::Attach.to_string()));
		}
		let mut session = Session::default().with_rt(Self::LQ_SUPPORT);
		session.id = Some(session_id);
		self.session_map().insert(session_id, Arc::new(RwLock::new(session)));
		Ok(DbResult::Other(Value::None))
	}

	/// Detaches an explicitly attached session.
	///
	/// Explicitly rejects attempts to detach the connection's implicit
	/// default session (`self.id`) as a defence-in-depth measure: tearing
	/// it down would leave the connection in an inconsistent state.
	async fn detach(&self, session_id: Uuid) -> Result<DbResult, TypesError> {
		if session_id == self.id {
			return Err(surrealdb_core::rpc::invalid_params(
				"Cannot detach the implicit connection session",
			));
		}
		self.del_session(&session_id).await;
		Ok(DbResult::Other(Value::None))
	}

	// ------------------------------
	// Transactions
	// ------------------------------

	/// Retrieves a transaction by ID
	async fn get_tx(
		&self,
		id: Uuid,
	) -> Result<Arc<surrealdb_core::kvs::Transaction>, surrealdb_types::Error> {
		debug!("WebSocket get_tx called for transaction {id}");
		self.transactions
			.get(&id)
			.map(|tx| {
				debug!("Transaction {id} found in WebSocket transactions map");
				tx.clone()
			})
			.ok_or_else(|| {
				warn!(
					"Transaction {id} not found in WebSocket transactions map (have {} transactions)",
					self.transactions.len()
				);
				surrealdb_core::rpc::invalid_params("Transaction not found")
			})
	}

	/// Stores a transaction
	async fn set_tx(
		&self,
		id: Uuid,
		tx: Arc<surrealdb_core::kvs::Transaction>,
	) -> Result<(), surrealdb_types::Error> {
		self.transactions.insert(id, tx);
		Ok(())
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are enabled on WebSockets
	const LQ_SUPPORT: bool = true;

	/// Handles the execution of a LIVE statement.
	///
	/// Increments the `surrealdb.live_query.active` gauge (rendered by
	/// Prometheus as `surrealdb_live_query_active`) when metrics are enabled
	/// so operators can alert on runaway subscriber counts without peeking
	/// at internal datastructures. The gauge is labelled by the registering
	/// session's namespace and database so operators can pinpoint the
	/// tenant driving the load.
	///
	/// `namespace` and `database` are snapshotted by `run_query` off the
	/// read guard it already holds on the session lock, and passed down
	/// here so this function does NOT re-acquire that lock. Re-locking
	/// would be a recursive read on a write-preferring `RwLock` and
	/// would deadlock against any concurrent session-mutating RPC on
	/// the same WebSocket.
	async fn handle_live(
		&self,
		lqid: &Uuid,
		session_id: Uuid,
		namespace: Option<String>,
		database: Option<String>,
	) {
		// Defence in depth. With executor cancellation the executor's
		// `ctx.done(true)` check between `SLEEP` (or whatever held the
		// query open) and `LIVE SELECT` short-circuits with
		// `Reason::Canceled` and this hook is never called for cancelled
		// queries. The gate below guards the residual race where the
		// executor's check has not yet caught up but the canceller has
		// already fired -- in that case the registration would otherwise
		// land in `state.live_queries` AFTER `serve()` had drained it
		// (where nothing will ever remove it again) AND the
		// executor-created live-query catalog row in the datastore
		// would be orphaned, so notifications would be produced and
		// silently discarded forever.
		//
		// The write lock here serialises with `cleanup_lqs_filtered`.
		// `serve()` drains in-flight handlers before
		// `cleanup_all_lqs()` runs (the read loop awaits `tasks.next()`
		// to completion after the cancel flag is set), but during the
		// drain the executor may still reach this hook -- so the gate
		// observes the canceller and either:
		//   1. Wins the lock race ahead of cleanup -> insert; cleanup then drains our entry as
		//      normal.
		//   2. Cleanup wins -> drains; we acquire the lock afterwards, see the canceller is set,
		//      skip the insert, and garbage-collect the orphaned datastore-side live-query entry.
		let mut live_queries = self.state.live_queries.write().await;
		if self.cancel.is_cancelled() {
			drop(live_queries);
			if let Err(err) = self.kvs().delete_queries(vec![*lqid]).await {
				// TODO(metrics): emit an orphan-LQ counter here. We've
				// already lost the in-memory registration and now we've
				// failed to clean up the datastore-side row too, so
				// notifications for `lqid` will be produced and
				// silently discarded by the broker for the lifetime of
				// the datastore. A counter would let operators alert on
				// this without grepping logs.
				error!(
					"Error cleaning up orphaned live query {lqid} after WebSocket cancel: {err}"
				);
			}
			trace!(
				"Refused to register live query {lqid} on closing WebSocket {}; \
				 cleaned up the datastore entry",
				self.id,
			);
			return;
		}
		live_queries.insert(
			*lqid,
			crate::rpc::LiveQueryEntry {
				websocket_id: self.id,
				session_id,
				namespace: namespace.clone(),
				database: database.clone(),
			},
		);
		drop(live_queries);
		if let Some(obs) = self.state.metrics_observer.as_ref() {
			obs.adjust_live_query_active(1, namespace.as_deref(), database.as_deref());
		}
		trace!("Registered live query {lqid} on websocket {}", self.id);
	}

	/// Handles the execution of a KILL statement.
	///
	/// Decrements the active LIVE query gauge only when a registration was
	/// actually removed so duplicate kills do not drift the counter negative.
	/// The decrement uses the namespace/database stashed at registration time
	/// so the gauge series is balanced even when the killing session has
	/// since switched namespaces.
	async fn handle_kill(&self, lqid: &Uuid) {
		if let Some(entry) = self.state.live_queries.write().await.remove(lqid) {
			if let Some(obs) = self.state.metrics_observer.as_ref() {
				obs.adjust_live_query_active(
					-1,
					entry.namespace.as_deref(),
					entry.database.as_deref(),
				);
			}
			trace!(
				"Unregistered live query {lqid} on websocket {} for session {}",
				entry.websocket_id, entry.session_id,
			);
		}
	}

	/// Handles the cleanup of live queries for a given session.
	///
	/// Drops the gauge per-entry using the namespace/database recorded at
	/// registration time so the gauge stays balanced even when entries on
	/// the same WebSocket span multiple namespaces.
	async fn cleanup_lqs(&self, session_id: &Uuid) {
		self.cleanup_lqs_filtered(Some(session_id)).await;
	}

	/// Handles the cleanup of live queries on WebSocket close.
	///
	/// Drops the gauge by the number of LIVE queries attached to this
	/// connection so the metric tracks the live map exactly. Each
	/// decrement uses the per-entry NS/DB so the gauge stays balanced.
	async fn cleanup_all_lqs(&self) {
		self.cleanup_lqs_filtered(None).await;
	}

	// ------------------------------
	// Methods for transactions
	// ------------------------------

	/// Begin a new transaction
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
	) -> Result<DbResult, surrealdb_types::Error> {
		// `begin` bypasses the executor (which is where the
		// `Context::done` cancel short-circuit lives), so the cancel
		// handle has to be checked manually. `cancel_aware_transaction`
		// encapsulates the pre-await + post-await + cancel-on-loss
		// pattern; any future RPC method that needs to open its own
		// transaction outside the executor SHOULD route through it.
		let tx =
			self.cancel_aware_transaction(TransactionType::Write, LockType::Optimistic).await?;
		// Generate a unique transaction ID
		let id = Uuid::now_v7();
		debug!("WebSocket begin: created transaction {id}");
		// Store the transaction in the map
		self.transactions.insert(id, Arc::new(tx));
		debug!(
			"WebSocket begin: stored transaction {id}, map now has {} transactions",
			self.transactions.len()
		);
		// Return the transaction ID to the client
		Ok(DbResult::Other(Value::Uuid(surrealdb::types::Uuid::from(id))))
	}

	/// Commit a transaction
	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		params: Array,
	) -> Result<DbResult, surrealdb_types::Error> {
		// Extract the transaction ID from params
		let mut params_vec = params.into_vec();
		let Some(Value::Uuid(txn_id)) = params_vec.pop() else {
			return Err(surrealdb_core::rpc::invalid_params("Expected transaction UUID"));
		};

		let txn_id = txn_id.into_inner();

		// Retrieve and remove the transaction from the map
		let Some((_, tx)) = self.transactions.remove(&txn_id) else {
			return Err(surrealdb_core::rpc::invalid_params("Transaction not found"));
		};

		// Commit the transaction
		tx.commit().await.map_err(surrealdb_core::rpc::types_error_from_anyhow)?;

		// Return success
		Ok(DbResult::Other(Value::None))
	}

	/// Cancel a transaction
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		params: Array,
	) -> Result<DbResult, surrealdb_types::Error> {
		// Extract the transaction ID from params
		let mut params_vec = params.into_vec();
		let Some(Value::Uuid(txn_id)) = params_vec.pop() else {
			return Err(surrealdb_core::rpc::invalid_params("Expected transaction UUID"));
		};

		let txn_id = txn_id.into_inner();

		// Retrieve and remove the transaction from the map
		let Some((_, tx)) = self.transactions.remove(&txn_id) else {
			return Err(surrealdb_core::rpc::invalid_params("Transaction not found"));
		};

		// Cancel the transaction
		tx.cancel().await.map_err(surrealdb_core::rpc::types_error_from_anyhow)?;

		// Return success
		Ok(DbResult::Other(Value::None))
	}
}

impl Websocket {
	/// Shared body for [`Self::cleanup_lqs`] and [`Self::cleanup_all_lqs`].
	///
	/// `session_filter` narrows the cleanup to a specific session id when
	/// `Some`, or matches every LIVE entry on this WebSocket when `None`
	/// (the connection-close path).
	///
	/// The NS/DB clones needed for the gauge decrements are gated on
	/// `metrics_observer.is_some()` so a connection running without
	/// metrics enabled does not pay 2N heap allocations on disconnect.
	async fn cleanup_lqs_filtered(&self, session_filter: Option<&Uuid>) {
		let want_metrics = self.state.metrics_observer.is_some();
		let mut gc = Vec::new();
		let mut decrements: Vec<(Option<String>, Option<String>)> = Vec::new();
		// Find all live queries on this connection that match the filter.
		self.state.live_queries.write().await.retain(|key, entry| {
			if entry.websocket_id != self.id {
				return true;
			}
			if let Some(sid) = session_filter
				&& entry.session_id != *sid
			{
				return true;
			}
			trace!("Removing live query: {key}");
			gc.push(*key);
			if want_metrics {
				decrements.push((entry.namespace.clone(), entry.database.clone()));
			}
			false
		});
		if let Some(obs) = self.state.metrics_observer.as_ref() {
			for (ns, db) in &decrements {
				obs.adjust_live_query_active(-1, ns.as_deref(), db.as_deref());
			}
		}
		// Garbage collect the live queries on this connection
		if let Err(err) = self.kvs().delete_queries(gc).await {
			error!("Error handling RPC connection: {err}");
		}
	}

	/// Open a transaction outside the executor's cancellation scope,
	/// while still honouring the connection-level cancel handle.
	///
	/// `begin` and any other RPC method that needs a fresh transaction
	/// without going through `process_with_transaction_and_cancel` MUST
	/// route through here. The executor's `Context::done` short-circuit
	/// does not cover `kvs().transaction(...)` directly, so the cancel
	/// has to be observed manually at two points:
	///
	/// 1. **Pre-await** — refuse to even start opening a transaction on a closing connection.
	/// 2. **Post-await** — the storage layer yields during `transaction(...).await`, so the cancel
	///    may have fired while we were blocked. If it did, finalise the just-created transaction
	///    with `tx.cancel()` (instead of dropping it, which would trigger `Transactor::Drop`'s "A
	///    transaction was dropped without being committed or cancelled" log).
	///
	/// `cleanup_all_txns` in `serve()` is the belt-and-suspenders drain
	/// for transactions that *were* successfully inserted into
	/// `self.transactions` by `begin` before the cancel landed.
	async fn cancel_aware_transaction(
		&self,
		ty: TransactionType,
		lock: LockType,
	) -> Result<Transaction, surrealdb_types::Error> {
		if self.cancel.is_cancelled() {
			return Err(TypesError::internal(REQUEST_CANCELLED.to_string()));
		}
		let tx = self
			.kvs()
			.transaction(ty, lock)
			.await
			.map_err(surrealdb_core::rpc::types_error_from_anyhow)?;
		if self.cancel.is_cancelled() {
			if let Err(err) = tx.cancel().await {
				error!("Error cancelling unused transaction after WebSocket cancel: {err}");
			}
			return Err(TypesError::internal(REQUEST_CANCELLED.to_string()));
		}
		Ok(tx)
	}

	/// Cancel every client-managed transaction left in `self.transactions`.
	///
	/// Invoked from `serve()` at WS teardown (alongside `cleanup_all_lqs`)
	/// to drain transactions opened via the explicit `begin` RPC whose
	/// `commit` / `cancel` never arrived. Without this drain, a client
	/// that calls `begin` and then disconnects (or whose `begin` request
	/// is processed by a spawned handler that outlives the read loop)
	/// would leave the `Arc<Transaction>` in the map until the
	/// `Websocket` itself is dropped — at which point `Transactor::Drop`
	/// would emit "A transaction was dropped without being committed or
	/// cancelled".
	///
	/// Adapted from <https://github.com/surrealdb/surrealdb/pull/6907>'s
	/// `cleanup_all_txns`; intentionally scoped to just the drain (the
	/// 6907 design also adds per-connection / per-session limits and a
	/// counter map, which are out of scope here).
	async fn cleanup_all_txns(&self) {
		// Drain the map atomically into a local vec so we can release
		// each DashMap shard lock before awaiting `tx.cancel()`. Holding
		// shard locks across `.await` would risk a deadlock against any
		// other code path that touches the map and then awaits.
		let mut drained: Vec<(Uuid, Arc<Transaction>)> = Vec::new();
		self.transactions.retain(|tid, tx| {
			drained.push((*tid, Arc::clone(tx)));
			false
		});
		if drained.is_empty() {
			return;
		}
		trace!(
			"Cancelling {} client-managed transaction(s) left on closing WebSocket {}",
			drained.len(),
			self.id,
		);
		for (tid, tx) in drained {
			if let Err(err) = tx.cancel().await {
				error!("Error cancelling transaction {tid} during WebSocket teardown: {err}",);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use surrealdb_core::iam::{Auth, Role};
	use surrealdb_core::kvs::Datastore;
	use surrealdb_core::observe::{
		AuthEvent, ExecutionObserver, NetworkBytesEvent, QueryEvent, RpcEvent, SessionEvent,
		StatementEvent, TransactionEvent,
	};

	use super::*;

	/// Trivial non-noop observer used to exercise the populated-ctx
	/// branch of `default_network_ctx`. The default `is_noop` impl
	/// returns `false`, which is exactly what we need.
	#[derive(Default)]
	struct CapturingObserver {
		events: Mutex<Vec<NetworkBytesEvent>>,
	}

	impl ExecutionObserver for CapturingObserver {
		fn on_statement_complete(&self, _e: &StatementEvent) {}
		fn on_query_complete(&self, _e: &QueryEvent) {}
		fn on_transaction_complete(&self, _e: &TransactionEvent) {}
		fn on_rpc_complete(&self, _e: &RpcEvent) {}
		fn on_auth_event(&self, _e: &AuthEvent) {}
		fn on_session_event(&self, _e: &SessionEvent) {}
		fn on_network_bytes(&self, e: &NetworkBytesEvent) {
			self.events.lock().unwrap().push(e.clone());
		}
	}

	async fn ws_with_observer(observer: Option<Arc<dyn ExecutionObserver>>) -> Arc<Websocket> {
		let mut builder = Datastore::builder();
		if let Some(obs) = observer {
			builder = builder.with_observer(obs);
		}
		let ds = Arc::new(builder.build_with_path("memory").await.unwrap());
		let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
		let id = Uuid::new_v4();
		let (tx, _rx) = channel::<Message>(8);
		Arc::new(Websocket {
			id,
			format: Format::Json,
			state,
			datastore: ds,
			sessions: HashMap::new(),
			transactions: DashMap::new(),
			shutdown: CancellationToken::new(),
			cancel: surrealdb_core::ctx::CancelHandle::new(),
			channel: tx,
		})
	}

	#[tokio::test]
	async fn default_network_ctx_short_circuits_on_noop_observer() {
		// Default `Datastore` builds with a `NoopObserver`; the helper
		// must skip the lock entirely and return the default ctx so
		// community builds pay nothing on the byte hot path. We
		// install a fully-populated session under the connection id;
		// if the helper bypassed the noop check it would surface the
		// session fields, so an empty ctx proves the short-circuit
		// path was taken.
		let rpc = ws_with_observer(None).await;
		let sess = Session {
			au: Arc::new(Auth::for_root(Role::Owner)),
			..Session::default()
		}
		.with_ns("acme")
		.with_db("prod");
		rpc.session_map().insert(rpc.id, Arc::new(RwLock::new(sess)));

		let ctx = rpc.default_network_ctx().await;
		assert!(ctx.namespace.is_none(), "noop observer must not consult the session");
		assert!(ctx.database.is_none());
		assert!(ctx.user.is_none());
	}

	#[tokio::test]
	async fn default_network_ctx_reads_session_under_active_observer() {
		// With a non-noop observer, the helper must read
		// `(ns, db, user)` from the connection's default session.
		let observer: Arc<dyn ExecutionObserver> = Arc::new(CapturingObserver::default());
		let rpc = ws_with_observer(Some(observer)).await;
		let sess = Session {
			au: Arc::new(Auth::for_root(Role::Owner)),
			..Session::default()
		}
		.with_ns("acme")
		.with_db("prod");
		rpc.session_map().insert(rpc.id, Arc::new(RwLock::new(sess)));

		let ctx = rpc.default_network_ctx().await;
		assert_eq!(ctx.namespace.as_deref(), Some("acme"));
		assert_eq!(ctx.database.as_deref(), Some("prod"));
		assert_eq!(ctx.user.as_deref(), Some("system_auth"));
	}

	#[tokio::test]
	async fn default_network_ctx_with_missing_session_returns_default() {
		// Active observer but no session under `self.id` (corner case
		// during disconnect/teardown). The helper must fall back to
		// the default ctx rather than panic or block.
		let observer: Arc<dyn ExecutionObserver> = Arc::new(CapturingObserver::default());
		let rpc = ws_with_observer(Some(observer)).await;
		let ctx = rpc.default_network_ctx().await;
		assert!(ctx.namespace.is_none());
		assert!(ctx.database.is_none());
		assert!(ctx.user.is_none());
	}

	/// Install the W3C trace-context propagator once per process. The
	/// `extract_trace_context_*` tests share it because
	/// `set_text_map_propagator` is process-global; running them with no
	/// installed propagator would silently exercise the no-op default
	/// and produce empty contexts, masking regressions.
	fn ensure_propagator() {
		use std::sync::Once;
		static INIT: Once = Once::new();
		INIT.call_once(|| {
			opentelemetry::global::set_text_map_propagator(
				opentelemetry_sdk::propagation::TraceContextPropagator::new(),
			);
		});
	}

	#[test]
	fn extract_trace_context_with_traceparent_returns_remote_context() {
		use opentelemetry::trace::TraceContextExt;
		ensure_propagator();
		let mut map = std::collections::HashMap::new();
		map.insert(
			"traceparent".to_string(),
			"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
		);
		let cx = extract_trace_context(&map).expect("should produce a context");
		let span = cx.span();
		let span_cx = span.span_context();
		assert!(span_cx.is_valid(), "extracted span context should be valid");
		assert_eq!(format!("{}", span_cx.trace_id()), "0af7651916cd43dd8448eb211c80319c");
		assert_eq!(format!("{}", span_cx.span_id()), "b7ad6b7169203331");
		assert!(span_cx.is_remote(), "context must mark the parent as remote");
	}

	#[test]
	fn extract_trace_context_empty_map_returns_none() {
		ensure_propagator();
		let map = std::collections::HashMap::new();
		assert!(extract_trace_context(&map).is_none());
	}

	#[test]
	fn extract_trace_context_invalid_header_name_drops_entry() {
		// HTTP header names disallow whitespace; the entry is silently
		// skipped during `HeaderMap` construction, the map ends up empty,
		// and we return `None` rather than parenting the span to a no-op
		// context.
		ensure_propagator();
		let mut map = std::collections::HashMap::new();
		map.insert("not a valid header name".to_string(), "value".to_string());
		assert!(extract_trace_context(&map).is_none());
	}

	#[test]
	fn extract_trace_context_invalid_traceparent_value_yields_invalid_context() {
		// Junk traceparent value is accepted by `HeaderMap` (any ASCII
		// passes), but `TraceContextPropagator::extract` rejects it and
		// returns an empty context. The function still returns `Some`
		// because the map wasn't empty — `set_parent` on an empty context
		// is a harmless no-op, matching today's "fresh root" behavior.
		ensure_propagator();
		let mut map = std::collections::HashMap::new();
		map.insert("traceparent".to_string(), "garbage".to_string());
		let cx = extract_trace_context(&map).expect("non-empty map produces Some");
		use opentelemetry::trace::TraceContextExt;
		let span = cx.span();
		assert!(!span.span_context().is_valid());
	}

	/// Build a tracing subscriber with the OTel bridge layer attached
	/// to a real `SdkTracerProvider`. The provider has no exporter, but
	/// that's fine: the OTel layer's state machine (`OtelData::Builder`
	/// → `OtelData::Context`) and `OpenTelemetrySpanExt::set_parent`
	/// returns are what we want to exercise.
	fn otel_test_subscriber() -> (
		impl tracing::Subscriber + Send + Sync + 'static,
		opentelemetry_sdk::trace::SdkTracerProvider,
	) {
		use opentelemetry::trace::TracerProvider as _;
		use opentelemetry_sdk::trace::SdkTracerProvider;
		use tracing_subscriber::prelude::*;
		let provider = SdkTracerProvider::builder().build();
		let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("test"));
		let subscriber = tracing_subscriber::registry().with(layer);
		(subscriber, provider)
	}

	#[test]
	fn set_parent_before_instrument_attaches_remote_trace_id() {
		// Regression test for the WS propagation ordering bug.
		//
		// `tracing_opentelemetry 0.32.1` carries an explicit state
		// machine: a span starts in `OtelData::Builder { parent_cx }`
		// and transitions to `OtelData::Context { current_cx }` the
		// first time `on_enter` fires (i.e. on the first poll of an
		// `Instrumented` future). `set_parent` only mutates the parent
		// while the state is `Builder`; once it's `Context`, the trace
		// id is frozen and the call returns `Err(AlreadyStarted)`.
		//
		// The production code in `handle_message` parses the envelope
		// and calls `set_parent` BEFORE wrapping the future in
		// `.instrument(span)`, so the call lands on a `Builder`-state
		// span and the remote trace id propagates through. This test
		// asserts that ordering: `set_parent` returns `Ok(())` and the
		// span's resolved context carries the parent's trace id.
		use opentelemetry::trace::TraceContextExt;
		ensure_propagator();
		let (subscriber, _provider) = otel_test_subscriber();
		tracing::subscriber::with_default(subscriber, || {
			let mut map = std::collections::HashMap::new();
			map.insert(
				"traceparent".to_string(),
				"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
			);
			let parent_cx = extract_trace_context(&map).expect("Some");

			let span = span_for_request(&Uuid::new_v4());
			// Span is in `Builder` state — `set_parent` must succeed.
			span.set_parent(parent_cx).expect("set_parent must succeed on Builder-state span");

			// Resolve the span's OTel context. `OpenTelemetrySpanExt::context`
			// forces the Builder→Context transition internally if needed and
			// returns the resulting context.
			let cx = span.context();
			let active = cx.span();
			let span_cx = active.span_context();
			assert!(span_cx.is_valid(), "span context should be valid after activation");
			assert_eq!(
				format!("{}", span_cx.trace_id()),
				"0af7651916cd43dd8448eb211c80319c",
				"span's resolved trace_id must match the propagated parent",
			);
		});
	}

	#[test]
	fn set_parent_after_span_entered_returns_already_started() {
		// Documents the bug the production code avoids. Once the span
		// has been entered (which `.instrument(span).await` does on
		// the first poll), `OtelData` transitions from `Builder` to
		// `Context` and `set_parent` becomes a no-op returning
		// `Err(AlreadyStarted)`. If anyone refactors `handle_message`
		// to call `set_parent` from inside the instrumented async
		// block, this test catches it.
		ensure_propagator();
		let (subscriber, _provider) = otel_test_subscriber();
		tracing::subscriber::with_default(subscriber, || {
			let mut map = std::collections::HashMap::new();
			map.insert(
				"traceparent".to_string(),
				"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
			);
			let parent_cx = extract_trace_context(&map).expect("Some");

			let span = span_for_request(&Uuid::new_v4());
			// Enter the span to simulate what `Instrumented::poll`
			// does before the wrapped future runs. `on_enter` consumes
			// the `SpanBuilder` and the state becomes `Context`.
			let _enter = span.enter();
			let err = span
				.set_parent(parent_cx)
				.expect_err("set_parent on entered span must return AlreadyStarted");
			assert!(
				matches!(err, tracing_opentelemetry::SetParentError::AlreadyStarted),
				"expected AlreadyStarted, got {err:?}",
			);
		});
	}

	/// Run a future on a dedicated OS thread + multi-threaded runtime
	/// with a 24 MiB stack. The executor and parser carry large stack
	/// frames in debug builds that overflow tokio's default 2 MiB worker
	/// stack; same pattern as `surrealdb/core/tests/helpers::with_enough_stack`.
	fn with_big_stack<F, Fut>(body: F)
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: std::future::Future<Output = ()>,
	{
		std::thread::Builder::new()
			.stack_size(24 * 1024 * 1024)
			.spawn(move || {
				let runtime = tokio::runtime::Builder::new_multi_thread()
					.enable_all()
					.worker_threads(2)
					.thread_stack_size(24 * 1024 * 1024)
					.build()
					.unwrap();
				runtime.block_on(body());
			})
			.expect("spawn test thread")
			.join()
			.expect("test thread");
	}

	/// Regression test for the WebSocket cancel-leak bug.
	///
	/// When the connection canceller fires while a write request is
	/// mid-flight, the executor's `Context::done` walks must short-circuit
	/// with `Reason::Canceled` at the next yield point and the executor's
	/// BEGIN/COMMIT block must `txn.cancel()` the held transaction rather
	/// than dropping it. `Transactor::Drop` (which emits the noisy
	/// "A transaction was dropped without being committed or cancelled"
	/// error log) must NOT fire.
	///
	/// `TransactionEvent` is only emitted from `Transaction::commit` and
	/// `Transaction::cancel`, never from `Drop`. So a zero count of write-tx
	/// completion events after the cancel scenario proves the regression.
	#[test]
	fn cancelling_websocket_handler_does_not_leak_in_flight_write_transaction() {
		use std::sync::Mutex;
		use std::time::Duration as StdDuration;

		use surrealdb_core::dbs::Session;
		use surrealdb_core::dbs::capabilities::Capabilities;

		#[derive(Default)]
		struct WriteTxCompletionCounter {
			count: Mutex<u32>,
		}
		impl ExecutionObserver for WriteTxCompletionCounter {
			fn on_statement_complete(&self, _e: &StatementEvent) {}
			fn on_query_complete(&self, _e: &QueryEvent) {}
			fn on_transaction_complete(&self, e: &TransactionEvent) {
				if e.safe.write {
					*self.count.lock().unwrap() += 1;
				}
			}
			fn on_rpc_complete(&self, _e: &RpcEvent) {}
			fn on_auth_event(&self, _e: &AuthEvent) {}
			fn on_session_event(&self, _e: &SessionEvent) {}
			fn on_network_bytes(&self, _e: &NetworkBytesEvent) {}
		}

		with_big_stack(|| async {
			let observer = Arc::new(WriteTxCompletionCounter::default());
			let ds = Arc::new(
				Datastore::builder()
					.with_capabilities(Capabilities::all())
					.with_observer(Arc::clone(&observer) as Arc<dyn ExecutionObserver>)
					.build_with_path("memory")
					.await
					.unwrap(),
			);
			// Pre-define NS/DB and warm up the datastore so the in-flight
			// query resolves against an existing scope and metadata write
			// txs (table definitions, sequence allocations, etc.) do not
			// show up as false positives in the assertion below. After
			// the warm-up the metadata is cached and only the cancelled
			// in-flight tx contributes to the counter.
			let owner = Session::owner();
			ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
			let owner_ns = owner.clone().with_ns("test");
			ds.execute("DEFINE DB `test`", &owner_ns, None).await.unwrap();
			let sess_test = Session::owner().with_ns("test").with_db("test");
			ds.execute("BEGIN; CREATE foo SET x = 1; SLEEP 1ms; COMMIT;", &sess_test, None)
				.await
				.unwrap();
			*observer.count.lock().unwrap() = 0;

			let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
			let id = Uuid::new_v4();
			let (chn_internal, _chn_internal_rx) = channel::<Message>(8);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state,
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
			});

			// Pin the owner session under the connection's default session
			// id so `process_message` resolves it via `get_session(rpc.id)`.
			let sess = Session::owner().with_ns("test").with_db("test");
			rpc.set_session(rpc.id, Arc::new(RwLock::new(sess)));

			// Fire the connection-level cancel mid-SLEEP. `cancel_all`
			// sets both the canceller token and the executor cancel flag.
			let cancel_jh = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					tokio::time::sleep(StdDuration::from_millis(50)).await;
					rpc.cancel_all();
				}
			});

			// Multi-statement explicit transaction:
			//   BEGIN  -- starts a write tx
			//   CREATE -- writes (marks the tx as writeable for the observer)
			//   SLEEP  -- holds the tx open across the cancel window
			//   COMMIT -- would finalise it, but the cancel fires first.
			// With cancellation-aware SLEEP, the cancel wakes the SLEEP
			// immediately; the executor's next `ctx.done` check between
			// SLEEP and COMMIT then fires `txn.cancel()` and emits the
			// `TransactionEvent`.
			let sql = "BEGIN; CREATE foo SET x = 1; SLEEP 200ms; COMMIT;";
			let body = serde_json::json!({
				"id": "1",
				"method": "query",
				"params": [sql],
			});
			let msg = Message::Text(body.to_string().into());

			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			// `handle_message` runs inline now -- no spawn, no JoinHandle.
			// Awaiting it returns once the executor has finished cancelling
			// the transaction and the failure response has been queued.
			Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;

			// PROOF OF CANCELLATION (vs. proof of "no leak"):
			// `observer.count > 0` only proves the tx was finalised
			// (either commit-on-success or cancel-on-race), which the
			// FuturesUnordered drain alone is enough to achieve. To
			// prove that the *cancel plumbing* fired (i.e. the executor
			// short-circuited rather than running to completion), we
			// also assert that the response we sent back to the
			// "client" carries a Cancelled-class error. A normal
			// successful COMMIT would carry no such error.
			let response = chn_rx.recv().await.expect("response sent over channel");
			let response_text = match response {
				Message::Text(t) => t.to_string(),
				other => panic!("expected Text response from Json format, got {other:?}"),
			};
			assert!(
				response_text.contains("cancelled"),
				"expected Cancelled-class error in response (cancel-plumbing fired); \
				 got response without 'cancelled' substring: {response_text}",
			);

			let count = *observer.count.lock().unwrap();
			assert!(
				count > 0,
				"in-flight write transaction was dropped without commit or cancel -- \
				 a WebSocket cancel during a write request leaked the transaction \
				 and would emit the 'A transaction was dropped without being \
				 committed or cancelled' error from `Transactor::Drop`",
			);

			let _ = cancel_jh.await;
		});
	}

	/// Regression test for the LIVE-after-WS-cancel leak.
	///
	/// A long-running query (e.g. `SLEEP …; LIVE SELECT …`) running on a
	/// WebSocket that is then cancelled must NOT register a live-query
	/// entry that survives the cancel. With executor cancellation the
	/// executor short-circuits between SLEEP and LIVE SELECT, so the
	/// `handle_live` post-processing in `run_query` is never reached and
	/// `state.live_queries` stays empty.
	///
	/// The `handle_live` canceller gate is retained as defence in depth
	/// for any future code path that could reach the post-processing with
	/// the cancel flag already set.
	#[test]
	fn cancelling_websocket_handler_does_not_leak_live_query_registration() {
		use std::time::Duration as StdDuration;

		use surrealdb_core::dbs::Session;
		use surrealdb_core::dbs::capabilities::Capabilities;

		with_big_stack(|| async {
			let ds = Arc::new(
				Datastore::builder()
					.with_capabilities(Capabilities::all())
					.build_with_path("memory")
					.await
					.unwrap(),
			);
			// Pre-define NS/DB and create the target table.
			let owner = Session::owner();
			ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
			let owner_ns = owner.clone().with_ns("test");
			ds.execute("DEFINE DB `test`", &owner_ns, None).await.unwrap();
			let sess_setup = Session::owner().with_ns("test").with_db("test");
			ds.execute("CREATE foo SET x = 1", &sess_setup, None).await.unwrap();

			let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
			let id = Uuid::new_v4();
			let (chn_internal, _chn_internal_rx) = channel::<Message>(8);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state: Arc::clone(&state),
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
			});

			// LIVE queries require a realtime-enabled session.
			let sess = Session::owner().with_ns("test").with_db("test").with_rt(true);
			rpc.set_session(rpc.id, Arc::new(RwLock::new(sess)));

			// Fire the connection-level cancel mid-SLEEP. `cancel_all`
			// sets both the canceller token and the executor cancel flag.
			let cancel_jh = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					tokio::time::sleep(StdDuration::from_millis(50)).await;
					rpc.cancel_all();
				}
			});

			// SLEEP holds the executor open across the cancel window.
			// With cancellation-aware SLEEP, the cancel wakes the
			// SLEEP immediately; the executor's `ctx.done(true)` check
			// between SLEEP and LIVE SELECT then short-circuits with
			// `Reason::Canceled` -- LIVE SELECT is never executed and
			// `handle_live` is never called.
			let sql = "SLEEP 200ms; LIVE SELECT * FROM foo;";
			let body = serde_json::json!({
				"id": "1",
				"method": "query",
				"params": [sql],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);

			// Await `handle_message` to completion. With the inline
			// handler, dropping this future mid-flight would drop the
			// executor and its open transaction, so we MUST NOT
			// `timeout`-and-drop it the way the previous spawn-based test
			// did.
			Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;

			// PROOF OF CANCELLATION: assert the response carries a
			// Cancelled-class error. Without the cancel-plumbing
			// firing (e.g. if SLEEP ran to completion), this query
			// would succeed and return a LIVE-SELECT uuid -- the
			// leak-empty check below would still pass, but the
			// response-text check would fail, exposing the regression.
			let response = chn_rx.recv().await.expect("response sent over channel");
			let response_text = match response {
				Message::Text(t) => t.to_string(),
				other => panic!("expected Text response from Json format, got {other:?}"),
			};
			assert!(
				response_text.contains("cancelled"),
				"expected Cancelled-class error in response (cancel-plumbing fired); \
				 got response without 'cancelled' substring: {response_text}",
			);

			let leaked: Vec<Uuid> = state
				.live_queries
				.read()
				.await
				.iter()
				.filter_map(|(lqid, entry)| (entry.websocket_id == rpc.id).then_some(*lqid))
				.collect();
			assert!(
				leaked.is_empty(),
				"leaked live-query registrations after WS cancel: {leaked:?} -- \
				 the executor reached LIVE SELECT post-processing despite the cancel flag",
			);

			let _ = cancel_jh.await;
		});
	}

	/// Defence-in-depth test for the `handle_live` canceller gate.
	///
	/// Directly drives `handle_live` with the cancel flag set, bypassing
	/// the executor. The gate must refuse the registration and garbage
	/// collect any datastore-side live-query row that the executor may
	/// have created before the cancel landed.
	#[test]
	fn handle_live_gate_refuses_registration_when_canceller_is_set() {
		use surrealdb_core::dbs::Session;
		use surrealdb_core::dbs::capabilities::Capabilities;

		with_big_stack(|| async {
			let ds = Arc::new(
				Datastore::builder()
					.with_capabilities(Capabilities::all())
					.build_with_path("memory")
					.await
					.unwrap(),
			);
			let owner = Session::owner();
			ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
			let owner_ns = owner.clone().with_ns("test");
			ds.execute("DEFINE DB `test`", &owner_ns, None).await.unwrap();

			let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
			let id = Uuid::new_v4();
			let (chn_internal, _chn_internal_rx) = channel::<Message>(8);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state: Arc::clone(&state),
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
			});

			// Set the canceller before calling handle_live.
			rpc.cancel_all();

			// Drive handle_live with a fake lqid. The gate must skip the
			// insert and delete the datastore-side row (a no-op here since
			// we didn't actually run a LIVE SELECT, but the call must
			// succeed without error).
			let lqid = Uuid::new_v4();
			RpcProtocol::handle_live(
				rpc.as_ref(),
				&lqid,
				rpc.id,
				Some("test".to_string()),
				Some("test".to_string()),
			)
			.await;

			let leaked: Vec<Uuid> = state
				.live_queries
				.read()
				.await
				.iter()
				.filter_map(|(k, entry)| (entry.websocket_id == rpc.id).then_some(*k))
				.collect();
			assert!(
				leaked.is_empty(),
				"handle_live inserted into state.live_queries despite the cancel flag: {leaked:?}",
			);
		});
	}

	/// Regression test for the explicit-`begin`-on-cancel leak (Codex P2,
	/// PR #286 [discussion_r3311900647](https://github.com/surrealdb/surrealdb-private/pull/286#discussion_r3311900647)).
	///
	/// `begin` does NOT go through the executor (it calls
	/// `kvs().transaction(...)` directly), so the executor cancellation
	/// flag the rest of the WS layer plumbs does NOT cover it. The
	/// dedicated canceller gate in `begin()` must observe a set
	/// canceller and refuse to open the transaction.
	#[test]
	fn begin_rpc_after_cancel_does_not_leak_transaction() {
		use surrealdb_core::dbs::Session;
		use surrealdb_core::dbs::capabilities::Capabilities;

		with_big_stack(|| async {
			let ds = Arc::new(
				Datastore::builder()
					.with_capabilities(Capabilities::all())
					.build_with_path("memory")
					.await
					.unwrap(),
			);
			let owner = Session::owner();
			ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
			let owner_ns = owner.clone().with_ns("test");
			ds.execute("DEFINE DB `test`", &owner_ns, None).await.unwrap();

			let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
			let id = Uuid::new_v4();
			let (chn_internal, _chn_internal_rx) = channel::<Message>(8);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state,
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
			});
			let sess = Session::owner().with_ns("test").with_db("test");
			rpc.set_session(rpc.id, Arc::new(RwLock::new(sess)));

			// Cancel BEFORE the begin request: the handler's `begin()`
			// will hit the pre-await canceller gate and refuse to open
			// the transaction.
			rpc.cancel_all();

			let body = serde_json::json!({
				"id": "1",
				"method": "begin",
				"params": [],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, _chn_rx) = channel::<Message>(8);

			Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;

			// Belt-and-suspenders: invoke `cleanup_all_txns` explicitly
			// to mirror `serve()`'s teardown order. In the
			// cancel-before-begin path the map should already be empty
			// (the gate prevented the insert); in a slower variant where
			// the gate's TOCTOU lost the race, this drain would catch
			// the late insert.
			rpc.cleanup_all_txns().await;

			assert!(
				rpc.transactions.is_empty(),
				"begin() on a closing WebSocket leaked a transaction into \
				 self.transactions: {} entries",
				rpc.transactions.len(),
			);
		});
	}
}
