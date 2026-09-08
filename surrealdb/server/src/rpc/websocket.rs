use core::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::ws::close_code::AGAIN;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry_http::HeaderExtractor;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::mem::ALLOC;
use surrealdb_core::rpc::RpcProtocol;
use surrealdb_core::rpc::format::Format;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::TransactionType;
use surrealdb_observe::{
	NetworkBytesEvent, NetworkBytesEventCtx, NetworkBytesEventSafe, NetworkDirection,
	SessionAction, SessionEvent, SessionEventCtx, SessionEventSafe, SessionProtocol,
};
use surrealdb_rpc::error::{invalid_params, stream_not_found};
use surrealdb_rpc::{DbResponse, DbResult, Method};
use surrealdb_types::{Array, Error as TypesError, HashMap, ToSql, Value};
use tokio::sync::RwLock;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;

/// The merged view of a connection's outbound queues; see
/// [`Websocket::outbound_queues`].
type Outbound = futures::stream::Select<
	futures::stream::Select<ReceiverStream<Message>, ReceiverStream<Message>>,
	ReceiverStream<Message>,
>;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use super::RpcState;
use crate::cnf::{
	MAX_TRANSACTIONS_PER_CONNECTION, MAX_TRANSACTIONS_PER_SESSION, PKG_NAME, PKG_VERSION,
	WEBSOCKET_MAX_ATTACHED_SESSIONS, WEBSOCKET_PING_FREQUENCY, WEBSOCKET_RESPONSE_BUFFER_SIZE,
	WEBSOCKET_RESPONSE_CHANNEL_SIZE, WEBSOCKET_RESPONSE_FLUSH_PERIOD,
};
use crate::rpc::CONN_CLOSED_ERR;
use crate::rpc::format::WsFormat;
use crate::telemetry::traces::rpc::span_for_request;

/// An error string sent when the server is out of memory
const SERVER_OVERLOADED: &str = "The server is unable to handle the request";

/// Report a request refused because tracked memory crossed
/// `SURREAL_MEMORY_THRESHOLD`.
///
/// A refusal short-circuits before dispatch, so no statement or RPC metric
/// moves and the only other outward sign is that clients stop getting answers.
/// `transport` names the ingress that refused, and the two byte counts — taken
/// from the check that refused, so the total is not recomputed — are what an
/// operator needs to tell "the threshold is set too low" apart from "this
/// instance is genuinely out of memory".
pub(crate) fn log_memory_refusal(transport: &str, allocated: usize, threshold: usize) {
	warn!(
		transport,
		memory_allocated = allocated,
		memory_threshold = threshold,
		"Refusing request: tracked memory is beyond SURREAL_MEMORY_THRESHOLD"
	);
}

/// An error string sent when the server is gracefully shutting down
const SERVER_SHUTTING_DOWN: &str = "The server is gracefully shutting down";

/// An error string surfaced by the `begin` RPC when the WebSocket
/// canceller has already fired -- avoids opening a write transaction on
/// a connection that is about to be torn down. Executor-driven RPCs
/// (query, etc.) surface `Error::QueryCancelled` from the executor
/// instead; this constant is `begin`-only because `begin` bypasses the
/// executor and goes straight to `kvs().transaction(...)`.
const REQUEST_CANCELLED: &str = "The request was cancelled because the WebSocket is closing";

/// An error string sent when a client stops reading the notifications its
/// live queries produce, filling the connection's outbound queue
const CLIENT_LAGGING: &str = "Notifications were produced faster than this connection read them";

/// How long the write loop may spend, in total, draining what is still
/// queued once the connection is being torn down.
///
/// Short on purpose: the frame this exists to deliver is a close frame
/// naming why the connection is ending, and the client most in need of it is
/// one that may have stopped reading altogether. Waiting longer would delay
/// the teardown of a connection that is already over.
const TEARDOWN_FLUSH_GRACE: Duration = Duration::from_secs(1);

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
	/// The active transactions for this WebSocket connection, each tracked with
	/// the id of the session that opened it (the connection's implicit default
	/// session is keyed by `self.id`). The session id drives per-session cleanup
	/// and the per-session transaction limit.
	pub(crate) transactions: DashMap<Uuid, (Uuid, Arc<Transaction>)>,
	/// Per-session counts of currently open transactions, keyed by session id.
	/// Enforces [`MAX_TRANSACTIONS_PER_CONNECTION`] /
	/// [`MAX_TRANSACTIONS_PER_SESSION`].
	pub(crate) counters: DashMap<Uuid, AtomicUsize>,
	/// The streaming queries currently executing on this connection, keyed by
	/// the canonical rendering of each one's request id. Entered before a
	/// stream's execution starts and removed when its driver returns, so
	/// `query_cancel` can reach any stream that is still producing frames.
	/// Per-connection, like [`Self::sessions`]: a client can only ever cancel
	/// its own streams. Bounded by [`Self::stream_slots`].
	pub(crate) streams: DashMap<String, crate::rpc::streaming::StreamHandle>,
	/// How many of this connection's concurrent-stream slots are claimed.
	///
	/// This count, not [`Self::streams`]'s length, is what
	/// [`WEBSOCKET_MAX_CONCURRENT_STREAMS`] is enforced against: a request
	/// claims its slot here, atomically and only while one is free, before it
	/// may enter the registry, and gives the slot back only after leaving it.
	/// So the registry is never larger than the cap at any instant a
	/// concurrent observer could catch it.
	pub(crate) stream_slots: AtomicUsize,
	/// A cancellation token called when shutting down the server
	pub(crate) shutdown: CancellationToken,
	/// Connection-level cancellation handle. Bundles a hot-path
	/// `AtomicBool` (consumed by the executor's `Context::done` walks) and
	/// an awaitable `CancellationToken` (consumed by `tokio::select!`
	/// sites such as the read/ping/write loops and `SLEEP`). Tripped in
	/// lockstep by [`Self::cancel_all`]; there is no way to fire one view
	/// without the other.
	pub(crate) cancel: surrealdb_core::ctx::CancelHandle,
	/// The channel used to send solicited output: everything answering a
	/// request this client made, plus the overload close frame.
	pub(crate) channel: Sender<Message>,
	/// The channel used to send unsolicited output: live-query notifications,
	/// and the close frame that ends a connection which has stopped reading
	/// them.
	///
	/// Separate from [`Self::channel`] because occupancy here is what decides
	/// that a client has stopped reading its notifications, and a client's own
	/// pipelined requests must not be able to look like that. One shared queue
	/// could not tell the two apart: a burst of replies filled it -- a single
	/// reply, at the configured minimum size -- and the next notification
	/// would end a connection whose writer was draining normally.
	///
	/// The queues are drained fairly, so none starves the others, and order
	/// is preserved within each. Nothing orders a notification against a
	/// reply: a notification answers no request.
	pub(crate) notifications: Sender<Message>,
	/// A single slot carrying the close frame that ends a connection which
	/// stopped reading its notifications.
	///
	/// Its own queue, because a slot held back inside another one is reserved
	/// only by convention: two deliveries can both read the same free slot
	/// before either fills it, and the second then cancels the connection
	/// with no room left to say why. A queue of one that nothing else may
	/// write cannot be filled out from under the delivery that needs it, and
	/// exactly one delivery can claim it -- so however many find the
	/// notification queue full at once, one close frame is queued and the
	/// client always learns the reason.
	pub(crate) lagging_close: Sender<Message>,
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
		// Create the channels for sending messages. Solicited and unsolicited
		// output queue separately; see [`Websocket::notifications`].
		let (sender, receiver) = channel(*WEBSOCKET_RESPONSE_CHANNEL_SIZE);
		let (notifier, notification_receiver) = channel(*WEBSOCKET_RESPONSE_CHANNEL_SIZE);
		let (closer, close_receiver) = channel(1);
		let rec_limit = datastore.parser_config().max_object_parsing_depth as usize;
		// Create and store the RPC connection
		let rpc = Arc::new(Websocket {
			id,
			format,
			state: Arc::clone(&state),
			shutdown: CancellationToken::new(),
			cancel: surrealdb_core::ctx::CancelHandle::new(),
			sessions: HashMap::new(),
			transactions: DashMap::new(),
			counters: DashMap::new(),
			streams: DashMap::new(),
			stream_slots: AtomicUsize::new(0),
			channel: sender.clone(),
			notifications: notifier.clone(),
			lagging_close: closer.clone(),
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
				tasks.spawn(Self::read(Arc::clone(&rpc), ws_receiver, sender.clone(), rec_limit));
				tasks.spawn(Self::write(
					Arc::clone(&rpc),
					ws_sender,
					receiver,
					notification_receiver,
					close_receiver,
				));
			}
			false => {
				// Split the socket into sending and receiving streams
				let (ws_sender, ws_receiver) = ws.split();
				// Spawn async tasks for the WebSocket
				tasks.spawn(Self::read(Arc::clone(&rpc), ws_receiver, sender.clone(), rec_limit));
				tasks.spawn(Self::write(
					Arc::clone(&rpc),
					ws_sender,
					receiver,
					notification_receiver,
					close_receiver,
				));
			}
		}
		// Wait for all tasks to finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				error!("Error handling RPC connection: {err}");
			}
		}
		// Close the internal output channels
		std::mem::drop(sender);
		std::mem::drop(notifier);
		std::mem::drop(closer);
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

	/// One view of a connection's outbound queues.
	///
	/// Every queue alternates with the others, so none can starve any of
	/// them. Biasing replies would let a pipelining client hold its own
	/// notifications back until the notification queue filled -- the state
	/// this connection is closed for; biasing notifications would delay the
	/// answers the client is waiting on; and deferring the lagging close
	/// frame until the other two ran dry would let the replies that
	/// cancellation is still draining keep it waiting until the grace period
	/// ran out, disconnecting a client without telling it why.
	///
	/// What keeps a close frame last on the wire is not this order but that
	/// the writer stops after one: nothing may follow a close, so whoever
	/// writes it writes nothing further. Alternation bounds how long it waits
	/// to be that frame -- at most one other goes ahead of it.
	///
	/// The stream ends only when every queue does, so one closing early
	/// cannot strand what is left in another.
	fn outbound_queues(
		replies: Receiver<Message>,
		notifications: Receiver<Message>,
		lagging_close: Receiver<Message>,
	) -> Outbound {
		futures::stream::select(
			futures::stream::select(
				ReceiverStream::new(replies),
				ReceiverStream::new(notifications),
			),
			ReceiverStream::new(lagging_close),
		)
	}

	/// Write messages to the client, and keep the connection alive
	///
	/// Owns the sending half of the socket, so it also emits the periodic ping:
	/// a separate ping task would have to hand its message to this one through
	/// the response channel anyway.
	async fn write<S: SinkExt<Message> + Unpin>(
		rpc: Arc<Websocket>,
		mut socket: S,
		internal_receiver: Receiver<Message>,
		notification_receiver: Receiver<Message>,
		close_receiver: Receiver<Message>,
	) where
		<S as Sink<Message>>::Error: fmt::Display,
	{
		let mut outbound =
			Self::outbound_queues(internal_receiver, notification_receiver, close_receiver);
		// Clone the WebSocket cancellation token (awaitable view of the
		// shared cancel handle).
		let canceller = rpc.cancel.token();
		// Check if the responses are buffered
		let buffer = *WEBSOCKET_RESPONSE_BUFFER_SIZE > 0;
		// How often should responses be flushed
		let period = Duration::from_millis(*WEBSOCKET_RESPONSE_FLUSH_PERIOD);
		// Keepalive ticker. The leading tick fires immediately, so a connection
		// is pinged as soon as it opens.
		let mut ping = tokio::time::interval(WEBSOCKET_PING_FREQUENCY);
		// Pings convey liveness, not history: after a stall, one ping says as
		// much as the whole backlog, so missed ticks are dropped rather than
		// burst onto the socket.
		ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		// Whether a close frame has gone out, after which the wire carries
		// nothing more.
		let mut closed = false;
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				// Process brances in order
				biased;
				// Check if we should teardown
				_ = canceller.cancelled() => break,
				// Send a regular ping message. Polled ahead of the response
				// channel so a saturated connection cannot starve the
				// keepalive; a tick is only ready once per period, so it
				// delays a queued response by at most one message.
				_ = ping.tick() => {
					// Write through the same buffer-aware path as responses so
					// the ping cannot overtake an already-queued message.
					let msg = Message::Ping(Bytes::from_static(b""));
					let result = match buffer {
						true => socket.feed(msg).await,
						false => socket.send(msg).await,
					};
					// Close the connection if the message fails
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
				},
				// Retrieve a message from any outbound queue
				Some(res) = outbound.next() => {
					// Capture the byte length before the message is moved
					// into the sink so we can fold it into the outbound
					// Prometheus counter when metrics are enabled.
					let out_bytes = Self::payload_len(&res);
					// And whether this is the last frame the wire may carry.
					let terminal = Self::ends_the_connection(&res);
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
					// double-counting on the error path above.
					rpc.record_sent_bytes(out_bytes).await;
					// Nothing may follow a close frame, so stop here rather
					// than leave the drain below to write behind it.
					if terminal {
						closed = true;
						break;
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
		// Write out whatever teardown left in the queues, and close.
		rpc.flush_on_teardown(&mut socket, &mut outbound, closed).await;
	}

	/// Write what is already queued and close the socket, so the close frame
	/// the connection owes its peer reaches the client.
	///
	/// Two kinds of close frame end up here. One this connection chose to send
	/// -- the overload notice in [`Self::close_socket`] on the reply queue,
	/// the lagging notice in [`Self::deliver_notification`] on its own slot --
	/// is queued and then trips the connection canceller, and the write loop
	/// polls that canceller ahead of the queues, so this drain on the way past
	/// is what carries it out; without it the client would be left to infer an
	/// unexplained disconnect in place of a stated reason. The reply owed to a
	/// close frame the peer sent is instead queued inside the transport, which
	/// read the connection as closing and refuses a second one from us, so
	/// closing the sink -- not merely flushing it -- is what writes that reply
	/// out and completes the handshake RFC 6455 section 5.5.1 requires.
	///
	/// `closed` says the write loop has already put a close frame on the wire,
	/// which leaves nothing to drain: the queues are skipped and only the
	/// close below runs.
	///
	/// Only what is already queued, because senders remain alive and a `recv`
	/// would wait for a message that is not coming. Each frame is sent rather
	/// than fed, so a frame the transport refuses fails on itself instead of
	/// being left parked in the sink, where it would abort the close behind it.
	/// Bounded by [`TEARDOWN_FLUSH_GRACE`] across the whole drain and
	/// abandoned on the first write error: the client this serves most is by
	/// definition one that may have stopped reading, and the connection is
	/// over either way.
	///
	/// Nothing else here waits, which is what keeps that bound the real one:
	/// byte accounting runs after the writes and never blocks on the session
	/// (see [`Self::teardown_network_ctx`]), so no request a client issues can
	/// stretch its own connection's teardown -- and with it a graceful
	/// shutdown -- past the grace period.
	async fn flush_on_teardown<S, Q>(&self, socket: &mut S, queued: &mut Q, closed: bool)
	where
		S: SinkExt<Message> + Unpin,
		<S as Sink<Message>>::Error: fmt::Display,
		Q: Stream<Item = Message> + Unpin,
	{
		let deadline = tokio::time::Instant::now() + TEARDOWN_FLUSH_GRACE;
		// What the drain wrote, charged once the writing is done rather than
		// between frames. Attribution reads the session, and nothing may sit
		// between one teardown write and the next that a client's own
		// concurrent request can extend -- see [`Self::teardown_network_ctx`].
		let mut written = Vec::new();
		// `now_or_never` rather than an await, so this takes what is already
		// queued and does not wait on senders that are still alive. Skipped
		// once a close frame has gone out: the wire carries nothing after
		// one, so there is nothing left to drain -- only the close below.
		while !closed && let Some(Some(msg)) = queued.next().now_or_never() {
			// Before the message moves into the sink, as in the write loop.
			let out_bytes = Self::payload_len(&msg);
			let terminal = Self::ends_the_connection(&msg);
			if !Self::teardown_progressed(tokio::time::timeout_at(deadline, socket.send(msg)).await)
			{
				// Stop draining, but still close below: the handshake has to
				// be finished off whether or not the frame behind this one
				// made it out.
				break;
			}
			written.push(out_bytes);
			// Nothing may follow a close frame.
			if terminal {
				break;
			}
		}
		// Close unconditionally, not just when something was drained above.
		// In buffered mode the write loop feeds without flushing and relies on
		// its periodic flush, so a frame it took from the channel before
		// handing over is sitting in the sink rather than on the wire -- and a
		// frame in the sink has not reached the client any more than one left
		// in the channel had. The reply to a peer's close frame is likewise
		// held inside the transport until the socket closes.
		Self::teardown_progressed(tokio::time::timeout_at(deadline, socket.close()).await);
		// A frame written here left the machine like any other, so it is
		// charged like any other.
		self.charge_teardown_bytes(&written).await;
	}

	/// Charge the frames [`Self::flush_on_teardown`] wrote, one event each as
	/// on every other path, from a single attribution snapshot.
	async fn charge_teardown_bytes(&self, written: &[usize]) {
		if written.iter().all(|bytes| *bytes == 0) {
			return;
		}
		let ctx = self.teardown_network_ctx().await;
		for bytes in written.iter().copied().filter(|bytes| *bytes > 0) {
			self.charge_sent_bytes(ctx.clone(), bytes);
		}
	}

	/// Whether a frame is the last one the connection may carry.
	///
	/// A close frame ends the conversation: the sink refuses data behind one,
	/// and a client told the connection is over has no reason to read
	/// further. Whoever writes one therefore writes nothing more, which is
	/// what makes a close frame the last thing on the wire without it having
	/// to be the last thing offered to the writer.
	fn ends_the_connection(msg: &Message) -> bool {
		matches!(msg, Message::Close(_))
	}

	/// The payload bytes an outbound frame is charged for.
	///
	/// Ping, Pong and Close carry nothing a tenant is billed for and report
	/// zero, which [`Self::record_sent_bytes`] then short-circuits on.
	fn payload_len(msg: &Message) -> usize {
		match msg {
			Message::Text(msg) => msg.len(),
			Message::Binary(msg) => msg.len(),
			_ => 0,
		}
	}

	/// Charge a frame's payload to the outbound network counters.
	///
	/// Every path that hands a frame to the sink goes through this, because a
	/// frame written but not counted understates the tenant-attributed
	/// counters that chargeback reads. Called after the hand-off succeeds, so
	/// a frame the sink refused is not charged.
	///
	/// `ctx` carries `(namespace, database, user)` from the bound session;
	/// record-access principals collapse to the `<record>` sentinel in
	/// [`NetworkBytesEventCtx`]'s `From<&Session>` impl to keep dimensional
	/// label cardinality bounded.
	async fn record_sent_bytes(&self, bytes: usize) {
		if bytes == 0 {
			return;
		}
		let ctx = self.default_network_ctx().await;
		self.charge_sent_bytes(ctx, bytes);
	}

	/// Emit one outbound byte event against an attribution already in hand.
	///
	/// Split out so the paths that cannot wait for a fresh snapshot still
	/// charge through the same code as the ones that can.
	fn charge_sent_bytes(&self, ctx: NetworkBytesEventCtx, bytes: usize) {
		self.datastore.observer().on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Sent,
				protocol: SessionProtocol::WebSocket,
				bytes: bytes as u64,
			},
			ctx,
		});
	}

	/// The connection's outbound attribution, taken without waiting for the
	/// session lock.
	///
	/// [`Self::default_network_ctx`] waits for a read guard, and a read guard
	/// waits behind any writer: `signin`, `signup`, `authenticate` and
	/// `refresh` each hold the session's write guard across a datastore round
	/// trip, and for password access across the hash verification itself.
	/// Teardown must not wait on that -- [`Self::flush_on_teardown`] bounds
	/// its writes by [`TEARDOWN_FLUSH_GRACE`] precisely because the client it
	/// serves may have stopped reading, and [`graceful_shutdown`] waits for
	/// the connection to leave the WebSocket map, so a request the client
	/// itself issued must not be able to hold shutdown open.
	///
	/// Falls back to the default attribution -- the same one an absent session
	/// yields -- when the guard is unavailable. The bytes are charged either
	/// way: a counter that drops what it wrote is worse than one that labels
	/// the last frames of a closing connection with the default tenant.
	///
	/// [`graceful_shutdown`]: crate::rpc::graceful_shutdown
	async fn teardown_network_ctx(&self) -> NetworkBytesEventCtx {
		if self.datastore.observer().is_noop() {
			return NetworkBytesEventCtx::default();
		}
		match self.get_session(&self.id).await {
			Ok(lock) => lock
				.try_read()
				.map(|session| NetworkBytesEventCtx::from(&*session))
				.unwrap_or_default(),
			Err(_) => NetworkBytesEventCtx::default(),
		}
	}

	/// Whether a teardown write got through, tracing the reason when it did
	/// not. A failure ends the drain: the connection is going away regardless,
	/// and the client it serves may be one that stopped reading.
	fn teardown_progressed<E: fmt::Display>(
		result: Result<Result<(), E>, tokio::time::error::Elapsed>,
	) -> bool {
		match result {
			Ok(Ok(())) => true,
			Ok(Err(err)) => {
				// Output any errors if not a close error
				if err.to_string() != CONN_CLOSED_ERR {
					trace!("WebSocket error while draining on teardown: {err}");
				}
				false
			}
			Err(_) => {
				trace!("Gave up draining a WebSocket that stopped reading");
				false
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
							if let Some(over) = ALLOC.beyond_threshold_by() {
								// Reject the message
								Self::close_socket(Arc::clone(&rpc), over).await;
								// Exit out of the loop
								break;
							}
							// Otherwise spawn and handle the message
							tasks.push(Self::handle_message(&rpc, msg, chn, rec_limit));
						}
						Message::Close(_) => {
							// The close frame RFC 6455 section 5.5.1 requires
							// in response to this one is already queued inside
							// the transport, which read the connection as
							// closing and refuses a second one from us. What
							// writes it out is `write` closing the sink from
							// [`Self::flush_on_teardown`], so tripping the
							// canceller is what sends the reply on its way.
							//
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
			//
			// Guarded because the field values are built eagerly whether or
			// not anything consumes them: `span` is a `debug_span!`, which no
			// subscriber is interested in at the default log level, and
			// `record` on such a span is a no-op that still costs its
			// arguments. Two of these allocate, on a path that runs once per
			// inbound message.
			if !span.is_disabled() {
				span.record("rpc.method", req.method.to_str());
				span.record("otel.name", format!("surrealdb.rpc/{}", req.method));
				span.record(
					"rpc.request_id",
					req.id.as_ref().map(|id| id.to_sql()).unwrap_or_default(),
				);
			}
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
					else if let Some((allocated, threshold)) = ALLOC.beyond_threshold_by() {
						log_memory_refusal("websocket", allocated, threshold);
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
					// Otherwise process the request inline. Either way, the
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
					// `Err(QueryCancelled)` like any other failure. The
					// streaming driver observes the same canceller
					// cooperatively for the same reason.
					//
					// The read loop drains its `FuturesUnordered` of
					// in-flight `handle_message` futures on cancel rather
					// than dropping it (see `read`), so this future is
					// never dropped mid-flight in production.
					//
					// A streaming query is answered with a sequence of frames
					// rather than one response, so it cannot go through the
					// single-response dispatch below; its driver owns the
					// whole exchange, including every failure answer.
					else if req.method == Method::QueryStream {
						let client_session: Option<Uuid> = req.session_id.map(Into::into);
						let session_id = client_session.unwrap_or(rpc.id);
						crate::rpc::streaming::process_query_stream(
							rpc,
							req.id,
							session_id,
							client_session,
							req.txn.map(Into::into),
							req.params,
							chn,
						)
						.await;
					}
					// Otherwise process the request message inline
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
	///
	/// The frame goes on the solicited-output queue, not the notification
	/// one: it answers the request being refused.
	///
	/// `over` is the tracked total and the threshold it exceeded, from the
	/// check that refused the message.
	async fn close_socket(rpc: Arc<Websocket>, over: (usize, usize)) {
		// Log the error as a warning
		log_memory_refusal("websocket", over.0, over.1);
		// Create a custom close frame
		let frame = CloseFrame {
			code: AGAIN,
			reason: SERVER_OVERLOADED.into(),
		};
		// Respond with a close message
		if let Err(err) = rpc.channel.send(Message::Close(Some(frame))).await {
			debug!("WebSocket error when sending close message: {err}");
		};
		// Cancel the WebSocket tasks AND the executor cancel flag so any
		// in-flight queries return early.
		rpc.cancel_all();
	}

	/// Queue a live-query notification on this connection, without waiting.
	///
	/// Reports whether the notification is on its way to the client.
	///
	/// The outbound channel is bounded and shared by every response on the
	/// connection, so a client that has stopped reading fills it. Waiting for
	/// capacity here would park one encoded notification per notification
	/// produced, with nothing bounding how many accumulate: the dispatcher
	/// that calls this keeps receiving from the datastore regardless of
	/// whether any delivery ever completes. So delivery never waits, and a
	/// connection whose queue is full is closed carrying the reason instead.
	/// Closing rather than dropping the notification is what tells the client
	/// it fell behind -- this transport retains nothing, so a notification
	/// dropped here could not be recovered, and a client told nothing would
	/// read the gap as quiescence.
	///
	/// The last slot is held back for that close frame, so a client that has
	/// fallen behind can be told why. Delivering the frame is still
	/// best-effort: another response may take the reserved slot first, and the
	/// teardown has to happen either way.
	pub(crate) fn deliver_notification(&self, response: DbResponse) -> bool {
		// A connection already being torn down takes nothing further. It stays
		// reachable from the live-query registry until its tasks finish, so
		// notifications keep arriving while the write loop drains, and the
		// drain frees the very capacity the check below reads. A frame queued
		// behind the close frame is one the protocol forbids -- the sink
		// refuses data after a close -- and that refusal would end the drain
		// holding the close frame it was there to deliver.
		if self.cancel.is_cancelled() {
			return false;
		}
		// One admission, decided by the queue itself rather than by reading
		// its capacity and sending afterwards: nothing is held back here, so
		// there is no window in which two deliveries both count the same free
		// slot. The queue carries notifications alone, so a full one means
		// this client is not reading them -- not that it has replies
		// outstanding.
		match crate::rpc::response::try_send(response, self.format, &self.notifications) {
			Ok(()) => return true,
			// The connection has already gone away, so there is no teardown
			// left to do and nobody to tell.
			Err(TrySendError::Closed(_)) => return false,
			Err(TrySendError::Full(_)) => {}
		}
		// The close frame's slot is its own and cannot have been filled by
		// anything else, so the only way to lose it is to another delivery
		// that found the queue full at the same moment -- which has queued
		// the same frame this one would have. Whoever claims it logs it.
		let frame = CloseFrame {
			code: AGAIN,
			reason: CLIENT_LAGGING.into(),
		};
		if self.lagging_close.try_send(Message::Close(Some(frame))).is_ok() {
			warn!("Closing a WebSocket whose client stopped reading its notifications");
		}
		// Cancel the WebSocket tasks AND the executor cancel flag so any
		// in-flight queries return early.
		self.cancel_all();
		false
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
		match self.get_session(&self.id).await {
			Ok(lock) => NetworkBytesEventCtx::from(&*lock.read().await),
			Err(_) => NetworkBytesEventCtx::default(),
		}
	}
}

impl RpcProtocol for Websocket {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.datastore
	}

	/// The datastore for this RPC interface as a shared handle.
	fn kvs_arc(&self) -> Arc<Datastore> {
		Arc::clone(&self.datastore)
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
			return Err(surrealdb_rpc::error::session_exists(session_id));
		}
		if self.session_map().len() >= *WEBSOCKET_MAX_ATTACHED_SESSIONS {
			return Err(surrealdb_rpc::error::method_not_allowed(Method::Attach.to_string()));
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
			return Err(surrealdb_rpc::error::invalid_params(
				"Cannot detach the implicit connection session",
			));
		}
		self.del_session(&session_id).await?;
		Ok(DbResult::Other(Value::None))
	}

	// ------------------------------
	// Transactions
	// ------------------------------

	/// Retrieves a transaction by ID
	async fn get_tx(
		&self,
		id: Uuid,
	) -> Result<Arc<surrealdb_datastore::Transaction>, surrealdb_types::Error> {
		debug!("WebSocket get_tx called for transaction {id}");
		self.transactions
			.get(&id)
			.map(|entry| {
				debug!("Transaction {id} found in WebSocket transactions map");
				Arc::clone(&entry.value().1)
			})
			.ok_or_else(|| {
				warn!(
					"Transaction {id} not found in WebSocket transactions map (have {} transactions)",
					self.transactions.len()
				);
				surrealdb_rpc::error::invalid_params("Transaction not found")
			})
	}

	/// Stores a transaction
	async fn set_tx(
		&self,
		id: Uuid,
		tx: Arc<surrealdb_datastore::Transaction>,
	) -> Result<(), surrealdb_types::Error> {
		// Tag the transaction with the connection's implicit default session and
		// reserve a slot so the open-transaction counter stays consistent with a
		// later `commit`/`cancel`/cleanup that releases it.
		//
		// NOTE: this has no callers today. Unlike `begin` it does NOT enforce the
		// per-session cap (it always keys under `self.id` and never checks the
		// limit); if it is ever wired up it should route through the same
		// reservation-with-check that `begin` uses, or it will silently bypass the
		// bound.
		{
			let counter = self.counters.entry(self.id).or_insert_with(|| AtomicUsize::new(0));
			counter.value().fetch_add(1, Ordering::AcqRel);
		}
		self.transactions.insert(id, (self.id, tx));
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
				// We've already lost the in-memory registration and now
				// we've failed to clean up the datastore-side row too, so
				// notifications for `lqid` will be produced and silently
				// discarded by the broker for the lifetime of the
				// datastore. Emit an orphan-LQ counter so operators can
				// alert on this without grepping logs.
				if let Some(obs) = self.state.metrics_observer.as_ref() {
					obs.record_live_query_orphaned(namespace.as_deref(), database.as_deref());
				}
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

	/// Cancels and drops any transactions still open for a session that is
	/// being detached or reset, so a client cannot leak transactions by
	/// abandoning a session without committing or cancelling them.
	async fn cleanup_txns(&self, session_id: &Uuid) {
		self.cleanup_txns_filtered(Some(session_id)).await;
	}

	// ------------------------------
	// Methods for transactions
	// ------------------------------

	/// Begin a new transaction
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		session_id: Uuid,
	) -> Result<DbResult, surrealdb_types::Error> {
		// Reject a `begin` for a session that was never attached (the durable
		// copy, if any, is consulted by `get_session`), matching every other
		// handler's session guard. Without this a client could supply an endless
		// stream of fabricated session ids — each getting its own independent
		// per-session budget and its own `counters` entry — which would both
		// defeat the per-session cap and grow the counter map without bound. The
		// connection's implicit default session (`self.id`) is always registered,
		// so it is exempt from the lookup. Done before the reservation so a
		// rejected `begin` never touches `counters`, keeping its keyspace bounded
		// to `{self.id}` plus the currently-attached sessions.
		if session_id != self.id {
			self.get_session(&session_id).await?;
		}
		// Enforce the open-transaction cap before opening anything. The
		// connection's implicit default session (keyed by `self.id`) is bounded
		// by the per-connection limit; every attached session is bounded
		// independently by the per-session limit. Reserve the slot up front with
		// a `fetch_add` so two concurrent `begin`s on the same session cannot
		// both observe a below-limit count and slip past; the reservation is
		// released again on every failure path below.
		let limit = if session_id == self.id {
			*MAX_TRANSACTIONS_PER_CONNECTION
		} else {
			*MAX_TRANSACTIONS_PER_SESSION
		};
		let prev = {
			let counter = self.counters.entry(session_id).or_insert_with(|| AtomicUsize::new(0));
			counter.value().fetch_add(1, Ordering::AcqRel)
		};
		if prev >= limit {
			// Roll back the reservation and reject: the session already holds the
			// maximum number of concurrently open transactions.
			self.release_txn_slot(&session_id);
			return Err(surrealdb_rpc::error::too_many_transactions());
		}
		// `begin` bypasses the executor (which is where the
		// `Context::done` cancel short-circuit lives), so the cancel
		// handle has to be checked manually. `cancel_aware_transaction`
		// encapsulates the pre-await + post-await + cancel-on-loss
		// pattern; any future RPC method that needs to open its own
		// transaction outside the executor SHOULD route through it.
		let tx = match self.cancel_aware_transaction(TransactionType::Write).await {
			Ok(tx) => tx,
			Err(e) => {
				// Opening the transaction failed, so give the reserved slot back.
				self.release_txn_slot(&session_id);
				return Err(e);
			}
		};
		// Generate a unique transaction ID
		let id = Uuid::now_v7();
		debug!("WebSocket begin: created transaction {id}");
		// Store the transaction in the map, tagged with the owning session so it
		// can be cleaned up when that session is detached or reset.
		self.transactions.insert(id, (session_id, Arc::new(tx)));
		debug!(
			"WebSocket begin: stored transaction {id}, map now has {} transactions",
			self.transactions.len()
		);
		// Close the begin/detach race. RPCs on one connection run concurrently,
		// so a `detach` can interleave with this `begin`: `del_session` removes
		// the session from `session_map` *before* draining its transactions, so a
		// `detach` that ran during the transaction-open await above would have
		// drained the map before this txn was inserted — missing it — while its
		// `remove_if` kept the counter entry alive (it still saw our reservation).
		// That would strand an open transaction and a counter entry under a
		// detached, never-reattached session id; a client rotating fresh ids could
		// grow both maps without bound for the connection's lifetime. Now that the
		// txn is published, re-check the session: if it is gone, undo — drain and
		// cancel it and drop the counter — and reject. `del_session`'s
		// remove-then-drain ordering pairs with this insert-then-recheck so one
		// side always sees the other in both interleavings. `self.id` is never
		// detached (exempt), and `reset` keeps the session attached, so a racing
		// `reset` leaves the txn correctly tracked rather than undone.
		if session_id != self.id && !self.session_map().contains_key(&session_id) {
			self.cleanup_txns_filtered(Some(&session_id)).await;
			return Err(surrealdb_rpc::error::session_not_found(session_id));
		}
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
			return Err(surrealdb_rpc::error::invalid_params("Expected transaction UUID"));
		};

		let txn_id = txn_id.into_inner();

		// Retrieve and remove the transaction from the map
		let Some((_, (session_id, tx))) = self.transactions.remove(&txn_id) else {
			return Err(surrealdb_rpc::error::invalid_params("Transaction not found"));
		};
		// The transaction is no longer open, so free its reserved slot.
		self.release_txn_slot(&session_id);

		// Commit the transaction
		tx.commit().await.map_err(surrealdb_core::rpc::types_error_from_anyhow)?;

		// Return success
		Ok(DbResult::Other(Value::None))
	}

	/// Cancel an in-flight streaming query by its originating request id.
	///
	/// Stops the stream's execution cooperatively: the driver observes the
	/// stop, drives the execution to its cancelled completion so its
	/// transaction is finalised, and still ends the stream with an `End`
	/// frame reflecting how far it got. The registry is per-connection, so a
	/// client can only ever cancel its own streams; a request id with no
	/// stream in flight — including one that just finished — is an error the
	/// caller can disregard.
	async fn query_cancel(&self, params: Array) -> Result<DbResult, surrealdb_types::Error> {
		let mut params = params.into_vec();
		let (Some(id), None) = (params.pop(), params.pop()) else {
			return Err(invalid_params("Expected the request id of the streaming query to cancel"));
		};
		let Some(handle) = self.streams.get(&id.to_sql()) else {
			return Err(stream_not_found());
		};
		handle.stop();
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
			return Err(surrealdb_rpc::error::invalid_params("Expected transaction UUID"));
		};

		let txn_id = txn_id.into_inner();

		// Retrieve and remove the transaction from the map
		let Some((_, (session_id, tx))) = self.transactions.remove(&txn_id) else {
			return Err(surrealdb_rpc::error::invalid_params("Transaction not found"));
		};
		// The transaction is no longer open, so free its reserved slot.
		self.release_txn_slot(&session_id);

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
	/// without going through a cancellable `Datastore::run` MUST route
	/// through here. The executor's `Context::done` short-circuit
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
	) -> Result<Transaction, surrealdb_types::Error> {
		if self.cancel.is_cancelled() {
			return Err(TypesError::internal(REQUEST_CANCELLED.to_string()));
		}
		let tx = self
			.kvs()
			.transaction(ty)
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

	/// Release one reserved open-transaction slot for a session, undoing the
	/// `fetch_add` reservation made in [`Self::begin`] / [`Self::set_tx`]. Called
	/// whenever a reservation is given back — an open transaction leaving the map
	/// (`commit`, `cancel`, cleanup), or a `begin` that reserved then bailed
	/// (over the limit, or the transaction failed to open). A missing counter
	/// entry is treated as already-released, so this can never underflow.
	///
	/// Prunes the entry once it drains to zero (for any session other than the
	/// connection's default `self.id`, whose counter is intentionally retained),
	/// so no release path can strand a zeroed counter entry under a
	/// detached/rotated session id. `remove_if` only fires while the count is
	/// still zero, so a concurrent `begin` that re-reserved the slot keeps it.
	fn release_txn_slot(&self, session_id: &Uuid) {
		let Some(counter) = self.counters.get(session_id) else {
			return;
		};
		let prev = counter.value().fetch_sub(1, Ordering::AcqRel);
		// Drop the shard guard before touching the map again.
		drop(counter);
		if prev == 1 && *session_id != self.id {
			self.counters.remove_if(session_id, |_, c| c.load(Ordering::Acquire) == 0);
		}
	}

	/// Shared body for [`Self::cleanup_all_txns`] and the per-session
	/// [`RpcProtocol::cleanup_txns`].
	///
	/// `session_filter` narrows the drain to a single session id when `Some`
	/// (session detach / reset), or drains every client-managed transaction on
	/// this WebSocket when `None` (the connection-close path). Each drained
	/// transaction is cancelled and its reserved slot released.
	///
	/// Drains transactions opened via the explicit `begin` RPC whose
	/// `commit` / `cancel` never arrived. Without this drain, a client that
	/// calls `begin` and then disconnects (or detaches the owning session)
	/// would leave the `Arc<Transaction>` in the map until the `Websocket`
	/// itself is dropped — at which point `Transactor::Drop` would emit "A
	/// transaction was dropped without being committed or cancelled".
	async fn cleanup_txns_filtered(&self, session_filter: Option<&Uuid>) {
		// Drain the matching entries atomically into a local vec so we can
		// release each DashMap shard lock before awaiting `tx.cancel()`. Holding
		// shard locks across `.await` would risk a deadlock against any other
		// code path that touches the map and then awaits.
		let mut drained: Vec<(Uuid, Uuid, Arc<Transaction>)> = Vec::new();
		self.transactions.retain(|tid, val| {
			let sid = val.0;
			if let Some(want) = session_filter
				&& sid != *want
			{
				return true;
			}
			drained.push((*tid, sid, Arc::clone(&val.1)));
			false
		});
		if !drained.is_empty() {
			trace!(
				"Cancelling {} client-managed transaction(s) on WebSocket {}",
				drained.len(),
				self.id,
			);
			for (tid, sid, tx) in drained {
				// Free the reserved slot first so the counter stays balanced even
				// if the cancel below errors.
				self.release_txn_slot(&sid);
				if let Err(err) = tx.cancel().await {
					error!("Error cancelling transaction {tid} during session cleanup: {err}",);
				}
			}
		}
		// Safety net: `release_txn_slot` already prunes an attached session's
		// counter entry as it drains to zero, but sweep once more here to catch a
		// stray zeroed entry that had no transaction to drain (e.g. left by a
		// transiently-racing reservation). The connection's default-session
		// counter (keyed by `self.id`) is intentionally retained.
		if let Some(want) = session_filter
			&& *want != self.id
		{
			self.counters.remove_if(want, |_, c| c.load(Ordering::Acquire) == 0);
		}
	}

	/// Cancel every client-managed transaction left in `self.transactions`.
	///
	/// Invoked from `serve()` at WS teardown (alongside `cleanup_all_lqs`) to
	/// drain all transactions regardless of the owning session.
	async fn cleanup_all_txns(&self) {
		self.cleanup_txns_filtered(None).await;
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use surrealdb_core::kvs::Datastore;
	use surrealdb_iam::{Auth, Role};
	use surrealdb_observe::{
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

	/// The receiving ends of a connection's outbound queues.
	struct Queues {
		replies: Receiver<Message>,
		notifications: Receiver<Message>,
		lagging_close: Receiver<Message>,
	}

	impl Queues {
		/// The queues as the write loop reads them, through the same
		/// construction it uses, so a test sees production's ordering.
		fn merged(self) -> Outbound {
			Websocket::outbound_queues(self.replies, self.notifications, self.lagging_close)
		}
	}

	async fn ws_with_observer(observer: Option<Arc<dyn ExecutionObserver>>) -> Arc<Websocket> {
		let mut builder = Datastore::builder();
		if let Some(obs) = observer {
			builder = builder.with_observer(obs);
		}
		let ds = builder.build_with_path("memory").await.unwrap();
		let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
		let id = Uuid::new_v4();
		let (tx, _rx) = channel::<Message>(8);
		let (notifier, _notify_rx) = channel::<Message>(8);
		let (closer, _close_rx) = channel::<Message>(1);
		Arc::new(Websocket {
			id,
			format: Format::Json,
			state,
			datastore: ds,
			sessions: HashMap::new(),
			transactions: DashMap::new(),
			counters: DashMap::new(),
			streams: DashMap::new(),
			stream_slots: AtomicUsize::new(0),
			shutdown: CancellationToken::new(),
			cancel: surrealdb_core::ctx::CancelHandle::new(),
			channel: tx,
			notifications: notifier,
			lagging_close: closer,
		})
	}

	/// A bare connection whose reply and notification queues each have the
	/// given capacity, handed back with every receiving end so a test controls
	/// how much room is left in each.
	async fn ws_with_channel(capacity: usize) -> (Arc<Websocket>, Queues) {
		ws_with_channel_and_observer(capacity, None).await
	}

	/// As [`ws_with_channel`], with an observer installed so a test can see
	/// the network byte events the connection emits.
	async fn ws_with_channel_and_observer(
		capacity: usize,
		observer: Option<Arc<dyn ExecutionObserver>>,
	) -> (Arc<Websocket>, Queues) {
		let mut builder = Datastore::builder();
		if let Some(obs) = observer {
			builder = builder.with_observer(obs);
		}
		let ds = builder.build_with_path("memory").await.unwrap();
		let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
		let (tx, rx) = channel::<Message>(capacity);
		let (notifier, notify_rx) = channel::<Message>(capacity);
		let (closer, close_rx) = channel::<Message>(1);
		let rpc = Arc::new(Websocket {
			id: Uuid::new_v4(),
			format: Format::Json,
			state,
			datastore: ds,
			sessions: HashMap::new(),
			transactions: DashMap::new(),
			counters: DashMap::new(),
			streams: DashMap::new(),
			stream_slots: AtomicUsize::new(0),
			shutdown: CancellationToken::new(),
			cancel: surrealdb_core::ctx::CancelHandle::new(),
			channel: tx,
			notifications: notifier,
			lagging_close: closer,
		});
		(
			rpc,
			Queues {
				replies: rx,
				notifications: notify_rx,
				lagging_close: close_rx,
			},
		)
	}

	/// Any response will do: `deliver_notification` is about how a response
	/// reaches the queue, not what it carries.
	fn deliverable() -> DbResponse {
		DbResponse::success(None, None, DbResult::Other(Value::None))
	}

	/// A client that has stopped reading fills the connection's queue. The
	/// notification cannot be dropped silently -- this transport retains
	/// nothing, so the client would read the gap as quiescence -- so the
	/// connection is closed carrying the reason, in the slot held back for it.
	///
	/// That the delivery does not *wait* for room is enforced by the compiler
	/// rather than here: `deliver_notification` is a plain `fn`, so it has no
	/// await to park on, which is what bounds the dispatcher's in-flight set.
	#[tokio::test]
	async fn a_notification_for_a_client_that_stopped_reading_closes_the_connection() {
		// A notification queue with no room left, which is what says this
		// client has stopped reading. The close frame's slot is separate, so
		// filling this one does not consume it.
		let (rpc, queues) = ws_with_channel(2).await;
		let mut frames = queues.merged();
		while rpc.notifications.capacity() > 0 {
			rpc.notifications.try_send(Message::Text("queued".into())).expect("a free slot");
		}

		let delivered = rpc.deliver_notification(deliverable());

		assert!(!delivered, "a notification that could not be queued is not reported as delivered");
		assert!(rpc.cancel.token().is_cancelled(), "the connection is torn down");
		// The close frame is queued, carrying the reason. That it is the last
		// thing to reach the wire is the writer's doing rather than the
		// queues' -- see
		// [`the_close_frame_for_a_lagging_client_reaches_the_socket`].
		let mut queued = Vec::new();
		while let Some(Some(msg)) = frames.next().now_or_never() {
			queued.push(msg);
		}
		let closes: Vec<&CloseFrame> = queued
			.iter()
			.filter_map(|msg| match msg {
				Message::Close(frame) => frame.as_ref(),
				_ => None,
			})
			.collect();
		let [frame] = closes.as_slice() else {
			panic!("exactly one close frame is queued, got {queued:?}");
		};
		assert_eq!(frame.code, AGAIN, "the client is invited to reconnect");
		assert_eq!(frame.reason.as_str(), CLIENT_LAGGING, "and told why it was closed");
	}

	/// Queueing the close frame is not enough on its own: the write loop polls
	/// the connection canceller ahead of the response channel, so what carries
	/// the frame to the client is the drain the loop performs on its way out.
	/// Without it a lagging client would be left inferring an unexplained
	/// disconnect in place of the reason it was given.
	#[tokio::test]
	async fn the_close_frame_for_a_lagging_client_reaches_the_socket() {
		// A notification queue with no room left, which is what trips the
		// lagging path.
		let (rpc, queues) = ws_with_channel(2).await;
		while rpc.notifications.capacity() > 0 {
			rpc.notifications.try_send(Message::Text("queued".into())).expect("a free slot");
		}

		// Trip the lagging path before the writer runs, so the canceller is
		// already set when it takes its first look -- the ordering that drops
		// the frame if the drain is missing.
		assert!(!rpc.deliver_notification(deliverable()), "the client is closed, not served");

		// Drive the real write loop over a sink that records what it wrote.
		let (sink, mut written) = futures::channel::mpsc::unbounded::<Message>();
		Websocket::write(
			Arc::clone(&rpc),
			sink,
			queues.replies,
			queues.notifications,
			queues.lagging_close,
		)
		.await;

		let mut messages = Vec::new();
		while let Ok(Some(msg)) = written.try_next() {
			messages.push(msg);
		}
		let Some(Message::Close(Some(frame))) = messages.pop() else {
			panic!("the close frame is the last thing written, got {messages:?}");
		};
		assert_eq!(frame.code, AGAIN, "the client is invited to reconnect");
		assert_eq!(frame.reason.as_str(), CLIENT_LAGGING, "and told why it was closed");
		assert!(
			messages.iter().all(|msg| matches!(msg, Message::Text(_))),
			"the notifications queued ahead of it go out too, got {messages:?}"
		);
	}

	/// A backlog of replies cannot hold the close frame back.
	///
	/// The connection this serves is by definition one with work outstanding:
	/// cancelling it drains every in-flight handler, and each of those still
	/// answers on the reply queue, so replies keep arriving while teardown
	/// runs. Offering the close frame only once the other queues had run dry
	/// would let those replies keep it waiting until the flush grace period
	/// expired, and the client would be disconnected with nothing said --
	/// which is the whole thing the frame exists to prevent. What keeps it
	/// last on the wire is that the writer stops after it, so it can be
	/// offered as early as fairness allows.
	#[tokio::test]
	async fn a_backlog_of_replies_does_not_starve_the_close_frame() {
		let (rpc, queues) = ws_with_channel(8).await;
		// Both other queues full: replies the client is waiting on, and the
		// unread notifications that make it a lagging one.
		while rpc.channel.capacity() > 0 {
			rpc.channel.try_send(Message::Text("reply".into())).expect("a free slot");
		}
		while rpc.notifications.capacity() > 0 {
			rpc.notifications.try_send(Message::Text("unread".into())).expect("a free slot");
		}
		assert!(!rpc.deliver_notification(deliverable()), "the client is closed, not served");

		let (sink, mut written) = futures::channel::mpsc::unbounded::<Message>();
		Websocket::write(
			Arc::clone(&rpc),
			sink,
			queues.replies,
			queues.notifications,
			queues.lagging_close,
		)
		.await;

		let mut messages = Vec::new();
		while let Ok(Some(msg)) = written.try_next() {
			messages.push(msg);
		}
		let Some(Message::Close(Some(frame))) = messages.pop() else {
			panic!("the close frame is the last thing written, got {messages:?}");
		};
		assert_eq!(frame.reason.as_str(), CLIENT_LAGGING, "and it carries the reason");
		// Sixteen frames were queued ahead of it. Alternation lets at most one
		// per queue go first, so a close frame waiting on the backlog to clear
		// is what this rules out -- not the exact count.
		assert!(
			messages.len() <= 3,
			"the close frame does not wait for the backlog, got {} ahead of it: {messages:?}",
			messages.len()
		);
	}

	/// A sink that behaves as the transport does once it has read the peer's
	/// close frame: it holds the reply it owes, refuses any further frame from
	/// this side, and writes that reply out only when the socket is closed.
	///
	/// A permissive sink cannot express this, and it is the shape that matters
	/// -- the reply to a client's close frame is never a frame this code hands
	/// over, so nothing but the close reaches it.
	#[derive(Clone, Default)]
	struct ClosingSink {
		/// What reached the wire.
		written: Arc<Mutex<Vec<Message>>>,
		/// The reply the transport queued for itself, still unwritten.
		reply_held: Arc<Mutex<bool>>,
	}

	impl ClosingSink {
		fn after_peer_close() -> Self {
			Self {
				written: Arc::default(),
				reply_held: Arc::new(Mutex::new(true)),
			}
		}
	}

	impl Sink<Message> for ClosingSink {
		// Any `Display` error will do; the refusal is what matters.
		type Error = std::fmt::Error;

		fn poll_ready(
			self: std::pin::Pin<&mut Self>,
			_: &mut std::task::Context<'_>,
		) -> std::task::Poll<Result<(), Self::Error>> {
			std::task::Poll::Ready(Ok(()))
		}

		fn start_send(self: std::pin::Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
			// Nothing may follow the peer's close frame.
			Err(std::fmt::Error)
		}

		fn poll_flush(
			self: std::pin::Pin<&mut Self>,
			_: &mut std::task::Context<'_>,
		) -> std::task::Poll<Result<(), Self::Error>> {
			// A flush alone does not release the reply.
			std::task::Poll::Ready(Ok(()))
		}

		fn poll_close(
			self: std::pin::Pin<&mut Self>,
			_: &mut std::task::Context<'_>,
		) -> std::task::Poll<Result<(), Self::Error>> {
			if std::mem::take(&mut *self.reply_held.lock().unwrap()) {
				self.written.lock().unwrap().push(Message::Close(None));
			}
			std::task::Poll::Ready(Ok(()))
		}
	}

	/// The reply owed to a close frame the peer sent is held inside the
	/// transport, which refuses any further frame from this side and writes
	/// that reply out only when the socket closes. Teardown that merely
	/// flushed would leave the peer with a transport-level reset in place of
	/// the close handshake RFC 6455 section 5.5.1 requires.
	#[tokio::test]
	async fn teardown_closes_the_socket_so_the_transports_reply_goes_out() {
		let (rpc, queues) = ws_with_channel(4).await;
		let mut frames = queues.merged();
		let mut sink = ClosingSink::after_peer_close();
		let view = sink.clone();

		// A response queued behind the peer's close frame can no longer be
		// sent, and being refused must not stop the handshake.
		rpc.channel.try_send(Message::Text("refused".into())).expect("a free slot");

		rpc.flush_on_teardown(&mut sink, &mut frames, false).await;

		let written = view.written.lock().unwrap().clone();
		assert_eq!(
			written,
			[Message::Close(None)],
			"the reply the transport was holding did not reach the wire",
		);
	}

	/// A sink that keeps what it has been handed apart from what it has been
	/// told to flush -- the distinction a buffered connection turns on, and
	/// which a plain recording sink cannot express.
	#[derive(Clone, Default)]
	struct BufferedSink {
		fed: Arc<Mutex<Vec<Message>>>,
		flushed: Arc<Mutex<Vec<Message>>>,
	}

	impl Sink<Message> for BufferedSink {
		// Any `Display` error will do; nothing here fails.
		type Error = std::fmt::Error;

		fn poll_ready(
			self: std::pin::Pin<&mut Self>,
			_: &mut std::task::Context<'_>,
		) -> std::task::Poll<Result<(), Self::Error>> {
			std::task::Poll::Ready(Ok(()))
		}

		fn start_send(self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
			self.fed.lock().unwrap().push(item);
			Ok(())
		}

		fn poll_flush(
			self: std::pin::Pin<&mut Self>,
			_: &mut std::task::Context<'_>,
		) -> std::task::Poll<Result<(), Self::Error>> {
			let drained: Vec<Message> = self.fed.lock().unwrap().drain(..).collect();
			self.flushed.lock().unwrap().extend(drained);
			std::task::Poll::Ready(Ok(()))
		}

		fn poll_close(
			self: std::pin::Pin<&mut Self>,
			cx: &mut std::task::Context<'_>,
		) -> std::task::Poll<Result<(), Self::Error>> {
			self.poll_flush(cx)
		}
	}

	/// In buffered mode the write loop feeds without flushing and leans on its
	/// periodic flush, so a frame it has taken from the channel is sitting in
	/// the sink rather than on the wire. Teardown therefore has to flush the
	/// sink, not merely drain whatever is still queued -- a frame in the sink
	/// has reached the client no more than one left in the channel had.
	#[tokio::test]
	async fn teardown_flushes_a_frame_the_write_loop_had_already_fed() {
		let (rpc, queues) = ws_with_channel(4).await;
		let mut frames = queues.merged();
		let mut sink = BufferedSink::default();
		let view = sink.clone();

		// Reproduce the handover state: the frame is out of the channel and
		// fed into the sink, unflushed, with nothing left queued behind it.
		rpc.channel.try_send(Message::Close(None)).expect("a free slot");
		let handed_over = frames.next().await.expect("the frame leaves the channel");
		SinkExt::feed(&mut sink, handed_over).await.expect("the sink takes it");
		assert!(view.flushed.lock().unwrap().is_empty(), "nothing has reached the wire yet");

		rpc.flush_on_teardown(&mut sink, &mut frames, false).await;

		assert_eq!(
			view.flushed.lock().unwrap().len(),
			1,
			"the fed frame is flushed even though the channel had nothing left"
		);
	}

	/// A frame the drain writes has left the machine, so it is charged like
	/// any other. The outbound counters are tenant-attributed and feed
	/// chargeback, so a frame delivered but uncounted understates them --
	/// silently, and in the customer's favour.
	#[tokio::test]
	async fn the_teardown_drain_charges_the_bytes_it_writes() {
		let observer = Arc::new(CapturingObserver::default());
		let (rpc, queues) = ws_with_channel_and_observer(
			4,
			Some(Arc::clone(&observer) as Arc<dyn ExecutionObserver>),
		)
		.await;
		let mut frames = queues.merged();
		// The session the byte context is attributed to.
		rpc.set_session(rpc.id, Arc::new(RwLock::new(Session::default())));
		// A payload-carrying response left queued when teardown began, plus
		// the close frame behind it, which carries nothing chargeable.
		rpc.channel.try_send(Message::Text("payload".into())).expect("a free slot");
		rpc.channel.try_send(Message::Close(None)).expect("a free slot");

		let mut sink = BufferedSink::default();
		rpc.flush_on_teardown(&mut sink, &mut frames, false).await;

		let charged: u64 = observer
			.events
			.lock()
			.unwrap()
			.iter()
			.filter(|event| matches!(event.safe.direction, NetworkDirection::Sent))
			.map(|event| event.safe.bytes)
			.sum();
		assert_eq!(
			charged,
			"payload".len() as u64,
			"the drained payload is charged, and the close frame adds nothing"
		);
	}

	/// `signin`, `signup`, `authenticate` and `refresh` hold the session's
	/// write guard across a datastore round trip -- and, for password access,
	/// across the hash verification. Teardown reads the same session for byte
	/// attribution, so waiting for that guard would let a request a client
	/// issued itself decide how long its connection takes to go away, and
	/// `graceful_shutdown` waits for exactly that.
	///
	/// The frames still have to be written and still have to be charged; only
	/// the tenant labels degrade to the default when the guard is unavailable.
	#[tokio::test]
	async fn teardown_does_not_wait_for_a_locked_session() {
		let observer = Arc::new(CapturingObserver::default());
		let (rpc, queues) = ws_with_channel_and_observer(
			4,
			Some(Arc::clone(&observer) as Arc<dyn ExecutionObserver>),
		)
		.await;
		let mut frames = queues.merged();
		let session = Arc::new(RwLock::new(Session::default()));
		rpc.set_session(rpc.id, Arc::clone(&session));
		// The state an in-flight authentication leaves the session in, held
		// for longer than teardown may wait.
		let held = session.write().await;
		rpc.channel.try_send(Message::Text("payload".into())).expect("a free slot");
		rpc.channel.try_send(Message::Close(None)).expect("a free slot");

		let mut sink = BufferedSink::default();
		let view = sink.clone();
		// Bounded by the drain's own grace period: teardown has no reason to
		// take longer than the writes it is there to make, and without this
		// bound the assertion below would hang rather than fail.
		let finished = tokio::time::timeout(
			TEARDOWN_FLUSH_GRACE,
			rpc.flush_on_teardown(&mut sink, &mut frames, false),
		)
		.await;

		assert!(finished.is_ok(), "teardown must not wait on the session lock");
		assert_eq!(
			view.flushed.lock().unwrap().len(),
			2,
			"both queued frames still reach the wire"
		);
		let charged: Vec<(u64, Option<String>)> = observer
			.events
			.lock()
			.unwrap()
			.iter()
			.filter(|event| matches!(event.safe.direction, NetworkDirection::Sent))
			.map(|event| (event.safe.bytes, event.ctx.namespace.clone()))
			.collect();
		assert_eq!(
			charged,
			vec![("payload".len() as u64, None)],
			"the payload is charged with the default attribution, not dropped"
		);
		drop(held);
	}

	/// The smallest queue an operator can configure still delivers. One slot
	/// is held back for a close frame, so the floor is what leaves a slot
	/// beside it: were the reservation able to consume the only slot, every
	/// notification would close the connection instead of reaching it, however
	/// promptly the client was reading.
	#[tokio::test]
	async fn a_notification_fits_the_smallest_configured_queue() {
		let (rpc, queues) = ws_with_channel(crate::cnf::WEBSOCKET_RESPONSE_CHANNEL_FLOOR).await;
		let mut frames = queues.merged();

		assert!(rpc.deliver_notification(deliverable()), "the notification is queued");

		assert!(!rpc.cancel.token().is_cancelled(), "a client that is reading is not closed");
		assert!(matches!(frames.next().await, Some(Message::Text(_))), "the notification");
	}

	/// A client's own replies must not be able to look like notification lag.
	///
	/// The two share a socket but not a queue, because occupancy is what
	/// decides a client has stopped reading its notifications, and a client
	/// that pipelines requests fills a reply queue as fast as the server can
	/// answer -- while reading perfectly well. Charging that against the
	/// notification budget would close a healthy connection and cancel its
	/// in-flight work, and at the configured minimum size a single
	/// outstanding reply would do it.
	#[tokio::test]
	async fn pipelined_replies_are_not_notification_lag() {
		// The reply queue is full to its last slot -- the state a client that
		// pipelines requests puts it in -- while the notification queue is
		// untouched.
		let (rpc, queues) = ws_with_channel(crate::cnf::WEBSOCKET_RESPONSE_CHANNEL_FLOOR).await;
		let mut frames = queues.merged();
		while rpc.channel.capacity() > 0 {
			rpc.channel.try_send(Message::Text("reply".into())).expect("a free slot");
		}

		assert!(rpc.deliver_notification(deliverable()), "the notification is queued");

		assert!(
			!rpc.cancel.token().is_cancelled(),
			"a client with replies outstanding has not stopped reading its notifications"
		);
		let mut seen = Vec::new();
		while let Some(Some(msg)) = frames.next().now_or_never() {
			seen.push(msg);
		}
		assert_eq!(seen.len(), 3, "both replies and the notification are queued: {seen:?}");
		assert!(
			!seen.iter().any(|msg| matches!(msg, Message::Close(_))),
			"nothing closed the connection: {seen:?}"
		);
	}

	/// However many deliveries find the notification queue full at once, the
	/// client is told why.
	///
	/// `deliver_notification` is a plain `fn`, so two of them cannot interleave
	/// inside one dispatcher task -- but `spawn_notifications` is public and
	/// callable more than once, and each call drives deliveries for the same
	/// connection from a task of its own. A close frame held back inside the
	/// notification queue was reserved only by convention: two deliveries
	/// could both read the same free slot, fill it between them, and the next
	/// one would cancel the connection with no room left to say why. The
	/// frame's own slot cannot be filled by anything but the frame.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn concurrent_lagging_deliveries_still_tell_the_client_why() {
		for _ in 0..500 {
			let (rpc, queues) = ws_with_channel(crate::cnf::WEBSOCKET_RESPONSE_CHANNEL_FLOOR).await;
			let mut frames = queues.merged();
			// Full, so every delivery below takes the lagging path.
			while rpc.notifications.capacity() > 0 {
				rpc.notifications.try_send(Message::Text("unread".into())).expect("a free slot");
			}

			let racers: Vec<_> = (0..4)
				.map(|_| {
					let rpc = Arc::clone(&rpc);
					tokio::spawn(async move { rpc.deliver_notification(deliverable()) })
				})
				.collect();
			for racer in racers {
				assert!(!racer.await.expect("the delivery ran"), "none of them is delivered");
			}

			assert!(rpc.cancel.token().is_cancelled(), "the connection is torn down");
			let mut closes = 0;
			while let Some(Some(msg)) = frames.next().now_or_never() {
				if matches!(msg, Message::Close(Some(ref frame)) if frame.code == AGAIN) {
					closes += 1;
				}
			}
			assert_eq!(closes, 1, "exactly one close frame, carrying the reason");
		}
	}

	/// A connection already being torn down takes no further notifications,
	/// however much room its queue appears to have.
	///
	/// It stays reachable from the live-query registry until its tasks
	/// finish, so notifications keep arriving while the write loop drains --
	/// and the drain frees the capacity a naive check would read as room. The
	/// frame would land behind the close frame, where the protocol forbids
	/// it, and the sink's refusal would end the drain still holding the close
	/// frame it existed to deliver.
	#[tokio::test]
	async fn a_notification_after_teardown_began_is_refused() {
		// Plenty of room, so capacity is not what refuses it.
		let (rpc, queues) = ws_with_channel(8).await;
		let mut frames = queues.merged();
		rpc.cancel_all();

		assert!(!rpc.deliver_notification(deliverable()), "the notification is refused");

		assert!(
			frames.next().now_or_never().is_none(),
			"nothing was queued behind a connection that is closing"
		);
	}

	/// The ordinary case: room in the queue, so the notification is queued and
	/// the connection is left alone.
	#[tokio::test]
	async fn a_notification_for_a_reading_client_is_queued() {
		let (rpc, queues) = ws_with_channel(8).await;
		let mut frames = queues.merged();

		assert!(rpc.deliver_notification(deliverable()), "the notification is queued");

		assert!(!rpc.cancel.token().is_cancelled(), "the connection is left alone");
		assert!(matches!(frames.next().await, Some(Message::Text(_))), "the notification");
	}

	/// A connection whose receiver is gone is already over. There is nobody to
	/// tell and no teardown left to do, so this reports the notification
	/// undelivered and does nothing else -- notably it does not fire the
	/// canceller, which the departing write loop has already handled.
	#[tokio::test]
	async fn a_notification_for_a_departed_client_is_dropped_quietly() {
		let (rpc, queues) = ws_with_channel(8).await;
		let frames = queues.merged();
		drop(frames);

		assert!(!rpc.deliver_notification(deliverable()), "nothing was queued");

		assert!(!rpc.cancel.token().is_cancelled(), "no teardown of a connection already gone");
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
		use surrealdb_rpc::capabilities::Capabilities;

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
			let ds = Datastore::builder()
				.with_capabilities(Capabilities::all())
				.with_observer(Arc::clone(&observer) as Arc<dyn ExecutionObserver>)
				.build_with_path("memory")
				.await
				.unwrap();
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
			let (chn_notify, _chn_notify_rx) = channel::<Message>(8);
			let (chn_close, _chn_close_rx) = channel::<Message>(1);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state,
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				counters: DashMap::new(),
				streams: DashMap::new(),
				stream_slots: AtomicUsize::new(0),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
				notifications: chn_notify,
				lagging_close: chn_close,
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
		use surrealdb_rpc::capabilities::Capabilities;

		with_big_stack(|| async {
			let ds = Datastore::builder()
				.with_capabilities(Capabilities::all())
				.build_with_path("memory")
				.await
				.unwrap();
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
			let (chn_notify, _chn_notify_rx) = channel::<Message>(8);
			let (chn_close, _chn_close_rx) = channel::<Message>(1);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state: Arc::clone(&state),
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				counters: DashMap::new(),
				streams: DashMap::new(),
				stream_slots: AtomicUsize::new(0),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
				notifications: chn_notify,
				lagging_close: chn_close,
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
		use surrealdb_rpc::capabilities::Capabilities;

		with_big_stack(|| async {
			let ds = Datastore::builder()
				.with_capabilities(Capabilities::all())
				.build_with_path("memory")
				.await
				.unwrap();
			let owner = Session::owner();
			ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
			let owner_ns = owner.clone().with_ns("test");
			ds.execute("DEFINE DB `test`", &owner_ns, None).await.unwrap();

			let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
			let id = Uuid::new_v4();
			let (chn_internal, _chn_internal_rx) = channel::<Message>(8);
			let (chn_notify, _chn_notify_rx) = channel::<Message>(8);
			let (chn_close, _chn_close_rx) = channel::<Message>(1);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state: Arc::clone(&state),
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				counters: DashMap::new(),
				streams: DashMap::new(),
				stream_slots: AtomicUsize::new(0),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
				notifications: chn_notify,
				lagging_close: chn_close,
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

	/// A WebSocket connection whose live-query notifications are delivered by
	/// the same dispatcher the server runs, so a `Killed` notification reaches
	/// the transport exactly as it does in production.
	struct LiveHarness {
		state: Arc<RpcState>,
		rpc: Arc<Websocket>,
		/// Frames the dispatcher sends to the connection.
		/// The notification queue's receiving end: what the dispatcher writes
		/// to, and so what this harness reads.
		frames: Receiver<Message>,
		/// The reply queue's receiving end, held open and otherwise unused.
		_replies: Receiver<Message>,
		/// The close frame's slot, held open and otherwise unused.
		_lagging_close: Receiver<Message>,
		canceller: CancellationToken,
		dispatcher: tokio::task::JoinHandle<()>,
	}

	impl LiveHarness {
		async fn new() -> Self {
			use surrealdb_rpc::capabilities::Capabilities;

			let (notify_tx, notify_rx) = surrealdb_core::channel::bounded(100);
			let ds = Datastore::builder()
				.with_capabilities(Capabilities::all())
				.with_notify(notify_tx)
				.build_with_path("memory")
				.await
				.unwrap();
			let owner = Session::owner();
			ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
			let owner_ns = owner.clone().with_ns("test");
			ds.execute("DEFINE DB `test`", &owner_ns, None).await.unwrap();
			let setup = Session::owner().with_ns("test").with_db("test");
			ds.execute("CREATE foo SET x = 1", &setup, None).await.unwrap();

			let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
			let canceller = CancellationToken::new();
			let dispatcher = tokio::spawn(crate::rpc::notifications(
				notify_rx,
				Arc::clone(&state),
				canceller.clone(),
			));

			let (notifier, notify_frames) = channel::<Message>(8);
			// Held so the other queues stay open; this harness watches only
			// what the dispatcher sends unprompted.
			let (closer, close_frames) = channel::<Message>(1);
			let (channel, replies) = channel::<Message>(8);
			let rpc = Arc::new(Websocket {
				id: Uuid::new_v4(),
				format: Format::Json,
				state: Arc::clone(&state),
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				counters: DashMap::new(),
				streams: DashMap::new(),
				stream_slots: AtomicUsize::new(0),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel,
				notifications: notifier,
				lagging_close: closer,
			});
			// The dispatcher delivers to connected WebSockets only.
			state.web_sockets.write().await.insert(rpc.id, Arc::clone(&rpc));
			// LIVE queries require a realtime-enabled session.
			let session = Session::owner().with_ns("test").with_db("test").with_rt(true);
			rpc.set_session(rpc.id, Arc::new(RwLock::new(session)));

			Self {
				state,
				rpc,
				frames: notify_frames,
				_replies: replies,
				_lagging_close: close_frames,
				canceller,
				dispatcher,
			}
		}

		/// Runs `sql` through the `query` RPC and returns the decoded response.
		async fn query(&self, id: &str, sql: &str) -> serde_json::Value {
			let body = serde_json::json!({
				"id": id,
				"method": "query",
				"params": [sql],
			});
			let (responses, mut response) = channel::<Message>(8);
			Websocket::handle_message(
				&self.rpc,
				Message::Text(body.to_string().into()),
				responses,
				1024,
			)
			.await;
			let message = response.recv().await.expect("response sent over channel");
			let text = match message {
				Message::Text(t) => t.to_string(),
				other => panic!("expected Text response from Json format, got {other:?}"),
			};
			serde_json::from_str(&text).unwrap_or_else(|e| panic!("JSON response ({e}): {text}"))
		}

		/// Registers a LIVE query and returns the id the transport recorded.
		async fn register_live(&self) -> Uuid {
			let response = self.query("live", "LIVE SELECT * FROM foo;").await;
			assert_eq!(
				response["result"][0]["status"], "OK",
				"LIVE SELECT did not succeed: {response}",
			);
			let registered = self.registrations().await;
			assert_eq!(
				registered.len(),
				1,
				"LIVE SELECT did not register exactly one live query: {registered:?}",
			);
			registered[0]
		}

		/// The live queries registered against this connection.
		async fn registrations(&self) -> Vec<Uuid> {
			self.state
				.live_queries
				.read()
				.await
				.iter()
				.filter_map(|(lqid, entry)| (entry.websocket_id == self.rpc.id).then_some(*lqid))
				.collect()
		}

		/// Awaits the next frame the dispatcher sends to the connection.
		async fn next_frame(&mut self) -> String {
			let message = tokio::time::timeout(Duration::from_secs(10), self.frames.recv())
				.await
				.expect("notification dispatched within 10s")
				.expect("notification channel open");
			match message {
				Message::Text(t) => t.to_string(),
				other => panic!("expected Text notification from Json format, got {other:?}"),
			}
		}

		/// Waits for `lqid` to leave the registry, then reports what is left
		/// against this connection.
		async fn registrations_after_unregistering(&self, lqid: &Uuid) -> Vec<Uuid> {
			// The dispatcher drops the entry after handing the frame to the
			// connection's channel, so give it a moment to get there.
			for _ in 0..100 {
				if !self.state.live_queries.read().await.contains_key(lqid) {
					break;
				}
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
			self.registrations().await
		}

		async fn shutdown(self) {
			self.canceller.cancel();
			let _ = self.dispatcher.await;
		}
	}

	/// A `KILL` statement must drop the live query's WebSocket registration.
	///
	/// A `KILL` statement evaluates to `NONE`, so no result value carries the
	/// id it killed. The `Killed` notification the statement queues on commit
	/// is the only thing that carries that id to this transport, so the
	/// notification dispatcher is what has to drop the entry -- otherwise
	/// `state.live_queries` holds it, and the active-LQ gauge counts it, until
	/// the connection closes.
	#[test]
	fn kill_statement_unregisters_live_query_over_websocket() {
		with_big_stack(|| async {
			let mut harness = LiveHarness::new().await;
			let lqid = harness.register_live().await;

			// KILL it as a statement, rather than through the `kill` RPC method.
			let response = harness.query("kill", &format!("KILL u'{lqid}';")).await;
			// PROOF THE KILL RAN: a KILL that errored would leave the
			// registration in place for a reason that has nothing to do with
			// the dispatcher, and the leak assertion below would be reporting
			// the wrong bug.
			assert_eq!(
				response["result"][0]["status"], "OK",
				"KILL statement did not succeed: {response}",
			);

			// The subscriber is still owed the KILLED frame.
			let frame = harness.next_frame().await;
			assert!(
				frame.contains("KILLED"),
				"expected a KILLED notification on the connection channel, got: {frame}",
			);

			// The registration goes with it.
			let leaked = harness.registrations_after_unregistering(&lqid).await;
			assert!(
				leaked.is_empty(),
				"KILL statement left live-query registrations behind: {leaked:?} -- \
				 the entry (and the active-LQ gauge) leak for the lifetime of the \
				 connection even though the subscription is gone",
			);

			harness.shutdown().await;
		});
	}

	/// Removing the table a live query watches must drop its WebSocket
	/// registration too.
	///
	/// `REMOVE TABLE` ends every subscription on the table and tells each
	/// subscriber with a `Killed` notification. No query result carries those
	/// ids -- the statement's own result says nothing about them -- so the
	/// dispatcher is the only place they can be unregistered.
	#[test]
	fn removing_a_table_unregisters_its_live_queries_over_websocket() {
		with_big_stack(|| async {
			let mut harness = LiveHarness::new().await;
			let lqid = harness.register_live().await;

			let response = harness.query("remove", "REMOVE TABLE foo;").await;
			assert_eq!(
				response["result"][0]["status"], "OK",
				"REMOVE TABLE did not succeed: {response}",
			);

			let frame = harness.next_frame().await;
			assert!(
				frame.contains("KILLED"),
				"expected a KILLED notification on the connection channel, got: {frame}",
			);

			let leaked = harness.registrations_after_unregistering(&lqid).await;
			assert!(
				leaked.is_empty(),
				"REMOVE TABLE left live-query registrations behind: {leaked:?} -- \
				 the subscription is gone from storage but the entry (and the \
				 active-LQ gauge) survive for the lifetime of the connection",
			);

			harness.shutdown().await;
		});
	}

	/// Regression test for the explicit-`begin`-on-cancel leak (Codex P2,
	/// PR #286).
	///
	/// `begin` does NOT go through the executor (it calls
	/// `kvs().transaction(...)` directly), so the executor cancellation
	/// flag the rest of the WS layer plumbs does NOT cover it. The
	/// dedicated canceller gate in `begin()` must observe a set
	/// canceller and refuse to open the transaction.
	#[test]
	fn begin_rpc_after_cancel_does_not_leak_transaction() {
		use surrealdb_core::dbs::Session;
		use surrealdb_rpc::capabilities::Capabilities;

		with_big_stack(|| async {
			let ds = Datastore::builder()
				.with_capabilities(Capabilities::all())
				.build_with_path("memory")
				.await
				.unwrap();
			let owner = Session::owner();
			ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
			let owner_ns = owner.clone().with_ns("test");
			ds.execute("DEFINE DB `test`", &owner_ns, None).await.unwrap();

			let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
			let id = Uuid::new_v4();
			let (chn_internal, _chn_internal_rx) = channel::<Message>(8);
			let (chn_notify, _chn_notify_rx) = channel::<Message>(8);
			let (chn_close, _chn_close_rx) = channel::<Message>(1);
			let rpc = Arc::new(Websocket {
				id,
				format: Format::Json,
				state,
				datastore: ds,
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				counters: DashMap::new(),
				streams: DashMap::new(),
				stream_slots: AtomicUsize::new(0),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: chn_internal,
				notifications: chn_notify,
				lagging_close: chn_close,
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

	/// The per-connection open-transaction cap is enforced: once the
	/// connection's implicit default session holds
	/// `MAX_TRANSACTIONS_PER_CONNECTION` open transactions, further `begin`s
	/// are rejected until one is committed or cancelled, without opening (and
	/// leaking) a transaction for the rejected request.
	#[test]
	fn begin_enforces_per_connection_transaction_limit() {
		with_big_stack(|| async {
			let rpc = ws_with_observer(None).await;
			let limit = *MAX_TRANSACTIONS_PER_CONNECTION;
			// Open transactions up to the limit on the default session.
			let mut first_id = None;
			for _ in 0..limit {
				let res =
					rpc.begin(None, rpc.id).await.expect("begin under the limit should succeed");
				if first_id.is_none() {
					let DbResult::Other(Value::Uuid(id)) = res else {
						panic!("begin should return a transaction uuid");
					};
					first_id = Some(id);
				}
			}
			assert_eq!(
				rpc.transactions.len(),
				limit,
				"every begin under the limit should be stored",
			);
			// The next begin must be rejected without opening a transaction.
			let err =
				rpc.begin(None, rpc.id).await.expect_err("begin over the limit should be rejected");
			assert!(
				err.to_string().contains("Too many open transactions"),
				"unexpected error: {err}",
			);
			assert_eq!(
				rpc.transactions.len(),
				limit,
				"a rejected begin must not add to the transactions map",
			);
			// Cancelling one frees a slot, so a subsequent begin succeeds again.
			let id = first_id.expect("captured a transaction id");
			let params = Array::from(vec![Value::Uuid(id)]);
			rpc.cancel(None, rpc.id, params).await.expect("cancel should succeed");
			assert_eq!(rpc.transactions.len(), limit - 1);
			rpc.begin(None, rpc.id).await.expect("begin after a cancel frees a slot");
			assert_eq!(rpc.transactions.len(), limit);
			// Drain everything so no transaction is dropped uncommitted.
			rpc.cleanup_all_txns().await;
			assert!(rpc.transactions.is_empty());
		});
	}

	/// `cleanup_txns` drains only the transactions belonging to the given
	/// session (releasing their slots) and leaves other sessions' transactions,
	/// and their counters, untouched.
	#[test]
	fn cleanup_txns_is_scoped_to_a_single_session() {
		with_big_stack(|| async {
			let rpc = ws_with_observer(None).await;
			// Register the connection's default session and attach a second one;
			// `begin` now rejects unknown sessions, so both must exist first.
			rpc.set_session(rpc.id, Arc::new(RwLock::new(Session::default())));
			let attached = Uuid::new_v4();
			rpc.attach(attached).await.expect("attach a second session");
			// Two transactions on the default session, one on an attached session.
			rpc.begin(None, rpc.id).await.expect("default begin 1");
			rpc.begin(None, rpc.id).await.expect("default begin 2");
			rpc.begin(None, attached).await.expect("attached begin");
			assert_eq!(rpc.transactions.len(), 3);
			// Clean up just the attached session.
			rpc.cleanup_txns(&attached).await;
			// The attached transaction is gone; the default ones remain.
			assert_eq!(rpc.transactions.len(), 2);
			assert!(
				rpc.transactions.iter().all(|e| e.value().0 == rpc.id),
				"only the default session's transactions should remain",
			);
			// The attached session's counter entry is removed once fully drained.
			assert!(rpc.counters.get(&attached).is_none());
			// The default session's counter still reflects its two open txns.
			assert_eq!(
				rpc.counters.get(&rpc.id).map(|c| c.value().load(Ordering::Acquire)),
				Some(2),
			);
			rpc.cleanup_all_txns().await;
			assert!(rpc.transactions.is_empty());
		});
	}

	/// `begin` rejects a session that was never attached, and — because the
	/// check runs before the slot reservation — a rejected `begin` leaves no
	/// `counters` entry behind. This is what keeps the per-session cap from
	/// being bypassed (and the counter keyspace from growing) via fabricated
	/// session ids.
	#[test]
	fn begin_rejects_an_unknown_session() {
		with_big_stack(|| async {
			let rpc = ws_with_observer(None).await;
			let unknown = Uuid::new_v4();
			let err = rpc
				.begin(None, unknown)
				.await
				.expect_err("begin on an unattached session should be rejected");
			assert!(
				err.to_string().to_lowercase().contains("session"),
				"expected a session-not-found error, got: {err}",
			);
			// No transaction opened, and — crucially — no counter entry minted for
			// the bogus session, so it cannot accrete unbounded map entries.
			assert!(rpc.transactions.is_empty());
			assert!(rpc.counters.get(&unknown).is_none());
		});
	}

	/// Releasing the last open transaction for an attached session prunes its
	/// counter entry, so no release path — normal `commit`/`cancel` or `begin`'s
	/// limit-rejection early return — can accrete zeroed counter entries under
	/// rotated session ids. The connection's default-session counter is exempt.
	#[test]
	fn releasing_last_txn_prunes_attached_session_counter() {
		with_big_stack(|| async {
			let rpc = ws_with_observer(None).await;
			let s = Uuid::new_v4();
			rpc.attach(s).await.expect("attach a session");
			// Open a transaction on the attached session: a counter entry appears.
			let res = rpc.begin(None, s).await.expect("begin");
			let DbResult::Other(Value::Uuid(id)) = res else {
				panic!("begin should return a transaction uuid");
			};
			assert_eq!(rpc.counters.get(&s).map(|c| c.value().load(Ordering::Acquire)), Some(1),);
			// Cancelling the last transaction must drop the counter entry, not
			// leave a zeroed one behind.
			rpc.cancel(None, s, Array::from(vec![Value::Uuid(id)])).await.expect("cancel");
			assert!(
				rpc.counters.get(&s).is_none(),
				"a zeroed counter entry was left behind for an attached session",
			);
			// The default-session counter, by contrast, is retained across a
			// begin/cancel cycle so the hot path does not churn its entry.
			let res = rpc.begin(None, rpc.id).await.expect("default begin");
			let DbResult::Other(Value::Uuid(did)) = res else {
				panic!("begin should return a transaction uuid");
			};
			rpc.cancel(None, rpc.id, Array::from(vec![Value::Uuid(did)])).await.expect("cancel");
			assert_eq!(
				rpc.counters.get(&rpc.id).map(|c| c.value().load(Ordering::Acquire)),
				Some(0),
			);
		});
	}

	/// The `begin`/`detach` race must not strand an orphan transaction or
	/// counter entry under a detached session. RPCs on one connection run
	/// concurrently, so without the post-insert session re-check a client
	/// rotating fresh session ids could grow `transactions` and `counters`
	/// without bound for the connection's lifetime. Runs many concurrent
	/// `begin`/`detach` pairs (the harness uses a multi-threaded runtime) and
	/// asserts that, whichever way each pair interleaves, nothing is left tagged
	/// with the now-detached session.
	#[test]
	fn begin_racing_detach_does_not_strand_orphans() {
		with_big_stack(|| async {
			let rpc = ws_with_observer(None).await;
			rpc.set_session(rpc.id, Arc::new(RwLock::new(Session::default())));
			for _ in 0..200 {
				let s = Uuid::new_v4();
				rpc.attach(s).await.expect("attach a fresh session");
				let r1 = Arc::clone(&rpc);
				let r2 = Arc::clone(&rpc);
				let begin = tokio::spawn(async move {
					let _ = r1.begin(None, s).await;
				});
				let detach = tokio::spawn(async move {
					let _ = r2.detach(s).await;
				});
				let _ = tokio::join!(begin, detach);
				// Whichever order the two ran, `s` ends detached, so no
				// transaction may remain tagged with it and its counter entry
				// must be gone (drained by `detach`, or undone by `begin`).
				assert!(
					rpc.transactions.iter().all(|e| e.value().0 != s),
					"a transaction was stranded under detached session {s}",
				);
				assert!(
					rpc.counters.get(&s).is_none(),
					"a counter entry was stranded under detached session {s}",
				);
			}
			rpc.cleanup_all_txns().await;
		});
	}

	// ------------------------------------------------------------------
	// Streaming (`query_stream` / `query_cancel`)
	// ------------------------------------------------------------------

	/// A Websocket over a fresh datastore with the given capabilities, its
	/// owner session pinned under the connection id against a prepared
	/// `test`/`test` namespace and database.
	async fn streaming_rpc(
		capabilities: surrealdb_rpc::capabilities::Capabilities,
	) -> Arc<Websocket> {
		let ds = Datastore::builder()
			.with_capabilities(capabilities)
			.build_with_path("memory")
			.await
			.unwrap();
		let owner = Session::owner();
		ds.execute("DEFINE NS `test`", &owner, None).await.unwrap();
		ds.execute("DEFINE DB `test`", &owner.clone().with_ns("test"), None).await.unwrap();
		let state = Arc::new(crate::rpc::RpcState::new(Arc::clone(&ds)));
		let (tx, _rx) = channel::<Message>(8);
		let (notifier, _notify_rx) = channel::<Message>(8);
		let (closer, _close_rx) = channel::<Message>(1);
		let rpc = Arc::new(Websocket {
			id: Uuid::new_v4(),
			format: Format::Json,
			state,
			datastore: ds,
			sessions: HashMap::new(),
			transactions: DashMap::new(),
			counters: DashMap::new(),
			streams: DashMap::new(),
			stream_slots: AtomicUsize::new(0),
			shutdown: CancellationToken::new(),
			cancel: surrealdb_core::ctx::CancelHandle::new(),
			channel: tx,
			notifications: notifier,
			lagging_close: closer,
		});
		let sess = Session::owner().with_ns("test").with_db("test").with_rt(true);
		rpc.set_session(rpc.id, Arc::new(RwLock::new(sess)));
		rpc
	}

	/// Run one SurrealQL statement directly against the datastore, for
	/// seeding.
	async fn seed(rpc: &Websocket, sql: &str) {
		let sess = Session::owner().with_ns("test").with_db("test");
		for result in rpc.kvs().execute(sql, &sess, None).await.unwrap() {
			result.result.expect("seeding should succeed");
		}
	}

	/// Send one request and collect every message it answers with, decoded
	/// from the Json format. The messages are consumed while the handler
	/// runs, so channel backpressure never parks it.
	async fn collect_messages(
		rpc: &Arc<Websocket>,
		body: serde_json::Value,
	) -> Vec<serde_json::Value> {
		let msg = Message::Text(body.to_string().into());
		let (chn_tx, mut chn_rx) = channel::<Message>(8);
		let handler = tokio::spawn({
			let rpc = Arc::clone(rpc);
			async move {
				Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
			}
		});
		let mut messages = Vec::new();
		while let Some(msg) = chn_rx.recv().await {
			messages.push(decode_json(msg));
		}
		handler.await.expect("handler completes");
		messages
	}

	/// Send one `query_stream` request for `sql` and collect its messages.
	async fn stream_request(rpc: &Arc<Websocket>, id: &str, sql: &str) -> Vec<serde_json::Value> {
		collect_messages(
			rpc,
			serde_json::json!({ "id": id, "method": "query_stream", "params": [sql] }),
		)
		.await
	}

	fn decode_json(msg: Message) -> serde_json::Value {
		match msg {
			Message::Text(text) => serde_json::from_str(&text).expect("valid response json"),
			other => panic!("expected Text response from Json format, got {other:?}"),
		}
	}

	/// The frame tag of a message, when it is a stream frame.
	fn frame_tag(msg: &serde_json::Value) -> Option<&str> {
		msg.get("result")?.get("stream")?.as_str()
	}

	/// The id a `LIVE SELECT` in a framed answer produced.
	///
	/// Found by the statement that finished as a live query rather than by
	/// taking the first `value` frame, since any preceding statement produces
	/// one of those as well.
	fn live_query_id(frames: &[serde_json::Value]) -> String {
		let index = frames
			.iter()
			.find(|m| {
				frame_tag(m) == Some("finished") && m["result"]["type"].as_str() == Some("live")
			})
			.map(|m| &m["result"]["index"])
			.unwrap_or_else(|| panic!("a statement finished as a live query: {frames:#?}"));
		frames
			.iter()
			.find(|m| frame_tag(m) == Some("value") && &m["result"]["index"] == index)
			.and_then(|m| m["result"]["value"].as_str())
			.unwrap_or_else(|| panic!("that statement carried its id: {frames:#?}"))
			.to_owned()
	}

	/// The `query_stream` answer is the framed form of the buffered answer:
	/// ordered frames bracketed by `begin` and `end`, rows re-batched along
	/// the ramp, a scalar statement marked `single`, and every frame carrying
	/// the request id.
	#[test]
	fn a_streaming_query_answers_with_ordered_frames() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			// Enough rows that the ramp needs more than one frame.
			seed(&rpc, "CREATE |foo:40| SET x = 1 RETURN NONE").await;
			let messages = stream_request(&rpc, "s1", "SELECT * FROM foo; RETURN 1 + 1;").await;

			for msg in &messages {
				assert_eq!(msg["id"], "s1", "every frame carries the request id: {msg}");
			}
			let tags: Vec<&str> =
				messages.iter().map(|m| frame_tag(m).expect("a stream frame")).collect();
			assert_eq!(tags.first(), Some(&"begin"), "the stream opens before any result");
			assert_eq!(tags.last(), Some(&"end"), "the stream ends exactly once");
			assert_eq!(messages[0]["result"]["statements"], 2);

			// Statement 0: forty rows, split along the ramp, then a
			// non-single finish.
			let rows: Vec<&serde_json::Value> =
				messages.iter().filter(|m| frame_tag(m) == Some("rows")).collect();
			assert!(rows.len() >= 2, "40 rows do not fit the first ramp frame: {tags:?}");
			let delivered: usize =
				rows.iter().map(|m| m["result"]["values"].as_array().unwrap().len()).sum();
			assert_eq!(delivered, 40, "every row arrives exactly once");
			assert_eq!(rows[0]["result"]["values"].as_array().unwrap().len(), 16);

			// Statement 1: a single value, marked as such by its finish.
			let value = messages
				.iter()
				.find(|m| frame_tag(m) == Some("value"))
				.expect("the RETURN produces a value frame");
			assert_eq!(value["result"]["index"], 1);
			assert_eq!(value["result"]["value"], 2);

			let finishes: Vec<&serde_json::Value> =
				messages.iter().filter(|m| frame_tag(m) == Some("finished")).collect();
			assert_eq!(finishes.len(), 2, "one finish per statement");
			assert_eq!(finishes[0]["result"]["index"], 0);
			assert_eq!(finishes[0]["result"]["single"], false);
			assert!(finishes[0]["result"].get("error").is_none());
			assert_eq!(finishes[1]["result"]["index"], 1);
			assert_eq!(finishes[1]["result"]["single"], true);

			let end = &messages[messages.len() - 1]["result"];
			assert_eq!(end["results"], 2);
			assert!(end.get("error").is_none());
			assert!(rpc.streams.is_empty(), "the stream registry is drained");
		});
	}

	/// Rows must reach the wire while the query is still executing: a
	/// trailing SLEEP holds the stream open long after the first statement's
	/// rows are produced, so observing them before the end frame — by more
	/// than the sleep — proves streaming rather than buffering.
	#[test]
	fn rows_reach_the_wire_before_the_query_finishes() {
		use std::time::Duration as StdDuration;
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			seed(&rpc, "CREATE |foo:20| SET x = 1 RETURN NONE").await;

			let body = serde_json::json!({
				"id": "t1",
				"method": "query_stream",
				"params": ["SELECT * FROM foo; SLEEP 300ms;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			let mut first_rows = None;
			let mut end = None;
			while let Some(msg) = chn_rx.recv().await {
				let msg = decode_json(msg);
				match frame_tag(&msg) {
					Some("rows") if first_rows.is_none() => {
						first_rows = Some(tokio::time::Instant::now());
					}
					Some("end") => end = Some(tokio::time::Instant::now()),
					_ => {}
				}
			}
			handler.await.expect("handler completes");
			let first_rows = first_rows.expect("rows were streamed");
			let end = end.expect("the stream ended");
			assert!(
				end.duration_since(first_rows) >= StdDuration::from_millis(250),
				"the first rows must precede the query's completion by the sleep",
			);
		});
	}

	/// Inside a transaction block that rolls back, rows already on the wire
	/// are retracted by their statement's error finish — never by silence.
	#[test]
	fn a_rolled_back_block_retracts_its_rows_on_the_wire() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			seed(&rpc, "CREATE |foo:20| SET x = 1 RETURN NONE").await;
			let messages = stream_request(
				&rpc,
				"r1",
				"BEGIN; SELECT * FROM foo; THROW 'rolled back'; COMMIT;",
			)
			.await;

			let rows_indices: Vec<i64> = messages
				.iter()
				.filter(|m| frame_tag(m) == Some("rows"))
				.map(|m| m["result"]["index"].as_i64().unwrap())
				.collect();
			assert!(!rows_indices.is_empty(), "rows go out before the block fails");
			for index in &rows_indices {
				let finish = messages
					.iter()
					.find(|m| {
						frame_tag(m) == Some("finished")
							&& m["result"]["index"].as_i64() == Some(*index)
					})
					.expect("every streamed statement still finishes");
				assert!(
					finish["result"].get("error").is_some(),
					"a rolled-back statement's finish must retract its rows: {finish}",
				);
			}
			assert_eq!(
				frame_tag(&messages[messages.len() - 1]),
				Some("end"),
				"the stream still ends in order",
			);
		});
	}

	/// `query_cancel` names an in-flight stream by its request id and stops
	/// it: the stream still ends cleanly — promptly, not after the query
	/// would have finished — and the registry entry is gone.
	#[test]
	fn query_cancel_stops_a_streaming_query() {
		use std::time::Duration as StdDuration;
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			let started = tokio::time::Instant::now();
			let body = serde_json::json!({
				"id": "c1",
				"method": "query_stream",
				"params": ["SLEEP 30s; RETURN 1;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			// The begin frame proves the stream is registered before the
			// cancel goes looking for it.
			let first = decode_json(chn_rx.recv().await.expect("a first frame"));
			assert_eq!(frame_tag(&first), Some("begin"));

			// Cancel through the full envelope path, as a client would.
			let cancel = collect_messages(
				&rpc,
				serde_json::json!({
					"id": "c2",
					"method": "query_cancel",
					"params": ["c1"],
				}),
			)
			.await;
			assert_eq!(cancel.len(), 1);
			assert_eq!(cancel[0]["id"], "c2");
			assert!(cancel[0].get("error").is_none(), "the cancel succeeds: {:?}", cancel[0]);

			// The stream ends long before the 30s sleep would have, and its
			// end frame says the answer is incomplete: a client must not read
			// a cancelled stream as a whole one.
			let mut end = None;
			while let Some(msg) = chn_rx.recv().await {
				let msg = decode_json(msg);
				if frame_tag(&msg) == Some("end") {
					end = Some(msg);
				}
			}
			handler.await.expect("handler completes");
			let end = end.expect("a cancelled stream still ends with its end frame");
			assert!(
				end["result"].get("error").is_some(),
				"a cancelled stream's end frame must retract what it never delivered: {end}",
			);
			assert!(
				started.elapsed() < StdDuration::from_secs(20),
				"the cancel must interrupt the sleep",
			);
			assert!(rpc.streams.is_empty(), "the registry entry is removed");

			// A second cancel finds nothing to stop.
			let again = RpcProtocol::query_cancel(
				rpc.as_ref(),
				Array::from(vec![Value::String("c1".to_string())]),
			)
			.await;
			assert!(again.is_err(), "cancelling a finished stream is an error");
		});
	}

	/// Frames are correlated by the request id, so a request without one is
	/// refused before anything runs.
	#[test]
	fn a_streaming_query_requires_a_request_id() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			let messages = collect_messages(
				&rpc,
				serde_json::json!({ "method": "query_stream", "params": ["RETURN 1;"] }),
			)
			.await;
			assert_eq!(messages.len(), 1, "one failure, no frames: {messages:?}");
			let error = messages[0]["error"]["message"].as_str().unwrap_or_default();
			assert!(error.contains("request id"), "names the missing id: {error}");
		});
	}

	/// A request id names one stream: while it is in flight, a second
	/// request under the same id is refused and the original is undisturbed.
	/// Undisturbed includes its registration — a cancel after the refusal
	/// must stop the original stream, not a leftover of the duplicate.
	#[test]
	fn a_duplicate_stream_id_is_refused() {
		use std::time::Duration as StdDuration;
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			let started = tokio::time::Instant::now();
			let body = serde_json::json!({
				"id": "dup",
				"method": "query_stream",
				"params": ["SLEEP 30s;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			let first = decode_json(chn_rx.recv().await.expect("a first frame"));
			assert_eq!(frame_tag(&first), Some("begin"));

			let duplicate = collect_messages(
				&rpc,
				serde_json::json!({
					"id": "dup",
					"method": "query_stream",
					"params": ["RETURN 1;"],
				}),
			)
			.await;
			assert_eq!(duplicate.len(), 1, "the duplicate is refused outright");
			assert!(
				duplicate[0]["error"]["message"]
					.as_str()
					.unwrap_or_default()
					.contains("already in progress"),
				"names the collision: {:?}",
				duplicate[0],
			);

			// The original stream is still cancellable — it was not clobbered.
			RpcProtocol::query_cancel(
				rpc.as_ref(),
				Array::from(vec![Value::String("dup".to_string())]),
			)
			.await
			.expect("the original stream is still registered");
			while chn_rx.recv().await.is_some() {}
			handler.await.expect("handler completes");
			// The cancel reached the ORIGINAL stream's execution: had the
			// duplicate clobbered its registration, the cancel would have
			// tripped a dangling handle and the sleep would run its full
			// course.
			assert!(
				started.elapsed() < StdDuration::from_secs(20),
				"the cancel must interrupt the original stream's sleep",
			);
		});
	}

	/// A connection holds at most [`WEBSOCKET_MAX_CONCURRENT_STREAMS`]
	/// executing streams: the excess request is refused outright — nothing is
	/// queued — and cancelling the others frees the slots.
	#[test]
	fn the_concurrent_stream_cap_refuses_the_excess_stream() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;

			use crate::cnf::WEBSOCKET_MAX_CONCURRENT_STREAMS;
			let rpc = streaming_rpc(Capabilities::all()).await;
			let mut streams = Vec::new();
			for i in 0..*WEBSOCKET_MAX_CONCURRENT_STREAMS {
				let body = serde_json::json!({
					"id": format!("s{i}"),
					"method": "query_stream",
					"params": ["SLEEP 30s;"],
				});
				let msg = Message::Text(body.to_string().into());
				let (chn_tx, mut chn_rx) = channel::<Message>(8);
				let handler = tokio::spawn({
					let rpc = Arc::clone(&rpc);
					async move {
						Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
					}
				});
				let first = decode_json(chn_rx.recv().await.expect("a first frame"));
				assert_eq!(frame_tag(&first), Some("begin"), "stream {i} starts");
				streams.push((handler, chn_rx));
			}

			let refused = collect_messages(
				&rpc,
				serde_json::json!({
					"id": "over",
					"method": "query_stream",
					"params": ["RETURN 1;"],
				}),
			)
			.await;
			assert_eq!(refused.len(), 1, "the excess stream is refused outright");
			assert!(
				refused[0]["error"]["message"]
					.as_str()
					.unwrap_or_default()
					.contains("Too many concurrent streaming queries"),
				"names the cap: {:?}",
				refused[0],
			);

			for i in 0..streams.len() {
				RpcProtocol::query_cancel(
					rpc.as_ref(),
					Array::from(vec![Value::String(format!("s{i}"))]),
				)
				.await
				.expect("cancel stream");
			}
			for (handler, mut chn_rx) in streams {
				while chn_rx.recv().await.is_some() {}
				handler.await.expect("handler completes");
			}
			assert!(rpc.streams.is_empty(), "every slot is freed");
			// The registry emptying is not enough on its own: capacity is held
			// by the slot count, so a refused request that gave back its entry
			// but not its slot would leave the connection permanently short of
			// the cap while looking idle.
			assert_eq!(
				rpc.stream_slots.load(Ordering::Acquire),
				0,
				"the refused request gave its slot back too",
			);
		});
	}

	/// A rejected request holds no reservation while it reports the rejection.
	///
	/// Reporting is an awaited send, so on a connection that has stopped
	/// reading it parks. The read loop keeps accepting messages, so if a
	/// rejection kept its reservation while parked, a client could grow the
	/// registry without bound precisely by refusing to read — defeating the
	/// cap it was being refused by.
	#[test]
	fn an_over_cap_rejection_holds_no_reservation_while_it_parks() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;

			use crate::cnf::WEBSOCKET_MAX_CONCURRENT_STREAMS;
			let cap = *WEBSOCKET_MAX_CONCURRENT_STREAMS;
			let rpc = streaming_rpc(Capabilities::all()).await;
			// Fill the cap with streams that stay open.
			let mut streams = Vec::new();
			for i in 0..cap {
				let body = serde_json::json!({
					"id": format!("s{i}"),
					"method": "query_stream",
					"params": ["SLEEP 30s;"],
				});
				let msg = Message::Text(body.to_string().into());
				let (chn_tx, mut chn_rx) = channel::<Message>(8);
				let handler = tokio::spawn({
					let rpc = Arc::clone(&rpc);
					async move {
						Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
					}
				});
				let first = decode_json(chn_rx.recv().await.expect("a first frame"));
				assert_eq!(frame_tag(&first), Some("begin"), "stream {i} starts");
				streams.push((handler, chn_rx));
			}
			assert_eq!(rpc.streams.len(), cap, "the cap is full");

			// A channel nobody drains, saturated so the next send parks: this
			// is a client that has stopped reading.
			let (stalled_tx, mut stalled_rx) = channel::<Message>(1);
			stalled_tx.send(Message::Text("filler".into())).await.expect("saturate the channel");

			// Over-cap requests, each with a fresh id, all parking on their
			// rejection.
			let mut rejected = Vec::new();
			for i in 0..8 {
				let body = serde_json::json!({
					"id": format!("over{i}"),
					"method": "query_stream",
					"params": ["RETURN 1;"],
				});
				let msg = Message::Text(body.to_string().into());
				rejected.push(tokio::spawn({
					let rpc = Arc::clone(&rpc);
					let chn = stalled_tx.clone();
					async move {
						Websocket::handle_message(&rpc, msg, chn, 1024).await;
					}
				}));
			}
			// Observe each rejection actually reaching its parked send, rather
			// than sleeping and hoping: the filler occupies the only slot, so
			// draining one message frees exactly one rejection to complete its
			// send. Every rejection observed this way is one that was parked
			// while the registry was inspected.
			let filler = stalled_rx.recv().await.expect("the filler message");
			assert!(matches!(filler, Message::Text(_)), "the filler is what saturated the channel");
			for observed in 0..rejected.len() {
				let msg = decode_json(
					tokio::time::timeout(std::time::Duration::from_secs(10), stalled_rx.recv())
						.await
						.expect("a rejection reaches its send")
						.expect("the channel stays open"),
				);
				assert!(
					msg["error"]["message"]
						.as_str()
						.unwrap_or_default()
						.contains("Too many concurrent streaming queries"),
					"each parked message is an over-cap rejection: {msg}",
				);
				// The invariant, checked while the remaining rejections are
				// still parked: none of them is holding a reservation. Sampled
				// once, so the number reported is the number that failed.
				let registered = rpc.streams.len();
				assert_eq!(
					registered,
					cap,
					"with {} rejection(s) still parked, the registry grew to {registered} \
					 beyond the cap of {cap}",
					rejected.len() - observed - 1,
				);
			}

			for handler in rejected {
				handler.await.expect("each rejection completes");
			}
			for i in 0..cap {
				RpcProtocol::query_cancel(
					rpc.as_ref(),
					Array::from(vec![Value::String(format!("s{i}"))]),
				)
				.await
				.expect("cancel stream");
			}
			for (handler, mut chn_rx) in streams {
				while chn_rx.recv().await.is_some() {}
				handler.await.expect("handler completes");
			}
		});
	}

	/// The bracket frames survive a cancel: a client told a stream ended must
	/// also have been told it began, or it cannot reconcile the two.
	#[test]
	fn a_cancel_cannot_drop_the_frames_that_bracket_a_stream() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			// Trip the stream's canceller before its first frame can go out, by
			// cancelling as soon as the registry entry exists. A channel with
			// room means the send would otherwise succeed, so only the cancel
			// race can drop `Begin`.
			let body = serde_json::json!({
				"id": "b1",
				"method": "query_stream",
				"params": ["SLEEP 5s; RETURN 1;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let canceller = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					// Cancel the moment the stream registers, which is before
					// its first frame is sent.
					loop {
						if RpcProtocol::query_cancel(
							rpc.as_ref(),
							Array::from(vec![Value::String("b1".to_string())]),
						)
						.await
						.is_ok()
						{
							return;
						}
						tokio::task::yield_now().await;
					}
				}
			});
			Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
			canceller.await.expect("the cancel lands");

			let mut frames = Vec::new();
			while let Some(msg) = chn_rx.recv().await {
				frames.push(decode_json(msg));
			}
			let tags: Vec<Option<&str>> = frames.iter().map(frame_tag).collect();
			assert_eq!(tags.first(), Some(&Some("begin")), "the stream still opens: {tags:?}");
			assert_eq!(tags.last(), Some(&Some("end")), "and still ends: {tags:?}");
		});
	}

	/// The terminal frame counts what the client actually received, not what was
	/// framed — so the count it is given and the `finished` frames it saw agree
	/// even when the two diverge.
	///
	/// They diverge when a statement's terminal frame is framed but never sent,
	/// which a cancel mid-stream produces: the executor keeps yielding the items
	/// it had already buffered, so framing continues past the stop while
	/// delivery does not.
	#[test]
	fn the_end_frame_counts_only_delivered_statements() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			seed(&rpc, "CREATE |foo:600| SET x = 1 RETURN NONE").await;
			let body = serde_json::json!({
				"id": "d1",
				"method": "query_stream",
				"params": ["SELECT * FROM foo; RETURN 1; RETURN 2;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			// Cancel once rows are flowing, so later statements are framed but
			// not delivered.
			let mut frames = Vec::new();
			loop {
				let msg = decode_json(chn_rx.recv().await.expect("a frame"));
				let rows = frame_tag(&msg) == Some("rows");
				frames.push(msg);
				if rows {
					break;
				}
			}
			RpcProtocol::query_cancel(
				rpc.as_ref(),
				Array::from(vec![Value::String("d1".to_string())]),
			)
			.await
			.expect("cancel the stream");
			while let Some(msg) = chn_rx.recv().await {
				frames.push(decode_json(msg));
			}
			handler.await.expect("handler completes");

			let delivered = frames.iter().filter(|m| frame_tag(m) == Some("finished")).count();
			// The divergence has to be real, or this would hold just as well
			// against a count taken from what was framed rather than delivered,
			// and could not have caught that.
			assert!(
				delivered < 3,
				"the cancel must leave at least one of the three statements unfinished on \
				 the wire, got {delivered}: {frames:#?}",
			);
			let end = frames.iter().find(|m| frame_tag(m) == Some("end")).expect("the stream ends");
			assert_eq!(
				end["result"]["results"].as_u64(),
				Some(delivered as u64),
				"the count must match the finished frames the client saw: {frames:#?}",
			);
			// And the stream says it did not complete, so the statements with no
			// terminal frame are retracted.
			assert!(
				end["result"].get("error").is_some(),
				"a cancelled stream's end frame carries an error: {end}",
			);
		});
	}

	/// A `LIVE SELECT` over a stream registers its subscription exactly as
	/// the buffered path does, so notifications flow and disconnect cleanup
	/// can find it.
	#[test]
	fn a_live_select_over_a_stream_registers_its_subscription() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			seed(&rpc, "DEFINE TABLE foo").await;
			let messages = stream_request(&rpc, "l1", "LIVE SELECT * FROM foo;").await;

			let value =
				messages.iter().find(|m| frame_tag(m) == Some("value")).unwrap_or_else(|| {
					panic!("the live query id arrives as a value frame: {messages:#?}")
				});
			let lqid: Uuid =
				value["result"]["value"].as_str().unwrap().parse().expect("a live query id");
			let finish = messages
				.iter()
				.find(|m| frame_tag(m) == Some("finished"))
				.expect("the statement finishes");
			assert_eq!(finish["result"]["type"], "live");

			let live = rpc.state.live_queries.read().await;
			let entry = live.get(&lqid).expect("the live query is registered for dispatch");
			assert_eq!(entry.websocket_id, rpc.id);
			assert_eq!(entry.session_id, rpc.id, "registered under the resolved session");
		});
	}

	/// A cancel that lands mid-result must not present the rows it did
	/// deliver as a complete answer: the statement it truncated gets no
	/// success finish, and the end frame carries the retraction.
	#[test]
	fn a_cancel_mid_result_never_reports_a_truncated_statement_as_complete() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			seed(&rpc, "CREATE |foo:400| SET x = 1 RETURN NONE").await;
			let body = serde_json::json!({
				"id": "m1",
				"method": "query_stream",
				"params": ["SELECT * FROM foo; SLEEP 20s;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			// Cancel once rows are flowing, so the stream is mid-answer.
			let mut seen_rows = false;
			while !seen_rows {
				let msg = decode_json(chn_rx.recv().await.expect("a frame"));
				seen_rows = frame_tag(&msg) == Some("rows");
			}
			RpcProtocol::query_cancel(
				rpc.as_ref(),
				Array::from(vec![Value::String("m1".to_string())]),
			)
			.await
			.expect("cancel the stream");

			let mut frames = Vec::new();
			while let Some(msg) = chn_rx.recv().await {
				frames.push(decode_json(msg));
			}
			handler.await.expect("handler completes");
			let end = frames.iter().find(|m| frame_tag(m) == Some("end")).expect("the stream ends");
			assert!(
				end["result"].get("error").is_some(),
				"the end frame must retract the statements it never finished: {end}",
			);
			// The SLEEP never ran to completion, so it must not be reported as
			// a finished statement.
			let finished: Vec<i64> = frames
				.iter()
				.filter(|m| frame_tag(m) == Some("finished"))
				.map(|m| m["result"]["index"].as_i64().unwrap())
				.collect();
			assert!(
				!finished.contains(&1),
				"an unfinished statement is not reported: {finished:?}"
			);
		});
	}

	/// A value the negotiated format cannot carry fails its statement rather
	/// than the stream, and the failure reaches the wire in order: the
	/// retraction precedes the terminal frame, and that terminal frame counts
	/// what was actually delivered.
	///
	/// CBOR cannot encode a regex, which makes this reachable with one
	/// ordinary query rather than a fault injection.
	#[test]
	fn an_unencodable_value_retracts_its_statement_in_order() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			// The frames of this connection are encoded as CBOR.
			let (notifier, _notify_rx) = channel::<Message>(8);
			let (closer, _close_rx) = channel::<Message>(1);
			let rpc = Arc::new(Websocket {
				id: rpc.id,
				format: Format::Cbor,
				state: Arc::clone(&rpc.state),
				datastore: Arc::clone(&rpc.datastore),
				sessions: HashMap::new(),
				transactions: DashMap::new(),
				counters: DashMap::new(),
				streams: DashMap::new(),
				stream_slots: AtomicUsize::new(0),
				shutdown: CancellationToken::new(),
				cancel: surrealdb_core::ctx::CancelHandle::new(),
				channel: rpc.channel.clone(),
				notifications: notifier,
				lagging_close: closer,
			});
			let sess = Session::owner().with_ns("test").with_db("test").with_rt(true);
			rpc.set_session(rpc.id, Arc::new(RwLock::new(sess)));

			// The request rides the same format as the frames it will be
			// answered with.
			let body = Value::Object(surrealdb_types::object! {
				id: "e1",
				method: "query_stream",
				params: Value::Array(Array::from(vec![Value::String(
					"RETURN /abc/; RETURN 1;".to_string(),
				)])),
			});
			let encoded = surrealdb_core::rpc::format::cbor::encode(body).expect("encode request");
			let msg = Message::Binary(encoded.into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			let mut frames = Vec::new();
			while let Some(msg) = chn_rx.recv().await {
				let Message::Binary(bytes) = msg else {
					panic!("cbor frames are binary");
				};
				let value = surrealdb_core::rpc::format::cbor::decode(&bytes, 64)
					.expect("a decodable frame");
				frames.push(value.to_sql());
			}
			handler.await.expect("handler completes");

			// The unencodable statement is retracted, and the retraction is not
			// the last thing on the wire -- the terminal frame is.
			let retraction = frames
				.iter()
				.position(|f| f.contains("'finished'") && f.contains("error"))
				.unwrap_or_else(|| panic!("the statement is retracted: {frames:#?}"));
			let end = frames
				.iter()
				.position(|f| f.contains("stream: 'end'"))
				.unwrap_or_else(|| panic!("the stream ends: {frames:#?}"));
			assert!(
				retraction < end,
				"a retraction raised while the queue drained must precede the end frame: {frames:#?}",
			);
			assert_eq!(end, frames.len() - 1, "nothing follows the end frame: {frames:#?}");
			// The statement that could be answered still was.
			assert!(
				frames.iter().any(|f| f.contains("stream: 'value'")),
				"the encodable statement is unaffected: {frames:#?}",
			);
		});
	}

	/// A `LIVE SELECT` whose session is detached before the stream finishes
	/// must not be registered: the registration would deliver change data
	/// under an authorization that has been torn down, and the per-session
	/// cleanup has already run past it.
	#[test]
	fn a_detached_sessions_live_query_is_discarded_not_registered() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			seed(&rpc, "DEFINE TABLE thing").await;
			// Attach a second session to run the stream under, so it can be
			// detached while the query is in flight.
			let session_id = Uuid::new_v4();
			rpc.attach(session_id).await.expect("attach a session");
			{
				let lock = rpc.get_session(&session_id).await.expect("the attached session");
				let mut session = lock.write().await;
				*session = Session::owner().with_ns("test").with_db("test").with_rt(true);
				session.id = Some(session_id);
			}

			let body = serde_json::json!({
				"id": "d1",
				"session": session_id.to_string(),
				"method": "query_stream",
				"params": ["SLEEP 100ms; LIVE SELECT * FROM thing;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			let first = decode_json(chn_rx.recv().await.expect("a first frame"));
			assert_eq!(frame_tag(&first), Some("begin"));
			// Detach while the SLEEP holds the query open, so the session is
			// gone by the time the LIVE statement's registration is attempted.
			rpc.detach(session_id).await.expect("detach the session");

			let mut frames = Vec::new();
			while let Some(msg) = chn_rx.recv().await {
				frames.push(decode_json(msg));
			}
			handler.await.expect("handler completes");
			assert!(
				rpc.state.live_queries.read().await.is_empty(),
				"a detached session's live query must not be registered",
			);
			// The client was told the live query succeeded -- it holds the id --
			// so being told the stream ended is not enough: an errored `End`
			// retracts only statements without a `Finished` frame, and this one
			// has one. The id has to be named so the client can drop it.
			let lqid = live_query_id(&frames);
			let end = frames.iter().find(|m| frame_tag(m) == Some("end")).expect("the stream ends");
			let error = end["result"]["error"]["message"].as_str().unwrap_or_default();
			assert!(
				error.contains(&lqid),
				"the discarded live query {lqid} must be named on the end frame, got: {error}",
			);
		});
	}

	/// A `LIVE SELECT` whose session stopped acting as the principal that
	/// created it must not be registered either.
	///
	/// `invalidate` is the case that makes "does the session still exist" the
	/// wrong question: it keeps the session and clears its authentication, so its
	/// `cleanup_lqs` sweeps a map this id is not in yet and the registration
	/// would otherwise land behind the teardown — delivering change data after a
	/// logout. This is the shape GHSA-2xrp-m9c6-75rj describes.
	#[test]
	fn an_invalidated_sessions_live_query_is_discarded_not_registered() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::Capabilities;
			let rpc = streaming_rpc(Capabilities::all()).await;
			seed(&rpc, "DEFINE TABLE thing").await;
			let session_id = Uuid::new_v4();
			rpc.attach(session_id).await.expect("attach a session");
			{
				let lock = rpc.get_session(&session_id).await.expect("the attached session");
				let mut session = lock.write().await;
				*session = Session::owner().with_ns("test").with_db("test").with_rt(true);
				session.id = Some(session_id);
			}

			let body = serde_json::json!({
				"id": "i1",
				"session": session_id.to_string(),
				"method": "query_stream",
				"params": ["SLEEP 100ms; LIVE SELECT * FROM thing;"],
			});
			let msg = Message::Text(body.to_string().into());
			let (chn_tx, mut chn_rx) = channel::<Message>(8);
			let handler = tokio::spawn({
				let rpc = Arc::clone(&rpc);
				async move {
					Websocket::handle_message(&rpc, msg, chn_tx, 1024).await;
				}
			});
			let first = decode_json(chn_rx.recv().await.expect("a first frame"));
			assert_eq!(frame_tag(&first), Some("begin"));
			// Invalidate while the SLEEP holds the query open. The session stays;
			// only the principal it acts as goes away.
			RpcProtocol::invalidate(rpc.as_ref(), session_id).await.expect("invalidate");

			let mut frames = Vec::new();
			while let Some(msg) = chn_rx.recv().await {
				frames.push(decode_json(msg));
			}
			handler.await.expect("handler completes");
			assert!(
				rpc.state.live_queries.read().await.is_empty(),
				"a live query created under a principal the session no longer has must not be \
				 registered: {frames:#?}",
			);
			// The id is taken from the statement that finished as a live query,
			// not from the first `value` frame: the `SLEEP` ahead of it produces
			// one too, carrying null. Unconditional, because the client was told
			// this id and holds a subscription that will never fire -- were the
			// lookup allowed to find nothing, a stream that never ran the
			// statement would satisfy the assertion above on its own.
			let lqid = live_query_id(&frames);
			let end = frames.iter().find(|m| frame_tag(m) == Some("end")).expect("the stream ends");
			let error = end["result"]["error"]["message"].as_str().unwrap_or_default();
			assert!(
				error.contains(&lqid),
				"the discarded live query {lqid} must be named on the end frame, got: {error}",
			);
		});
	}

	/// Denying either the `query_stream` capability or the underlying
	/// `query` capability refuses a streaming request before anything runs.
	#[test]
	fn streaming_is_refused_by_either_capability() {
		with_big_stack(|| async {
			use surrealdb_rpc::capabilities::{Capabilities, MethodTarget, Targets};
			for denied in [Method::QueryStream, Method::Query] {
				let caps = Capabilities::all().without_rpc_methods(Targets::Some(
					[MethodTarget {
						method: denied,
					}]
					.into(),
				));
				let rpc = streaming_rpc(caps).await;
				let messages = stream_request(&rpc, "d1", "RETURN 1;").await;
				assert_eq!(messages.len(), 1, "one failure, no frames: {messages:?}");
				assert!(
					messages[0]["error"]["message"]
						.as_str()
						.unwrap_or_default()
						.contains("not allowed"),
					"denying {denied} refuses streaming: {:?}",
					messages[0],
				);
			}
		});
	}
}
