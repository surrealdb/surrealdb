//! WebSocket engine
//!
//! This module provides WebSocket connectivity to SurrealDB servers.
//! The core logic is shared between native and WASM platforms, with
//! platform-specific implementations in the `native` and `wasm` submodules.

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_channel::Sender;
use futures::{Sink, SinkExt};
use surrealdb_rpc::{DbResponse, DbResult, QueryResult, QueryResultBuilder, Token};
use surrealdb_types::{
	AuthError, ConnectionError, Error as TypesError, NotAllowedError, SerializationError,
	ValidationError,
};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::conn::{Command, RequestData, Route};
use crate::engine::remote::{RemoteCommand, RouterRequest};
use crate::engine::{SessionError, session_error_to_error};
use crate::opt::IntoEndpoint;
use crate::types::{Array, HashMap, Notification, Number, SurrealValue, Value};
use crate::{Connect, Error, Surreal};

pub(crate) const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);

// ============================================================================
// Core Types
// ============================================================================

#[derive(Debug, Clone)]
struct PendingRequest {
	/// The command to register for replay on success
	command: Option<Command>,
	/// The channel to send the result of the request into.
	response_channel: Sender<Result<Vec<QueryResult>, TypesError>>,
}

/// Per-session state for WebSocket connections
struct SessionState {
	/// Send requests which are still awaiting an answer.
	pending_requests: HashMap<i64, PendingRequest>,
	/// Pending live queries
	live_queries: HashMap<Uuid, Sender<crate::Result<Notification>>>,
	/// Messages which ought to be replayed on a reconnect for this session
	replay: boxcar::Vec<Command>,
	/// The replay in progress for this session, if any.
	///
	/// A session's state — namespace, database, authentication, variables — is
	/// established on the server by replaying [`Self::replay`] against it.
	/// While a replay is in progress the session is only half-built and
	/// requests must not run against it: they are parked in [`Self::deferred`].
	replay_cursor: Mutex<Option<ReplayCursor>>,
	/// Requests parked until the replay in progress finishes.
	///
	/// Only ever locked for the push or the drain itself, never across an
	/// await, and only from the single router task.
	deferred: Mutex<Vec<Route>>,
	/// The last ID used for a request
	last_id: AtomicI64,
}

/// A replay in progress: the request id currently awaiting acknowledgement, and
/// the index of the command to send once it lands.
///
/// The log is order-sensitive — two `set`s of one key, or a `set` followed by an
/// `unset`, leave different end states depending on the order they are applied —
/// and the server may apply requests that arrive together on one connection in
/// any order. Tracking a single outstanding command is what forces the replay to
/// go one at a time.
#[derive(Debug, Clone, Copy)]
struct ReplayCursor {
	/// Request id of the command in flight.
	awaiting: i64,
	/// Position in [`SessionState::replay`] of the command in flight.
	index: usize,
	/// Whether the command in flight has already been retried with a refreshed
	/// token. Bounds the retry to one attempt so an expiry the refresh token
	/// cannot fix fails the session instead of looping.
	refreshed: bool,
}

impl Default for SessionState {
	fn default() -> Self {
		Self {
			pending_requests: HashMap::new(),
			live_queries: HashMap::new(),
			replay: boxcar::Vec::new(),
			replay_cursor: Mutex::new(None),
			deferred: Mutex::new(Vec::new()),
			last_id: AtomicI64::new(0),
		}
	}
}

impl Clone for SessionState {
	fn clone(&self) -> Self {
		Self {
			replay: self.replay.clone(),
			pending_requests: HashMap::new(),
			live_queries: HashMap::new(),
			// The clone runs its own replay and parks its own requests; the
			// parent's in-flight setup and backlog belong to the parent.
			replay_cursor: Mutex::new(None),
			deferred: Mutex::new(Vec::new()),
			last_id: AtomicI64::new(0),
		}
	}
}

/// Handle result for WebSocket operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleResult {
	/// Socket disconnected, should continue to reconnect
	Disconnected,
	/// Nothing wrong, continue as normal.
	Ok,
}

// ============================================================================
// Platform Abstraction Traits
// ============================================================================

/// Trait for abstracting over different WebSocket message types (native vs WASM).
trait WsMessage: Sized + Clone + Unpin + Send {
	/// Create a binary message from bytes.
	fn binary(payload: Vec<u8>) -> Self;

	/// Check if this is a binary message and get the bytes.
	fn as_binary(&self) -> Option<&[u8]>;

	/// Check if this message should be processed (filters out ping/pong/etc).
	fn should_process(&self) -> bool {
		true
	}

	/// Get a description for logging purposes.
	fn log_description(&self) -> &'static str {
		"message"
	}
}

// ============================================================================
// Shared Helper Functions
// ============================================================================

/// Serialize a router request to a WebSocket message.
fn serialize_request<M: WsMessage>(request: RouterRequest) -> M {
	let request_value = request.into_value();
	let payload = surrealdb_types::encode(&request_value).expect("router request should serialize");
	M::binary(payload)
}

/// Create a ping message for keep-alive.
fn create_ping_message<M: WsMessage>() -> M {
	let request = Command::Health
		.into_router_request(None, None)
		.expect("HEALTH command should convert to router request");
	serialize_request(request)
}

/// Create a kill message for terminating a live query.
fn create_kill_message<M: WsMessage>(live_query_id: Uuid, session_id: Uuid) -> M {
	let request = Command::Kill {
		uuid: live_query_id,
	}
	.into_router_request(None, Some(session_id))
	.expect("KILL command should convert to router request");
	serialize_request(request)
}

/// Send a message through the sink.
async fn send_message<M, S, E>(sink: &RwLock<S>, message: M) -> Result<(), E>
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
{
	sink.write().await.send(message).await
}

// ============================================================================
// Request Handling
// ============================================================================

/// Handle an incoming route request.
///
/// This is the core logic for processing commands from the SDK client.
/// It's shared between native and WASM implementations.
async fn handle_route<M, S, E>(
	route: Route,
	max_message_size: Option<usize>,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	let session_id = route.request.session_id;

	// Get session state
	let session_state = match sessions.get(&session_id) {
		Some(Ok(state)) => state,
		Some(Err(error)) => {
			if route.response.send(Err(session_error_to_error(error))).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Ok;
		}
		None => {
			let error = session_error_to_error(SessionError::NotFound(session_id));
			if route.response.send(Err(error)).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Ok;
		}
	};

	// The session's replayed setup is still in flight, so the server has not
	// applied all of it yet. Park the request until it has: dispatching now
	// would race the replay and could run against a session with no namespace,
	// no authentication, or none of its variables.
	if replay_cursor(&session_state).is_some() {
		park_route(&session_state, route);
		return HandleResult::Ok;
	}

	dispatch_route::<M, S, E>(route, max_message_size, &session_state, sink).await
}

/// Send a request for a session whose setup the server has already applied.
async fn dispatch_route<M, S, E>(
	Route {
		request,
		response,
	}: Route,
	max_message_size: Option<usize>,
	session_state: &Arc<SessionState>,
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	let RequestData {
		command,
		session_id,
	} = request;

	// Generate a new request ID
	let id = session_state.last_id.fetch_add(1, Ordering::SeqCst);

	// Check for duplicate request IDs
	if session_state.pending_requests.contains_key(&id) {
		let error = Error::validation(
			format!("Duplicate request ID: {id}"),
			ValidationError::InvalidParams,
		);
		if response.send(Err(error)).await.is_err() {
			trace!("Receiver dropped");
		}
		return HandleResult::Ok;
	}

	// Handle special commands
	match command {
		Command::SubscribeLive {
			ref uuid,
			ref notification_sender,
		} => {
			session_state.live_queries.insert(*uuid, notification_sender.clone());
			if response.send(Ok(vec![QueryResultBuilder::instant_none()])).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Ok;
		}
		Command::Kill {
			ref uuid,
		} => {
			session_state.live_queries.remove(uuid);
		}
		_ => {}
	}

	// Serialize the request
	let Some(router_request) = command.clone().into_router_request(Some(id), Some(session_id))
	else {
		response
			.send(Err(Error::internal(
				"The protocol or storage engine does not support backups on this architecture"
					.to_string(),
			)))
			.await
			.ok();
		return HandleResult::Ok;
	};

	let message: M = serialize_request(router_request);

	// Check message size
	if let Some(max_size) = max_message_size
		&& let Some(binary) = message.as_binary()
		&& binary.len() > max_size
	{
		if response
			.send(Err(Error::validation(
				format!("Message too long: {}", binary.len()),
				ValidationError::InvalidParams,
			)))
			.await
			.is_err()
		{
			trace!("Receiver dropped");
		}
		return HandleResult::Ok;
	}

	// Send the message
	match send_message(sink, message).await {
		Ok(_) => {
			session_state.pending_requests.insert(
				id,
				PendingRequest {
					command: if command.replayable() {
						Some(command)
					} else {
						None
					},
					response_channel: response,
				},
			);
		}
		Err(error) => {
			let err = Error::connection(
				format!("WebSocket error: {:?}", error),
				ConnectionError::ConnectionFailed,
			);
			if response.send(Err(err)).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Disconnected;
		}
	}

	HandleResult::Ok
}

// ============================================================================
// Response Handling
// ============================================================================

/// Handle a response from the server.
///
/// This processes incoming messages and routes them to the appropriate
/// pending request or live query handler.
async fn handle_response<M, S, E>(
	message: &M,
	max_message_size: Option<usize>,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	// Check if this message should be processed
	if !message.should_process() {
		trace!("Received {}", message.log_description());
		return HandleResult::Ok;
	}

	// Get binary data
	let Some(binary) = message.as_binary() else {
		trace!("Received non-binary message");
		return HandleResult::Ok;
	};

	match surrealdb_rpc::db_response_from_bytes(binary) {
		Ok(response) => {
			handle_db_response::<M, S, E>(response, max_message_size, sessions, sink).await
		}
		Err(error) => {
			handle_parse_error(
				Error::serialization(error.to_string(), SerializationError::Deserialization),
				binary,
				sessions,
			)
			.await
		}
	}
}

/// Handle a successfully parsed database response.
async fn handle_db_response<M, S, E>(
	response: DbResponse,
	max_message_size: Option<usize>,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	let Some(session_id) = response.session_id else {
		return HandleResult::Ok;
	};

	let session_state = match sessions.get(&session_id) {
		Some(Ok(state)) => state,
		_ => return HandleResult::Ok,
	};

	match response.id {
		// Normal response with ID
		Some(id) => {
			if let Value::Number(Number::Int(id_num)) = id {
				handle_response_with_id::<M, S, E>(
					id_num,
					response.result,
					session_id,
					&session_state,
					max_message_size,
					sessions,
					sink,
				)
				.await
			} else {
				HandleResult::Ok
			}
		}
		// Live query notification (no ID)
		None => {
			handle_live_notification::<M, S, E>(response.result, session_id, &session_state, sink)
				.await
		}
	}
}

/// Handle a response that has an ID (normal request/response).
async fn handle_response_with_id<M, S, E>(
	id: i64,
	result: Result<DbResult, TypesError>,
	session_id: Uuid,
	session_state: &Arc<SessionState>,
	max_message_size: Option<usize>,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	// An acknowledgement of a replayed setup command rather than a reply to a
	// caller's request.
	if let Some(cursor) = replay_cursor(session_state)
		&& cursor.awaiting == id
	{
		if let Err(error) = result {
			// An expired access token on a replayed `authenticate` is routine:
			// the handle has been idle past the token's lifetime, or is being
			// cloned after it elapsed. The token carries its own refresh half,
			// so retry the command with it rather than abandoning the session —
			// poisoning here would permanently disable a handle that holds
			// perfectly good credentials, since a poisoned session is never
			// replayed again and cannot route even an explicit refresh.
			if !cursor.refreshed
				&& let Some(Command::Authenticate {
					token,
				}) = session_state.replay.get(cursor.index)
				&& let Token::WithRefresh {
					..
				} = token && error
				.not_allowed_details()
				.is_some_and(|a| matches!(a, NotAllowedError::Auth(AuthError::TokenExpired)))
			{
				let refresh_request = RouterRequest {
					id: Some(id),
					method: "authenticate",
					params: Some(Value::Array(Array::from(vec![token.clone().into_value()]))),
					txn: None,
					session_id: Some(session_id),
				};
				let message: M = serialize_request(refresh_request);

				if let Err(send_error) = send_message(sink, message).await {
					trace!("failed to send refresh query to the server; {send_error:?}");
					fail_replay(session_state, sessions, session_id, error).await;
				} else {
					// Same command, same id: stay parked on it until the
					// refreshed attempt is answered.
					set_replay_cursor(
						session_state,
						Some(ReplayCursor {
							refreshed: true,
							..cursor
						}),
					);
				}
				return HandleResult::Ok;
			}

			fail_replay(session_state, sessions, session_id, error).await;
			return HandleResult::Ok;
		}

		// This command has been applied, so the next one can go out. Sending
		// them one at a time is what keeps an order-sensitive log — a `set`
		// followed by an `unset` of the same key — from being applied in the
		// wrong order by a server that dispatches concurrently.
		if send_replay_command::<M, S, E>(session_id, session_state, cursor.index + 1, sink).await {
			return HandleResult::Ok;
		}

		// The log is exhausted (or the connection broke, which will reconnect
		// and replay again), so release anything parked behind the replay.
		return flush_deferred_routes::<M, S, E>(session_state, max_message_size, sink).await;
	}

	let Some(mut pending) = session_state.pending_requests.take(&id) else {
		warn!("got response for request with id '{id}', which was not in pending requests");
		return HandleResult::Ok;
	};

	match result {
		Ok(DbResult::Query(results)) => {
			if let Some(command) = pending.command {
				super::record_replayable(&session_state.replay, command);
			}
			if let Err(err) = pending.response_channel.send(Ok(results)).await {
				tracing::error!("Failed to send query results to channel: {err:?}");
			}
		}
		Ok(DbResult::Live(_)) => {
			tracing::error!("Unexpected live query result in response");
		}
		Ok(DbResult::Other(mut value)) => {
			if let Some(command) = pending.command {
				if let Command::Authenticate {
					token,
					..
				} = &command
				{
					value = token.clone().into_value();
				}
				super::record_replayable(&session_state.replay, command);
			}
			let result = QueryResultBuilder::started_now().finish_with_result(Ok(value));
			if let Err(err) = pending.response_channel.send(Ok(vec![result])).await {
				tracing::error!("Failed to send query results to channel: {err:?}");
			}
		}
		Err(error) => {
			// Handle automatic token refresh
			if let Some(Command::Authenticate {
				token,
				..
			}) = pending.command
				&& let Token::WithRefresh {
					..
				} = &token && error
				.not_allowed_details()
				.is_some_and(|a| matches!(a, NotAllowedError::Auth(AuthError::TokenExpired)))
			{
				// Attempt automatic refresh
				let refresh_request = RouterRequest {
					id: Some(id),
					method: "authenticate",
					params: Some(Value::Array(Array::from(vec![token.into_value()]))),
					txn: None,
					session_id: Some(session_id),
				};
				let message: M = serialize_request(refresh_request);

				match send_message(sink, message).await {
					Err(send_error) => {
						trace!("failed to send refresh query to the server; {send_error:?}");
						pending.response_channel.send(Err(error)).await.ok();
					}
					Ok(..) => {
						// Keep request pending for retry after refresh
						pending.command = None;
						session_state.pending_requests.insert(id, pending);
					}
				}
				return HandleResult::Ok;
			}

			// Return error to caller
			pending.response_channel.send(Err(error)).await.ok();
		}
	}

	HandleResult::Ok
}

/// Park a request behind a session's in-flight setup.
fn park_route(session_state: &SessionState, route: Route) {
	match session_state.deferred.lock() {
		Ok(mut deferred) => deferred.push(route),
		Err(poisoned) => poisoned.into_inner().push(route),
	}
}

/// Take everything parked behind a session's setup.
///
/// The lock is released before the caller awaits on any of the routes, and only
/// the router task ever touches the queue.
fn take_deferred_routes(session_state: &SessionState) -> Vec<Route> {
	match session_state.deferred.lock() {
		Ok(mut deferred) => std::mem::take(&mut *deferred),
		Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
	}
}

/// Dispatch the requests parked while a session's setup was in flight.
async fn flush_deferred_routes<M, S, E>(
	session_state: &Arc<SessionState>,
	max_message_size: Option<usize>,
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	let mut outcome = HandleResult::Ok;
	for route in take_deferred_routes(session_state) {
		if let HandleResult::Disconnected =
			dispatch_route::<M, S, E>(route, max_message_size, session_state, sink).await
		{
			outcome = HandleResult::Disconnected;
		}
	}
	outcome
}

/// Fail the requests parked behind a session setup that did not complete.
async fn fail_deferred_routes(session_state: &SessionState, error: TypesError) {
	for route in take_deferred_routes(session_state) {
		route.response.send(Err(error.clone())).await.ok();
	}
}

/// Abandon the replay in progress: poison the session and fail everything
/// parked behind it.
///
/// Used when a replayed setup command is rejected, or when its reply cannot be
/// read. Either way the session never reached the state it was cloned or
/// reconnected into, so it is not the session the caller asked for, and the
/// acknowledgement the replay is waiting on is never going to arrive.
async fn fail_replay(
	session_state: &SessionState,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	session_id: Uuid,
	error: TypesError,
) {
	set_replay_cursor(session_state, None);
	sessions.insert(session_id, Err(SessionError::Remote(error.to_string())));
	fail_deferred_routes(session_state, error).await;
}

/// Handle a live query notification.
async fn handle_live_notification<M, S, E>(
	result: Result<DbResult, TypesError>,
	session_id: Uuid,
	session_state: &Arc<SessionState>,
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	if let Ok(DbResult::Live(notification)) = result {
		let live_query_id = notification.id.into_inner();

		if let Some(sender) = session_state.live_queries.get(&live_query_id)
			&& sender.send(Ok(notification)).await.is_err()
		{
			// Receiver dropped, kill the live query
			session_state.live_queries.remove(&live_query_id);
			let kill: M = create_kill_message(live_query_id, session_id);

			if let Err(error) = send_message(sink, kill).await {
				trace!("failed to send kill query to the server; {error:?}");
				return HandleResult::Disconnected;
			}
		}
	}

	HandleResult::Ok
}

/// Handle a parse error by trying to extract the ID and return the error.
async fn handle_parse_error(
	error: crate::Error,
	binary: &[u8],
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
) -> HandleResult {
	#[derive(SurrealValue)]
	#[surreal(crate = "crate::types")]
	struct ErrorResponse {
		id: Option<Value>,
		#[surreal(rename = "session")]
		session_id: Option<Uuid>,
	}

	match surrealdb_types::decode::<ErrorResponse>(binary) {
		Ok(ErrorResponse {
			id,
			session_id,
		}) => {
			let Some(session_id) = session_id else {
				return HandleResult::Ok;
			};

			let session_state = match sessions.get(&session_id) {
				Some(Ok(state)) => state,
				_ => return HandleResult::Ok,
			};

			match id {
				Some(Value::Number(Number::Int(id_num))) => {
					if let Some(pending) = session_state.pending_requests.take(&id_num) {
						let _ = pending.response_channel.send(Err(error)).await;
					} else if replay_cursor(&session_state).is_some_and(|c| c.awaiting == id_num) {
						// A replayed setup command whose reply we cannot read. The
						// session's state on the server is now unknown, so poison it
						// and fail the requests parked behind the replay instead of
						// waiting for an acknowledgement that will never come.
						fail_replay(&session_state, sessions, session_id, error).await;
					} else {
						warn!(
							"got response for request with id '{id_num}', which was not in pending requests"
						);
					}
				}
				// The envelope named a session but carries no usable request id,
				// so we cannot tell which request it answers. Treat a replay in
				// flight the same as the fully undecodable case: its
				// acknowledgement may be exactly what this frame was, and leaving
				// the cursor set would park every later request for the session
				// forever.
				_ => {
					if replay_cursor(&session_state).is_some() {
						fail_replay(&session_state, sessions, session_id, error).await;
					}
				}
			}
		}
		_ => {
			// The payload could not be decoded far enough to recover the request
			// id, so we cannot route the error to a single waiting request. Rather
			// than silently dropping it — which leaves every awaiting query hanging
			// forever (https://github.com/surrealdb/surrealdb/issues/7037) — fail
			// all currently-pending requests so callers surface the deserialization
			// error instead of blocking indefinitely.
			error!("Failed to deserialise message, failing pending requests; {error:?}");
			fail_all_pending_requests(sessions, error).await;
		}
	}

	HandleResult::Ok
}

/// Fail every pending request across all sessions with the given error.
///
/// Used when an incoming message cannot be parsed well enough to identify which
/// request it belongs to; failing the requests prevents them from hanging.
async fn fail_all_pending_requests(
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	error: crate::Error,
) {
	for (session_id, session) in sessions.to_vec() {
		let Ok(session_state) = session else {
			continue;
		};
		for (id, _) in session_state.pending_requests.to_vec() {
			if let Some(pending) = session_state.pending_requests.take(&id) {
				pending.response_channel.send(Err(error.clone())).await.ok();
			}
		}
		// The unreadable frame may have been the acknowledgement a replay was
		// waiting on. A session mid-replay would otherwise park every later
		// request behind an acknowledgement that never arrives.
		if replay_cursor(&session_state).is_some() {
			fail_replay(&session_state, sessions, session_id, error.clone()).await;
		}
	}
}

// ============================================================================
// Session Management
// ============================================================================

/// Establish a session's state on the server by replaying its command log.
///
/// Used both to build a newly registered or cloned session and to rebuild every
/// session after a reconnect.
///
/// Each command is sent with a request id recorded in
/// [`SessionState::replay_acks`], so the router can tell when the server has
/// applied the whole batch. Requests for the session are parked until it has:
/// the server may apply requests that arrive together on one connection in any
/// order, so a request dispatched alongside the replay can observe a session
/// whose namespace, authentication or variables are not in place yet.
async fn replay_session<M, S, E>(
	session_id: Uuid,
	session_state: &SessionState,
	sink: &RwLock<S>,
) -> crate::Result<()>
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	// This replay supersedes any earlier one: acknowledgements for a command
	// sent on a connection that has since been replaced will never arrive, and
	// leaving the cursor on it would park this session's requests forever.
	send_replay_command::<M, S, E>(session_id, session_state, 0, sink).await;
	Ok(())
}

/// Send the replay command at `index` and record it as the acknowledgement the
/// session is waiting on.
///
/// Returns whether a command is now in flight. `false` means the session is no
/// longer replaying, either because the log is exhausted or because the command
/// could not be sent.
async fn send_replay_command<M, S, E>(
	session_id: Uuid,
	session_state: &SessionState,
	index: usize,
	sink: &RwLock<S>,
) -> bool
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	let Some(command) = session_state.replay.get(index) else {
		set_replay_cursor(session_state, None);
		return false;
	};

	let id = session_state.last_id.fetch_add(1, Ordering::SeqCst);
	set_replay_cursor(
		session_state,
		Some(ReplayCursor {
			awaiting: id,
			index,
			refreshed: false,
		}),
	);

	let request = command
		.clone()
		.into_router_request(Some(id), Some(session_id))
		.expect("replay commands should always convert to route requests");

	let message: M = serialize_request(request);

	if let Err(error) = send_message(sink, message).await {
		// The command never reached the server, so nothing will ever
		// acknowledge it. Drop the cursor rather than park this session's
		// requests behind it; a broken connection is replayed again on
		// reconnect.
		set_replay_cursor(session_state, None);
		debug!("{:?}", error);
		return false;
	}

	true
}

/// The replay command this session is currently waiting to have acknowledged.
fn replay_cursor(session_state: &SessionState) -> Option<ReplayCursor> {
	match session_state.replay_cursor.lock() {
		Ok(cursor) => *cursor,
		Err(poisoned) => *poisoned.into_inner(),
	}
}

fn set_replay_cursor(session_state: &SessionState, cursor: Option<ReplayCursor>) {
	match session_state.replay_cursor.lock() {
		Ok(mut slot) => *slot = cursor,
		Err(poisoned) => *poisoned.into_inner() = cursor,
	}
}

/// Handle new session registration.
async fn handle_session_initial<M, S, E>(
	session_id: Uuid,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	let session_state = Arc::new(SessionState::default());
	session_state.replay.push(Command::Attach {
		session_id,
	});
	sessions.insert(session_id, Ok(Arc::clone(&session_state)));

	if let Err(error) = replay_session::<M, S, E>(session_id, &session_state, sink).await {
		sessions.insert(session_id, Err(SessionError::Remote(error.to_string())));
	}
}

/// Handle session cloning.
async fn handle_session_clone<M, S, E>(
	old: Uuid,
	new: Uuid,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	match sessions.get(&old) {
		Some(Ok(session_state)) => {
			let mut session_state = session_state.as_ref().clone();
			// Replace the attach command with the new session id
			if let Some(cmd) = session_state.replay.get_mut(0) {
				*cmd = Command::Attach {
					session_id: new,
				};
			}
			let session_state = Arc::new(session_state);
			sessions.insert(new, Ok(Arc::clone(&session_state)));

			if let Err(error) = replay_session::<M, S, E>(new, &session_state, sink).await {
				sessions.insert(new, Err(SessionError::Remote(error.to_string())));
			}
		}
		Some(Err(error)) => {
			sessions.insert(new, Err(error));
		}
		None => {
			sessions.insert(new, Err(SessionError::NotFound(old)));
		}
	}
}

/// Handle session drop.
async fn handle_session_drop<M, S, E>(
	session_id: Uuid,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	if sessions.get(&session_id).is_some() {
		// Fire-and-forget: the session is going away, so there is nothing left
		// to order the detach against and no one to hand an acknowledgement to.
		let request = Command::Detach {
			session_id,
		}
		.into_router_request(None, Some(session_id))
		.expect("detach should always convert to a route request");

		let message: M = serialize_request(request);

		if let Err(error) = send_message(sink, message).await {
			debug!("{:?}", error);
		}
	}
	sessions.remove(&session_id);
}

/// Dispatch a session-lifecycle event to the appropriate handler.
async fn handle_session<M, S, E>(
	session_id: crate::SessionId,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
	sink: &RwLock<S>,
) where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	match session_id {
		crate::SessionId::Initial(id) => {
			handle_session_initial::<M, S, E>(id, sessions, sink).await
		}
		crate::SessionId::Clone {
			old,
			new,
		} => handle_session_clone::<M, S, E>(old, new, sessions, sink).await,
		crate::SessionId::Drop(id) => handle_session_drop::<M, S, E>(id, sessions, sink).await,
	}
}

/// Clear all pending requests on connection reset.
///
/// Requests still parked behind a session's setup are failed the same way as
/// ones already on the wire: the connection they were waiting on is gone.
async fn clear_pending_requests(sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>) {
	for state in sessions.values().into_iter().flatten() {
		for request in state.pending_requests.values() {
			let err = crate::Error::connection(
				"Connection reset".to_string(),
				surrealdb_types::ConnectionError::ConnectionFailed,
			);
			request.response_channel.send(Err(err)).await.ok();
			request.response_channel.close();
		}
		state.pending_requests.clear();

		let err = crate::Error::connection(
			"Connection reset".to_string(),
			surrealdb_types::ConnectionError::ConnectionFailed,
		);
		fail_deferred_routes(&state, err).await;
	}
}

/// Clear all live queries on connection reset.
async fn clear_live_queries(sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>) {
	for state in sessions.values().into_iter().flatten() {
		for sender in state.live_queries.values() {
			let err = crate::Error::connection(
				"Connection reset".to_string(),
				surrealdb_types::ConnectionError::ConnectionFailed,
			);
			sender.send(Err(err)).await.ok();
			sender.close();
		}
		state.live_queries.clear();
	}
}

/// Reset all sessions on disconnect.
async fn reset_sessions(sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>) {
	tokio::join!(clear_pending_requests(sessions), clear_live_queries(sessions));
}

// ============================================================================
// Public Types
// ============================================================================

/// The WS scheme used to connect to `ws://` endpoints
#[derive(Debug)]
pub struct Ws;

/// The WSS scheme used to connect to `wss://` endpoints
#[derive(Debug)]
pub struct Wss;

/// A WebSocket client for communicating with the server via WebSockets
#[derive(Debug, Clone)]
pub struct Client(());

impl Surreal<Client> {
	/// Connects to a specific database endpoint, saving the connection on the
	/// static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use std::sync::LazyLock;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::ws::Client;
	/// use surrealdb::engine::remote::ws::Ws;
	///
	/// static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect::<Ws>("localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect<P>(
		&self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			surreal: Arc::clone(&self.inner).into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_rpc::{DbResult, QueryResult, Token};
	use surrealdb_types::{AuthError, Error as TypesError, NotAllowedError};
	use tokio::sync::RwLock;
	use uuid::Uuid;

	use super::{
		HandleResult, PendingRequest, SessionState, WsMessage, fail_all_pending_requests,
		handle_parse_error, handle_response_with_id, handle_route, replay_cursor, replay_session,
	};
	use crate::conn::{Command, RequestData, Route};
	use crate::engine::SessionError;
	use crate::types::{HashMap, Number, Value};

	type Sessions = HashMap<Uuid, Result<Arc<SessionState>, SessionError>>;

	/// An empty session registry: these tests drive the pending-request path,
	/// which never consults it.
	fn no_sessions() -> Sessions {
		HashMap::new()
	}

	/// Mock WebSocket message for testing.
	#[derive(Clone)]
	struct MockMessage;

	impl WsMessage for MockMessage {
		fn binary(_payload: Vec<u8>) -> Self {
			MockMessage
		}

		fn as_binary(&self) -> Option<&[u8]> {
			None
		}
	}

	#[tokio::test]
	async fn handle_response_removes_pending_request() {
		let session_state = Arc::new(SessionState::default());
		let sessions = no_sessions();
		let session_id = Uuid::new_v4();
		let request_id: i64 = 1;

		// Insert a pending request
		let (sender, receiver) = async_channel::bounded(1);
		session_state.pending_requests.insert(
			request_id,
			PendingRequest {
				command: None,
				response_channel: sender,
			},
		);
		assert_eq!(session_state.pending_requests.len(), 1);

		// Handle a successful response
		let sink = RwLock::new(futures::sink::drain::<MockMessage>());
		let result = handle_response_with_id::<MockMessage, _, _>(
			request_id,
			Ok(DbResult::Other(Value::None)),
			session_id,
			&session_state,
			None,
			&sessions,
			&sink,
		)
		.await;

		// Entry should be removed from pending_requests
		assert_eq!(result, HandleResult::Ok);
		assert!(
			session_state.pending_requests.is_empty(),
			"pending request should be removed after handling response"
		);

		// Response should have been delivered to the receiver
		let response = receiver.recv().await.unwrap();
		assert!(response.is_ok());
	}

	#[tokio::test]
	async fn handle_response_error_removes_pending_request() {
		let session_state = Arc::new(SessionState::default());
		let sessions = no_sessions();
		let session_id = Uuid::new_v4();
		let request_id: i64 = 1;

		// Insert a pending request (no replayable command, so no token refresh path)
		let (sender, receiver) = async_channel::bounded(1);
		session_state.pending_requests.insert(
			request_id,
			PendingRequest {
				command: None,
				response_channel: sender,
			},
		);
		assert_eq!(session_state.pending_requests.len(), 1);

		// Handle an error response
		let sink = RwLock::new(futures::sink::drain::<MockMessage>());
		let error = TypesError::internal("test error".to_string());
		let result = handle_response_with_id::<MockMessage, _, _>(
			request_id,
			Err(error),
			session_id,
			&session_state,
			None,
			&sessions,
			&sink,
		)
		.await;

		// Entry should be removed from pending_requests
		assert_eq!(result, HandleResult::Ok);
		assert!(
			session_state.pending_requests.is_empty(),
			"pending request should be removed after handling error response"
		);

		// Error should have been delivered to the receiver
		let response = receiver.recv().await.unwrap();
		assert!(response.is_err());
	}

	#[tokio::test]
	async fn handle_multiple_responses_cleans_up_all_entries() {
		let session_state = Arc::new(SessionState::default());
		let sessions = no_sessions();
		let session_id = Uuid::new_v4();
		let sink = RwLock::new(futures::sink::drain::<MockMessage>());

		// Insert many pending requests
		let mut receivers = Vec::new();
		for id in 0..100i64 {
			let (sender, receiver) = async_channel::bounded(1);
			session_state.pending_requests.insert(
				id,
				PendingRequest {
					command: None,
					response_channel: sender,
				},
			);
			receivers.push(receiver);
		}
		assert_eq!(session_state.pending_requests.len(), 100);

		// Handle all responses
		for id in 0..100i64 {
			handle_response_with_id::<MockMessage, _, _>(
				id,
				Ok(DbResult::Other(Value::None)),
				session_id,
				&session_state,
				None,
				&sessions,
				&sink,
			)
			.await;
		}

		// All entries should have been removed
		assert!(
			session_state.pending_requests.is_empty(),
			"all pending requests should be removed, but {} remain",
			session_state.pending_requests.len()
		);

		// All responses should have been delivered
		for receiver in &receivers {
			let response = receiver.recv().await.unwrap();
			assert!(response.is_ok());
		}
	}

	/// A registered session with `commands` queued for replay, none sent yet.
	fn session_with_replay(commands: Vec<Command>) -> (Uuid, Arc<SessionState>, Sessions) {
		let session_id = Uuid::new_v4();
		let session_state = Arc::new(SessionState::default());
		for command in commands {
			session_state.replay.push(command);
		}

		let sessions = HashMap::new();
		sessions.insert(session_id, Ok(Arc::clone(&session_state)));

		(session_id, session_state, sessions)
	}

	fn set_cmd(key: &str, value: i64) -> Command {
		Command::Set {
			key: key.to_string(),
			value: Value::Number(Number::Int(value)),
		}
	}

	/// Acknowledge the replay command currently in flight.
	async fn ack_replay<S>(
		session_id: Uuid,
		session_state: &Arc<SessionState>,
		sessions: &Sessions,
		sink: &RwLock<S>,
		result: Result<DbResult, TypesError>,
	) where
		S: futures::Sink<MockMessage, Error = std::convert::Infallible> + Unpin,
	{
		let cursor = replay_cursor(session_state).expect("a replay command should be in flight");
		handle_response_with_id::<MockMessage, _, _>(
			cursor.awaiting,
			result,
			session_id,
			session_state,
			None,
			sessions,
			sink,
		)
		.await;
	}

	fn route_for(
		session_id: Uuid,
	) -> (Route, async_channel::Receiver<Result<Vec<QueryResult>, TypesError>>) {
		let (response, receiver) = async_channel::bounded(1);
		let route = Route {
			request: RequestData {
				command: Command::Health,
				session_id,
			},
			response,
		};
		(route, receiver)
	}

	/// Regression for the clone-setup race. A session inherits its namespace,
	/// authentication and variables by replaying commands at the server, which is
	/// free to apply requests that arrive together in any order. So a request must
	/// not go out while that replay is unacknowledged, and must be released once
	/// it lands — otherwise it can execute against a half-built session.
	#[tokio::test]
	async fn route_waits_for_replay_acknowledgement() {
		let (session_id, session_state, sessions) = session_with_replay(vec![set_cmd("x", 1)]);
		let sink = RwLock::new(Vec::<MockMessage>::new());

		replay_session::<MockMessage, _, _>(session_id, &session_state, &sink).await.unwrap();
		assert_eq!(sink.read().await.len(), 1, "the replay command should be on the wire");

		let (route, _receiver) = route_for(session_id);
		assert_eq!(
			handle_route::<MockMessage, _, _>(route, None, &sessions, &sink).await,
			HandleResult::Ok
		);
		assert_eq!(
			sink.read().await.len(),
			1,
			"request must not reach the wire before the session's setup is acknowledged"
		);
		assert_eq!(session_state.deferred.lock().unwrap().len(), 1, "request should be parked");

		// The acknowledgement lands, completing the session's setup.
		ack_replay(session_id, &session_state, &sessions, &sink, Ok(DbResult::Other(Value::None)))
			.await;

		assert!(replay_cursor(&session_state).is_none(), "replay should be finished");
		assert!(
			session_state.deferred.lock().unwrap().is_empty(),
			"parked request should be released once setup is acknowledged"
		);
		assert_eq!(sink.read().await.len(), 2, "released request should be sent");
		assert_eq!(session_state.pending_requests.len(), 1, "released request should be pending");
	}

	/// The replay log is order-sensitive: two `set`s of one key leave different
	/// end states depending on which lands last. The server may apply requests
	/// that arrive together in any order, so the commands must go one at a time,
	/// each only after the previous has been acknowledged.
	#[tokio::test]
	async fn replay_commands_are_sent_one_at_a_time() {
		let (session_id, session_state, sessions) =
			session_with_replay(vec![set_cmd("x", 1), set_cmd("x", 2)]);
		let sink = RwLock::new(Vec::<MockMessage>::new());

		replay_session::<MockMessage, _, _>(session_id, &session_state, &sink).await.unwrap();
		assert_eq!(
			sink.read().await.len(),
			1,
			"only the first command may be in flight; the second would be free to overtake it"
		);
		let first = replay_cursor(&session_state).expect("first command in flight");

		ack_replay(session_id, &session_state, &sessions, &sink, Ok(DbResult::Other(Value::None)))
			.await;

		assert_eq!(
			sink.read().await.len(),
			2,
			"the second command should follow its predecessor's acknowledgement"
		);
		let second = replay_cursor(&session_state).expect("second command in flight");
		assert_ne!(second.awaiting, first.awaiting, "each command needs its own request id");

		ack_replay(session_id, &session_state, &sessions, &sink, Ok(DbResult::Other(Value::None)))
			.await;

		assert!(
			replay_cursor(&session_state).is_none(),
			"the session should be ready once the log is exhausted"
		);
	}

	fn refreshable_auth_cmd() -> Command {
		Command::Authenticate {
			token: Token::WithRefresh {
				access: "expired-access".to_string(),
				refresh: "valid-refresh".to_string(),
			},
		}
	}

	fn token_expired() -> TypesError {
		TypesError::not_allowed(
			"token expired".to_string(),
			NotAllowedError::Auth(AuthError::TokenExpired),
		)
	}

	/// An access token that expired while the handle sat idle is routine, and the
	/// token carries its own refresh half. Poisoning the session would strand a
	/// handle that holds perfectly good credentials — a poisoned session is never
	/// replayed again and cannot route even an explicit refresh — so the command
	/// is retried once with the refresh token instead.
	#[tokio::test]
	async fn expired_token_during_replay_is_refreshed_not_poisoned() {
		let (session_id, session_state, sessions) =
			session_with_replay(vec![refreshable_auth_cmd(), set_cmd("x", 1)]);
		let sink = RwLock::new(Vec::<MockMessage>::new());

		replay_session::<MockMessage, _, _>(session_id, &session_state, &sink).await.unwrap();
		let first = replay_cursor(&session_state).expect("authenticate in flight");

		ack_replay(session_id, &session_state, &sessions, &sink, Err(token_expired())).await;

		assert!(
			matches!(sessions.get(&session_id), Some(Ok(_))),
			"a refreshable expiry must not poison the session"
		);
		let retry = replay_cursor(&session_state).expect("replay should still be in flight");
		assert_eq!(retry.index, first.index, "the retry re-sends the same command");
		assert!(retry.refreshed, "the retry should be recorded so it happens only once");
		assert_eq!(sink.read().await.len(), 2, "the refreshed authenticate should be sent");

		// The refreshed attempt succeeds, so the replay carries on to the `set`.
		ack_replay(session_id, &session_state, &sessions, &sink, Ok(DbResult::Other(Value::None)))
			.await;

		let next = replay_cursor(&session_state).expect("set should now be in flight");
		assert_eq!(next.index, first.index + 1, "the replay should advance past authenticate");
		assert!(!next.refreshed, "a fresh command starts with no retry spent");
	}

	/// The retry is bounded: an expiry the refresh token cannot fix must fail the
	/// session rather than loop.
	#[tokio::test]
	async fn expired_token_is_refreshed_only_once() {
		let (session_id, session_state, sessions) =
			session_with_replay(vec![refreshable_auth_cmd()]);
		let sink = RwLock::new(Vec::<MockMessage>::new());

		replay_session::<MockMessage, _, _>(session_id, &session_state, &sink).await.unwrap();
		ack_replay(session_id, &session_state, &sessions, &sink, Err(token_expired())).await;
		assert!(replay_cursor(&session_state).is_some_and(|c| c.refreshed));

		// The refreshed attempt is rejected too.
		ack_replay(session_id, &session_state, &sessions, &sink, Err(token_expired())).await;

		assert!(replay_cursor(&session_state).is_none(), "the replay should be abandoned");
		assert!(
			matches!(sessions.get(&session_id), Some(Err(_))),
			"a refresh that cannot recover should poison the session"
		);
		assert_eq!(sink.read().await.len(), 2, "no further refresh attempts");
	}

	/// A reply whose envelope decodes but carries no usable request id could be
	/// the acknowledgement a replay is waiting on. Leaving the cursor set would
	/// park every later request for that session forever.
	#[tokio::test]
	async fn response_without_usable_id_releases_a_parked_replay() {
		let (session_id, session_state, sessions) = session_with_replay(vec![set_cmd("x", 1)]);
		let sink = RwLock::new(Vec::<MockMessage>::new());

		replay_session::<MockMessage, _, _>(session_id, &session_state, &sink).await.unwrap();
		let (route, receiver) = route_for(session_id);
		handle_route::<MockMessage, _, _>(route, None, &sessions, &sink).await;
		assert_eq!(session_state.deferred.lock().unwrap().len(), 1);

		// An error envelope naming the session but with no id at all.
		let mut envelope = crate::types::Object::new();
		envelope.insert("id".to_string(), Value::None);
		envelope.insert("session".to_string(), Value::Uuid(session_id.into()));
		let binary = surrealdb_types::encode(&Value::Object(envelope)).unwrap();

		handle_parse_error(TypesError::internal("unreadable".to_string()), &binary, &sessions)
			.await;

		assert!(replay_cursor(&session_state).is_none(), "the stuck replay should be abandoned");
		assert!(
			receiver.recv().await.unwrap().is_err(),
			"the parked request should be failed, not left hanging"
		);
	}

	/// A reply that cannot be decoded is the acknowledgement a replay was waiting
	/// on. Without releasing it the session parks every later request behind an
	/// acknowledgement that will never arrive.
	#[tokio::test]
	async fn undecodable_response_releases_a_parked_replay() {
		let (session_id, session_state, sessions) = session_with_replay(vec![set_cmd("x", 1)]);
		let sink = RwLock::new(Vec::<MockMessage>::new());

		replay_session::<MockMessage, _, _>(session_id, &session_state, &sink).await.unwrap();

		let (route, receiver) = route_for(session_id);
		handle_route::<MockMessage, _, _>(route, None, &sessions, &sink).await;
		assert_eq!(session_state.deferred.lock().unwrap().len(), 1);

		fail_all_pending_requests(&sessions, TypesError::internal("undecodable".to_string())).await;

		assert!(replay_cursor(&session_state).is_none(), "the stuck replay should be abandoned");
		assert!(
			receiver.recv().await.unwrap().is_err(),
			"the parked request should be failed, not left hanging"
		);
		assert!(
			matches!(sessions.get(&session_id), Some(Err(_))),
			"a session whose setup cannot be confirmed should be poisoned"
		);
	}

	/// A session whose setup the server rejected never reaches the state the
	/// caller asked for, so it is poisoned and the requests parked behind it are
	/// failed rather than run against a half-built session.
	#[tokio::test]
	async fn failed_replay_acknowledgement_fails_parked_requests() {
		let (session_id, session_state, sessions) = session_with_replay(vec![set_cmd("x", 1)]);
		let sink = RwLock::new(Vec::<MockMessage>::new());

		replay_session::<MockMessage, _, _>(session_id, &session_state, &sink).await.unwrap();
		let sent_during_replay = sink.read().await.len();

		let (route, receiver) = route_for(session_id);
		handle_route::<MockMessage, _, _>(route, None, &sessions, &sink).await;
		assert_eq!(session_state.deferred.lock().unwrap().len(), 1);

		ack_replay(
			session_id,
			&session_state,
			&sessions,
			&sink,
			Err(TypesError::internal("setup rejected".to_string())),
		)
		.await;

		assert_eq!(
			sink.read().await.len(),
			sent_during_replay,
			"a rejected setup must not release the request"
		);
		assert!(
			receiver.recv().await.unwrap().is_err(),
			"the parked request should be failed, not left hanging"
		);
		assert!(
			matches!(sessions.get(&session_id), Some(Err(_))),
			"the session should be poisoned once its setup is rejected"
		);
	}
}
