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
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_channel::Sender;
use futures::{Sink, SinkExt};
use surrealdb_core::dbs::{QueryResult, QueryResultBuilder};
use surrealdb_core::iam::token::Token;
use surrealdb_core::rpc::{DbResponse, DbResult};
use surrealdb_types::Error as TypesError;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::conn::{Command, RequestData, Route};
use crate::engine::SessionError;
use crate::engine::remote::RouterRequest;
use crate::err::Error;
use crate::opt::IntoEndpoint;
use crate::types::{Array, HashMap, Notification, Number, SurrealValue, Value};
use crate::{Connect, Surreal};

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
	/// The last ID used for a request
	last_id: AtomicI64,
}

impl Default for SessionState {
	fn default() -> Self {
		Self {
			pending_requests: HashMap::new(),
			live_queries: HashMap::new(),
			replay: boxcar::Vec::new(),
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
	let payload = surrealdb_core::rpc::format::flatbuffers::encode(&request_value)
		.expect("router request should serialize");
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
	Route {
		request,
		response,
	}: Route,
	max_message_size: Option<usize>,
	sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>,
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

	// Get session state
	let session_state = match sessions.get(&session_id) {
		Some(Ok(state)) => state,
		Some(Err(error)) => {
			if response.send(Err(Error::from(error).into())).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Ok;
		}
		None => {
			let error = Error::from(SessionError::NotFound(session_id));
			if response.send(Err(error.into())).await.is_err() {
				trace!("Receiver dropped");
			}
			return HandleResult::Ok;
		}
	};

	// Generate a new request ID
	let id = session_state.last_id.fetch_add(1, Ordering::SeqCst);

	// Check for duplicate request IDs
	if session_state.pending_requests.contains_key(&id) {
		let error = Error::DuplicateRequestId(id);
		if response.send(Err(error.into())).await.is_err() {
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
		response.send(Err(Error::BackupsNotSupported.into())).await.ok();
		return HandleResult::Ok;
	};

	let message: M = serialize_request(router_request);

	// Check message size
	if let Some(max_size) = max_message_size
		&& let Some(binary) = message.as_binary()
		&& binary.len() > max_size
	{
		if response.send(Err(Error::MessageTooLong(binary.len()).into())).await.is_err() {
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
			let err = Error::Ws(format!("{:?}", error));
			if response.send(Err(err.into())).await.is_err() {
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

	match DbResponse::from_bytes(binary) {
		Ok(response) => handle_db_response::<M, S, E>(response, sessions, sink).await,
		Err(error) => handle_parse_error(error.into(), binary, sessions).await,
	}
}

/// Handle a successfully parsed database response.
async fn handle_db_response<M, S, E>(
	response: DbResponse,
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
	sink: &RwLock<S>,
) -> HandleResult
where
	M: WsMessage,
	S: Sink<M, Error = E> + Unpin,
	E: std::fmt::Debug,
{
	let Some(mut pending) = session_state.pending_requests.take(&id) else {
		warn!("got response for request with id '{id}', which was not in pending requests");
		return HandleResult::Ok;
	};

	match result {
		Ok(DbResult::Query(results)) => {
			if let Some(command) = pending.command {
				session_state.replay.push(command);
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
				session_state.replay.push(command.clone());
				if let Command::Authenticate {
					token,
					..
				} = command
				{
					value = token.into_value();
				}
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
				} = &token && error.auth_details().is_some_and(|a| a.token_expired)
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
	struct ErrorResponse {
		id: Option<Value>,
		#[surreal(rename = "session")]
		session_id: Option<Uuid>,
	}

	match surrealdb_core::rpc::format::flatbuffers::decode::<ErrorResponse>(binary) {
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

			if let Some(Value::Number(Number::Int(id_num))) = id {
				if let Some(pending) = session_state.pending_requests.take(&id_num) {
					let _ = pending.response_channel.send(Err(error.into())).await;
				} else {
					warn!(
						"got response for request with id '{id_num}', which was not in pending requests"
					);
				}
			}
		}
		_ => {
			warn!("Failed to deserialise message; {error:?}");
		}
	}

	HandleResult::Ok
}

// ============================================================================
// Session Management
// ============================================================================

/// Replay commands for a session after reconnect.
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
	for (_, command) in &session_state.replay {
		let request = command
			.clone()
			.into_router_request(None, Some(session_id))
			.expect("replay commands should always convert to route requests");

		let message: M = serialize_request(request);

		if let Err(error) = send_message(sink, message).await {
			debug!("{:?}", error);
		}
	}

	Ok(())
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
	sessions.insert(session_id, Ok(session_state.clone()));

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
			sessions.insert(new, Ok(session_state.clone()));

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
		let session_state = SessionState::default();
		session_state.replay.push(Command::Detach {
			session_id,
		});
		replay_session::<M, S, E>(session_id, &session_state, sink).await.ok();
	}
	sessions.remove(&session_id);
}

/// Clear all pending requests on connection reset.
async fn clear_pending_requests(sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>) {
	for state in sessions.values().into_iter().flatten() {
		for request in state.pending_requests.values() {
			let error = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
			let err = crate::err::Error::from(error);
			request.response_channel.send(Err(err.into())).await.ok();
			request.response_channel.close();
		}
		state.pending_requests.clear();
	}
}

/// Clear all live queries on connection reset.
async fn clear_live_queries(sessions: &HashMap<Uuid, Result<Arc<SessionState>, SessionError>>) {
	for state in sessions.values().into_iter().flatten() {
		for sender in state.live_queries.values() {
			let error = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
			sender.send(Err(error.into())).await.ok();
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
			surreal: self.inner.clone().into(),
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

	use surrealdb_core::rpc::{DbResult, DbResultError};
	use tokio::sync::RwLock;
	use uuid::Uuid;

	use super::{HandleResult, PendingRequest, SessionState, WsMessage, handle_response_with_id};
	use crate::types::Value;

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
		let error = DbResultError::InternalError("test error".into());
		let result = handle_response_with_id::<MockMessage, _, _>(
			request_id,
			Err(error),
			session_id,
			&session_state,
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
}
