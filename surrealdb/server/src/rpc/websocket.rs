use core::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::ws::close_code::AGAIN;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::{Sink, SinkExt, StreamExt};
use opentelemetry::Context as TelemetryContext;
use opentelemetry::trace::FutureExt;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, LockType, Transaction, TransactionType};
use surrealdb_core::mem::ALLOC;
use surrealdb_core::rpc::format::Format;
use surrealdb_core::rpc::{DbResponse, DbResult, DbResultError, Method, RpcProtocol};
use surrealdb_types::{Array, HashMap, Value};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};
use uuid::Uuid;

use super::RpcState;
use crate::cnf::{
	MAX_TRANSACTIONS_PER_CONNECTION, MAX_TRANSACTIONS_PER_SESSION, PKG_NAME, PKG_VERSION,
	WEBSOCKET_PING_FREQUENCY, WEBSOCKET_RESPONSE_BUFFER_SIZE, WEBSOCKET_RESPONSE_CHANNEL_SIZE,
	WEBSOCKET_RESPONSE_FLUSH_PERIOD,
};
use crate::rpc::CONN_CLOSED_ERR;
use crate::rpc::format::WsFormat;
use crate::telemetry;
use crate::telemetry::metrics::ws::RequestContext;
use crate::telemetry::traces::rpc::span_for_request;

/// An error string sent when the server is out of memory
const SERVER_OVERLOADED: &str = "The server is unable to handle the request";

/// An error string sent when the server is gracefully shutting down
const SERVER_SHUTTING_DOWN: &str = "The server is gracefully shutting down";

pub struct Websocket {
	/// The unique id of this WebSocket connection
	pub(crate) id: Uuid,
	/// The request and response format for messages
	pub(crate) format: Format,
	/// The system state for all RPC WebSocket connections
	pub(crate) state: Arc<RpcState>,
	/// The datastore accessible to all RPC WebSocket connections
	pub(crate) datastore: Arc<Datastore>,
	/// The active sessions for this WebSocket connection
	pub(crate) sessions: HashMap<Option<Uuid>, Arc<RwLock<Session>>>,
	/// The active transactions for this WebSocket connection.
	/// Entries are <transaction_id, (session_id, transaction)> values.
	pub(crate) transactions: DashMap<Uuid, (Option<Uuid>, Arc<Transaction>)>,
	/// The active transaction counts for this WebSocket connection.
	/// Entries are <session_id, count> values.
	pub(crate) counters: DashMap<Option<Uuid>, AtomicUsize>,
	/// A cancellation token called when shutting down the server
	pub(crate) shutdown: CancellationToken,
	/// A cancellation token for cancelling all spawned tasks
	pub(crate) canceller: CancellationToken,
	/// The channels used to send and receive WebSocket messages
	pub(crate) channel: Sender<Message>,
}

impl Websocket {
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
		// Create a channel for sending messages
		let (sender, receiver) = channel(*WEBSOCKET_RESPONSE_CHANNEL_SIZE);
		// Create and store the RPC connection
		let rpc = Arc::new(Websocket {
			id,
			format,
			state: state.clone(),
			shutdown: CancellationToken::new(),
			canceller: CancellationToken::new(),
			sessions: HashMap::new(),
			transactions: DashMap::new(),
			counters: DashMap::new(),
			channel: sender.clone(),
			datastore,
		});
		// Store the default session with None key
		// Enable realtime queries for WebSocket connections
		let session = session.with_rt(true);
		rpc.set_session(None, Arc::new(RwLock::new(session)));
		// Add this WebSocket to the list
		state.web_sockets.write().await.insert(id, rpc.clone());
		// Start telemetry metrics for this connection
		telemetry::metrics::ws::on_connect();
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
				tasks.spawn(Self::ping(rpc.clone(), sender.clone()));
				tasks.spawn(Self::read(rpc.clone(), ws_receiver, sender.clone()));
				tasks.spawn(Self::write(rpc.clone(), ws_sender, receiver));
			}
			false => {
				// Split the socket into sending and receiving streams
				let (ws_sender, ws_receiver) = ws.split();
				// Spawn async tasks for the WebSocket
				tasks.spawn(Self::ping(rpc.clone(), sender.clone()));
				tasks.spawn(Self::read(rpc.clone(), ws_receiver, sender.clone()));
				tasks.spawn(Self::write(rpc.clone(), ws_sender, receiver));
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
		// Cleanup any open transactions for this WebSocket
		rpc.cleanup_all_txns().await;
		// Remove this WebSocket from the list
		state.web_sockets.write().await.remove(&id);
		// Stop telemetry metrics for this connection
		telemetry::metrics::ws::on_disconnect();
	}

	/// Send Ping messages to the client
	async fn ping(rpc: Arc<Websocket>, internal_sender: Sender<Message>) {
		// Create the interval ticker
		let mut interval = tokio::time::interval(WEBSOCKET_PING_FREQUENCY);
		// Clone the WebSocket cancellation token
		let canceller = rpc.canceller.clone();
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
						// Cancel the WebSocket tasks
						canceller.cancel();
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
		// Clone the WebSocket cancellation token
		let canceller = rpc.canceller.clone();
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
					// Check if the socket is buffered
					let res = match buffer {
						// Send the message to the socket buffer
						true => socket.feed(res).await,
						// Send the message direct to the socket
						false => socket.send(res).await
					};
					// Check if there was an error
					if let Err(err) = res {
						// Output any errors if not a close error
						if err.to_string() != CONN_CLOSED_ERR {
							trace!("WebSocket error: {err}");
						}
						// Cancel the WebSocket tasks
						canceller.cancel();
						// Exit out of the loop
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
						// Cancel the WebSocket tasks
						canceller.cancel();
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
	) {
		// Clone the WebSocket shutdown token
		let shutdown = rpc.shutdown.clone();
		// Clone the WebSocket cancellation token
		let canceller = rpc.canceller.clone();
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
								Self::close_socket(rpc.clone(), chn).await;
								// Exit out of the loop
								break;
							}
							// Otherwise spawn and handle the message
							tasks.push(Self::handle_message(&rpc, msg, chn));
						}
						Message::Close(_) => {
							// Respond with a close message
							if let Err(err) = internal_sender.send(Message::Close(None)).await {
								trace!("WebSocket error when replying to the close message: {err}");
							};
							// Cancel the WebSocket tasks
							canceller.cancel();
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
						// Cancel the WebSocket tasks
						canceller.cancel();
						// Exit out of the loop
						break;
					}
				}
			}
		}
		// Continue with the shutdown process
		tokio::select! {
			// Process brances in order
			biased;
			// Check if we have been cancelled
			_ = canceller.cancelled() => (),
			// Check if we are shutting down
			_ = shutdown.cancelled() => {
				// Wait for all tasks to finish
				while tasks.next().await.is_some() {
					// Do nothing
				}
			},
		}
		// Cancel the WebSocket tasks
		canceller.cancel();
		// Ensure everything is dropped
		std::mem::drop(tasks);
	}

	/// Handle an individual WebSocket message
	async fn handle_message(rpc: &Arc<Websocket>, msg: Message, chn: Sender<Message>) {
		// Clone the WebSocket cancellation token
		let shutdown = rpc.shutdown.clone();
		// Clone the WebSocket cancellation token
		let canceller = rpc.canceller.clone();
		// Calculate the message length and format
		let len = match msg {
			Message::Text(ref msg) => msg.len(),
			Message::Binary(ref msg) => msg.len(),
			_ => 0,
		};
		// Prepare span and otel context
		let span = span_for_request(&rpc.id);
		// Parse the request
		async move {
			let span = Span::current();
			let req_cx = RequestContext::default();
			let otel_cx = Arc::new(TelemetryContext::new().with_value(req_cx.clone()));
			// Parse the RPC request structure
			match rpc.format.req_ws(msg) {
				Ok(req) => {
					// Now that we know the method, we can update the span and create otel context
					span.record("rpc.method", req.method.to_str());
					span.record("otel.name", format!("surrealdb.rpc/{}", req.method));
					span.record(
						"rpc.request_id",
						req.id.clone().map(|id| format!("{id:?}")).unwrap_or_default(),
					);
					let otel_cx = Arc::new(TelemetryContext::current_with_value(
						req_cx.with_method(req.method.to_str()).with_size(len),
					));
					// Process the message
					tokio::select! {
						//
						biased;
						// Check if we should teardown
						_ = canceller.cancelled() => (),
						// Wait for the message to be processed
						_ = async move {
							// Don't start processing if we are gracefully shutting down
							if shutdown.is_cancelled() {
								// Process the response
								crate::rpc::response::send(
									DbResponse::failure(req.id, req.session_id.map(Into::into), DbResultError::InternalError(SERVER_SHUTTING_DOWN.to_string())),
									otel_cx.clone(),
									rpc.format,
									chn
								)
									.with_context(otel_cx.as_ref().clone())
									.await;
							}
							// Check to see whether we have available memory
							else if ALLOC.is_beyond_threshold() {
								// Process the response
								crate::rpc::response::send(
									DbResponse::failure(req.id, req.session_id.map(Into::into), DbResultError::InternalError(SERVER_OVERLOADED.to_string())),
									otel_cx.clone(),
									rpc.format,
									chn
								)
									.with_context(otel_cx.as_ref().clone())
									.await;
							}
							// Otherwise process the request message
							else {
								// Process the message
								let result = Self::process_message(
									rpc.clone(),
									req.session_id.map(Into::into),
									req.txn.map(Into::into),
									req.method,
									req.params,
								)
									.await;

								crate::rpc::response::send(
									match result {
										Ok(result) => DbResponse::success(req.id, req.session_id.map(Into::into), result),
										Err(err) => DbResponse::failure(req.id, req.session_id.map(Into::into), err),
									},
									otel_cx.clone(),
									rpc.format,
									chn
								)
									.with_context(otel_cx.as_ref().clone())
									.await;
							}
						} => (),
					}
				}
				Err(err) => {
					// Process the response
					crate::rpc::response::send(
						DbResponse::failure(None, None, err),
						otel_cx.clone(),
						rpc.format,
						chn
					)
						.with_context(otel_cx.as_ref().clone())
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
		session_id: Option<Uuid>,
		txn: Option<Uuid>,
		method: Method,
		params: Array,
	) -> Result<DbResult, DbResultError> {
		debug!("Process RPC request");
		// Check that the method is a valid method
		if !method.is_valid() {
			return Err(DbResultError::MethodNotFound("Method not found".to_string()));
		}
		// Execute the specified method
		RpcProtocol::execute(rpc.as_ref(), txn, session_id, method, params)
			.await
			.map_err(Into::into)
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
		// Cancel the WebSocket tasks
		rpc.canceller.cancel();
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

	/// A pointer to all active sessions
	fn session_map(&self) -> &HashMap<Option<Uuid>, Arc<RwLock<Session>>> {
		&self.sessions
	}

	// ------------------------------
	// Transactions
	// ------------------------------

	/// Retrieves a transaction by ID
	async fn get_tx(
		&self,
		id: Uuid,
	) -> Result<Arc<surrealdb_core::kvs::Transaction>, surrealdb_core::rpc::RpcError> {
		trace!("WebSocket get_tx called for transaction {id}");
		self.transactions
			.get(&id)
			.map(|entry| {
				trace!("Transaction {id} found in WebSocket transactions map");
				entry.value().1.clone()
			})
			.ok_or_else(|| {
				warn!(
					"Transaction {id} not found in WebSocket transactions map (have {} transactions)",
					self.transactions.len()
				);
				surrealdb_core::rpc::RpcError::InvalidParams("Transaction not found".to_string())
			})
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are enabled on WebSockets
	const LQ_SUPPORT: bool = true;

	/// Handles the execution of a LIVE statement
	async fn handle_live(&self, lqid: &Uuid, session_id: Option<Uuid>) {
		self.state.live_queries.write().await.insert(*lqid, (self.id, session_id));
		trace!("Registered live query {lqid} on websocket {}", self.id);
	}

	/// Handles the execution of a KILL statement
	async fn handle_kill(&self, lqid: &Uuid) {
		if let Some((id, session_id)) = self.state.live_queries.write().await.remove(lqid) {
			if let Some(session_id) = session_id {
				trace!("Unregistered live query {lqid} on websocket {id} for session {session_id}");
			} else {
				trace!("Unregistered live query {lqid} on websocket {id} for default session");
			}
		}
	}

	/// Handles the cleanup of live queries
	async fn cleanup_lqs(&self, session_id: Option<&Uuid>) {
		let mut gc = Vec::new();
		// Find all live queries for to this connection
		self.state.live_queries.write().await.retain(|key, value| {
			if value.0 == self.id && value.1.as_ref() == session_id {
				trace!("Removing live query: {key}");
				gc.push(*key);
				return false;
			}
			true
		});
		// Garbage collect the live queries on this connection
		if let Err(err) = self.kvs().delete_queries(gc).await {
			error!("Error handling RPC connection: {err}");
		}
	}

	/// Handles the cleanup of live queries
	async fn cleanup_all_lqs(&self) {
		let mut gc = Vec::new();
		// Find all live queries for to this connection
		self.state.live_queries.write().await.retain(|key, value| {
			if value.0 == self.id {
				trace!("Removing live query: {key}");
				gc.push(*key);
				return false;
			}
			true
		});
		// Garbage collect the live queries on this connection
		if let Err(err) = self.kvs().delete_queries(gc).await {
			error!("Error handling RPC connection: {err}");
		}
	}

	/// Handles the cleanup of transactions for a specific session
	async fn cleanup_txns(&self, session_id: Option<&Uuid>) {
		// Collect transaction IDs that match the given session
		let txn_ids: Vec<Uuid> = self
			.transactions
			.iter()
			.filter(|entry| entry.value().0.as_ref() == session_id)
			.map(|entry| *entry.key())
			.collect();
		// Cancel and remove each matching transaction
		for txn_id in txn_ids {
			if let Some((_, (sid, tx))) = self.transactions.remove(&txn_id) {
				trace!("Cancelling transaction {txn_id} during session cleanup");
				if let Err(err) = tx.cancel().await {
					error!("Error cancelling transaction {txn_id}: {err}");
				}
				// Release the reserved slot
				if let Some(c) = self.counters.get(&sid) {
					c.fetch_sub(1, Ordering::Relaxed);
				}
			}
		}
	}

	/// Handles the cleanup of all transactions on this connection
	async fn cleanup_all_txns(&self) {
		// Collect all transaction IDs
		let txn_ids: Vec<Uuid> = self.transactions.iter().map(|entry| *entry.key()).collect();
		// Cancel and remove each transaction
		for txn_id in txn_ids {
			if let Some((_, (sid, tx))) = self.transactions.remove(&txn_id) {
				trace!("Cancelling transaction {txn_id} during connection cleanup");
				if let Err(err) = tx.cancel().await {
					error!("Error cancelling transaction {txn_id}: {err}");
				}
				// Release the reserved slot
				if let Some(c) = self.counters.get(&sid) {
					c.fetch_sub(1, Ordering::Relaxed);
				}
			}
		}
	}

	// ------------------------------
	// Methods for transactions
	// ------------------------------

	/// Begin a new transaction
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		session_id: Option<Uuid>,
	) -> Result<DbResult, surrealdb_core::rpc::RpcError> {
		// Determine the transaction limit for connections
		let limit = match session_id {
			Some(_) => *MAX_TRANSACTIONS_PER_SESSION,
			None => *MAX_TRANSACTIONS_PER_CONNECTION,
		};
		// Increase the transaction counter for the session
		let prev = self
			.counters
			.entry(session_id)
			.or_insert_with(|| AtomicUsize::new(0))
			.fetch_add(1, Ordering::Relaxed);
		// Check if the transaction limit has been reached
		if prev >= limit {
			// Over limit — undo the reservation
			if let Some(c) = self.counters.get(&session_id) {
				c.fetch_sub(1, Ordering::Relaxed);
			}
			return Err(surrealdb_core::rpc::RpcError::TooManyTransactions);
		}
		// Create a new transaction
		let tx = match self.kvs().transaction(TransactionType::Write, LockType::Optimistic).await {
			Ok(tx) => tx,
			Err(e) => {
				// Undo the reservation on failure
				if let Some(c) = self.counters.get(&session_id) {
					c.fetch_sub(1, Ordering::Relaxed);
				}
				return Err(e.into());
			}
		};
		// Generate a unique transaction ID
		let id = Uuid::now_v7();
		trace!("WebSocket begin: created transaction {id}");
		// Store the transaction in the map with its session association
		self.transactions.insert(id, (session_id, Arc::new(tx)));
		trace!(
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
		_session_id: Option<Uuid>,
		params: Array,
	) -> Result<DbResult, surrealdb_core::rpc::RpcError> {
		// Extract the transaction ID from params
		let mut params_vec = params.into_vec();
		let Some(Value::Uuid(txn_id)) = params_vec.pop() else {
			return Err(surrealdb_core::rpc::RpcError::InvalidParams(
				"Expected transaction UUID".to_string(),
			));
		};

		let txn_id = txn_id.into_inner();

		// Retrieve and remove the transaction from the map
		let Some((_, (sid, tx))) = self.transactions.remove(&txn_id) else {
			return Err(surrealdb_core::rpc::RpcError::InvalidParams(
				"Transaction not found".to_string(),
			));
		};

		// Release the reserved slot
		if let Some(c) = self.counters.get(&sid) {
			c.fetch_sub(1, Ordering::Relaxed);
		}

		// Commit the transaction
		tx.commit().await?;

		// Return success
		Ok(DbResult::Other(Value::None))
	}

	/// Cancel a transaction
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Option<Uuid>,
		params: Array,
	) -> Result<DbResult, surrealdb_core::rpc::RpcError> {
		// Extract the transaction ID from params
		let mut params_vec = params.into_vec();
		let Some(Value::Uuid(txn_id)) = params_vec.pop() else {
			return Err(surrealdb_core::rpc::RpcError::InvalidParams(
				"Expected transaction UUID".to_string(),
			));
		};

		let txn_id = txn_id.into_inner();

		// Retrieve and remove the transaction from the map
		let Some((_, (sid, tx))) = self.transactions.remove(&txn_id) else {
			return Err(surrealdb_core::rpc::RpcError::InvalidParams(
				"Transaction not found".to_string(),
			));
		};

		// Release the reserved slot
		if let Some(c) = self.counters.get(&sid) {
			c.fetch_sub(1, Ordering::Relaxed);
		}

		// Cancel the transaction
		tx.cancel().await?;

		// Return success
		Ok(DbResult::Other(Value::None))
	}
}
