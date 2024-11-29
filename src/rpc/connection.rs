use crate::cnf::{
	PKG_NAME, PKG_VERSION, WEBSOCKET_MAX_CONCURRENT_REQUESTS, WEBSOCKET_PING_FREQUENCY,
};
use crate::rpc::failure::Failure;
use crate::rpc::format::WsFormat;
use crate::rpc::response::{failure, IntoRpcResponse};
use crate::rpc::CONN_CLOSED_ERR;
use crate::telemetry;
use crate::telemetry::metrics::ws::RequestContext;
use crate::telemetry::traces::rpc::span_for_request;
use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use opentelemetry::trace::FutureExt;
use opentelemetry::Context as TelemetryContext;
use std::collections::BTreeMap;
use std::sync::Arc;
use surrealdb::channel::{self, Receiver, Sender};
use surrealdb::dbs::Session;
#[cfg(surrealdb_unstable)]
use surrealdb::gql::{Pessimistic, SchemaCache};
use surrealdb::kvs::Datastore;
use surrealdb::rpc::format::Format;
use surrealdb::rpc::method::Method;
use surrealdb::rpc::Data;
use surrealdb::rpc::RpcContext;
use surrealdb::sql::Array;
use surrealdb::sql::Value;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::Span;
use uuid::Uuid;

use super::RpcState;

pub struct Connection {
	/// The unique id of this WebSocket connection
	pub(crate) id: Uuid,
	/// The request and response format for messages
	pub(crate) format: Format,
	/// The persistent session for this WebSocket connection
	pub(crate) session: Session,
	/// The system state for all RPC WebSocket connections
	pub(crate) state: Arc<RpcState>,
	/// The datastore accessible to all RPC WebSocket connections
	pub(crate) datastore: Arc<Datastore>,
	/// The persistent parameters for this WebSocket connection
	pub(crate) vars: BTreeMap<String, Value>,
	/// A cancellation token called when shutting down the server
	pub(crate) shutdown: CancellationToken,
	/// A cancellation token for cancelling all spawned tasks
	pub(crate) canceller: CancellationToken,
	/// A semaphore for limiting the number of concurrent calls
	pub(crate) semaphore: Arc<Semaphore>,
	/// The channels used to send and receive WebSocket messages
	pub(crate) channel: (Sender<Message>, Receiver<Message>),
	/// The GraphQL schema cache stored in advance
	#[cfg(surrealdb_unstable)]
	pub(crate) gql_schema: SchemaCache<Pessimistic>,
}

impl Connection {
	/// Instantiate a new RPC
	pub fn new(
		datastore: Arc<Datastore>,
		state: Arc<RpcState>,
		id: Uuid,
		mut session: Session,
		format: Format,
	) -> Arc<RwLock<Connection>> {
		// Enable real-time mode
		session.rt = true;
		// Create and store the RPC connection
		Arc::new(RwLock::new(Connection {
			id,
			state,
			format,
			session,
			vars: BTreeMap::new(),
			shutdown: CancellationToken::new(),
			canceller: CancellationToken::new(),
			semaphore: Arc::new(Semaphore::new(*WEBSOCKET_MAX_CONCURRENT_REQUESTS)),
			channel: channel::bounded(*WEBSOCKET_MAX_CONCURRENT_REQUESTS),
			#[cfg(surrealdb_unstable)]
			gql_schema: SchemaCache::new(datastore.clone()),
			datastore,
		}))
	}

	/// Serve the RPC endpoint
	pub async fn serve(rpc: Arc<RwLock<Connection>>, ws: WebSocket) {
		// Get the RPC lock
		let rpc_lock = rpc.read().await;
		// Get the WebSocket id
		let id = rpc_lock.id;
		// Get the WebSocket state
		let state = rpc_lock.state.clone();
		// Log the succesful WebSocket connection
		trace!("WebSocket {} connected", id);
		// Split the socket into sending and receiving streams
		let (sender, receiver) = ws.split();
		// Create an internal channel for sending and receiving
		let internal_sender = rpc_lock.channel.0.clone();
		let internal_receiver = rpc_lock.channel.1.clone();
		// Drop the lock early so rpc is free to be written to.
		std::mem::drop(rpc_lock);
		// Add this WebSocket to the list
		state.web_sockets.write().await.insert(id, rpc.clone());
		// Start telemetry metrics for this connection
		if let Err(err) = telemetry::metrics::ws::on_connect() {
			error!("Error running metrics::ws::on_connect hook: {}", err);
		}
		// Spawn async tasks for the WebSocket
		let mut tasks = JoinSet::new();
		tasks.spawn(Self::ping(rpc.clone(), internal_sender.clone()));
		tasks.spawn(Self::read(rpc.clone(), receiver, internal_sender.clone()));
		tasks.spawn(Self::write(rpc.clone(), sender, internal_receiver.clone()));
		// Wait for all tasks to finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				error!("Error handling RPC connection: {}", err);
			}
		}
		// Close the internal response channel
		internal_sender.close();
		// Log the WebSocket disconnection
		trace!("WebSocket {} disconnected", id);
		// Cleanup the live queries for this WebSocket
		rpc.read().await.cleanup_lqs().await;
		// Remove this WebSocket from the list
		state.web_sockets.write().await.remove(&id);
		// Stop telemetry metrics for this connection
		if let Err(err) = telemetry::metrics::ws::on_disconnect() {
			error!("Error running metrics::ws::on_disconnect hook: {}", err);
		}
	}

	/// Send Ping messages to the client
	async fn ping(rpc: Arc<RwLock<Connection>>, internal_sender: Sender<Message>) {
		// Create the interval ticker
		let mut interval = tokio::time::interval(WEBSOCKET_PING_FREQUENCY);
		// Clone the WebSocket cancellation token
		let canceller = rpc.read().await.canceller.clone();
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has cancelled
				_ = canceller.cancelled() => break,
				// Yield back to other tasks
				_ = tokio::task::yield_now() => (),
				// Send a regular ping message
				_ = interval.tick() => {
					// Create a new ping message
					let msg = Message::Ping(vec![]);
					// Close the connection if the message fails
					if internal_sender.send(msg).await.is_err() {
						// Cancel the WebSocket tasks
						rpc.read().await.canceller.cancel();
						// Exit out of the loop
						break;
					}
				},
			}
		}
	}

	/// Write messages to the client
	async fn write(
		rpc: Arc<RwLock<Connection>>,
		mut sender: SplitSink<WebSocket, Message>,
		internal_receiver: Receiver<Message>,
	) {
		// Pin the internal receiving channel
		let mut internal_receiver = Box::pin(internal_receiver);
		// Clone the WebSocket cancellation token
		let canceller = rpc.read().await.canceller.clone();
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has cancelled
				_ = canceller.cancelled() => break,
				// Yield back to other tasks
				_ = tokio::task::yield_now() => (),
				// Wait for the next message to send
				Some(res) = internal_receiver.next() => {
					// Send the message to the client
					if let Err(err) = sender.send(res).await {
						// Output any errors if not a close error
						if err.to_string() != CONN_CLOSED_ERR {
							debug!("WebSocket error: {:?}", err);
						}
						// Cancel the WebSocket tasks
						rpc.read().await.canceller.cancel();
						// Exit out of the loop
						break;
					}
				},
			}
		}
	}

	/// Read messages sent from the client
	async fn read(
		rpc: Arc<RwLock<Connection>>,
		mut receiver: SplitStream<WebSocket>,
		internal_sender: Sender<Message>,
	) {
		// Get all required values
		let (shutdown, canceller) = {
			// Read the connection state
			let rpc = rpc.read().await;
			// Clone the WebSocket shutdown token
			let shutdown = rpc.shutdown.clone();
			// Clone the WebSocket cancellation token
			let canceller = rpc.canceller.clone();
			// Return the required values
			(shutdown, canceller)
		};
		// Store spawned tasks so we can wait for them
		let mut tasks = JoinSet::new();
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has shutdown
				_ = shutdown.cancelled(), if tasks.is_empty() => break,
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Yield back to other tasks
				_ = tokio::task::yield_now() => (),
				// Remove any completed tasks
				Some(out) = tasks.join_next() => match out {
					// The task completed successfully
					Ok(_) => continue,
					// There was an uncaught panic in the task
					Err(err) => {
						// There was an error with the task
						trace!("WebSocket request error: {:?}", err);
						// Cancel the WebSocket tasks
						rpc.read().await.canceller.cancel();
						// Exit out of the loop
						break;
					}
				},
				// Wait for the next received message
				Some(msg) = receiver.next() => match msg {
					// We've received a message from the client
					Ok(msg) => match msg {
						Message::Text(_) => {
							tasks.spawn(Self::handle_message(rpc.clone(), msg, internal_sender.clone()));
						}
						Message::Binary(_) => {
							tasks.spawn(Self::handle_message(rpc.clone(), msg, internal_sender.clone()));
						}
						Message::Close(_) => {
							// Respond with a close message
							if let Err(err) = internal_sender.send(Message::Close(None)).await {
								trace!("WebSocket error when replying to the Close frame: {:?}", err);
							};
							// Cancel the WebSocket tasks
							rpc.read().await.canceller.cancel();
							// Exit out of the loop
							break;
						}
						_ => {
							// Ignore everything else
						}
					},
					Err(err) => {
						// There was an error with the WebSocket
						trace!("WebSocket error: {:?}", err);
						// Cancel the WebSocket tasks
						rpc.read().await.canceller.cancel();
						// Exit out of the loop
						break;
					}
				}
			}
		}
		// Wait for all tasks to finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				// There was an error with the task
				trace!("WebSocket request error: {:?}", err);
			}
		}
		// Cancel the WebSocket tasks
		rpc.read().await.canceller.cancel();
		// Abort all tasks
		tasks.shutdown().await;
	}

	/// Handle individual WebSocket messages
	async fn handle_message(rpc: Arc<RwLock<Connection>>, msg: Message, chn: Sender<Message>) {
		// Get all required values
		let (id, mut fmt, shutdown, canceller, semaphore) = {
			// Read the connection state
			let rpc = rpc.read().await;
			// Fetch the connection id
			let id = rpc.id;
			// Fetch the connection output format
			let format = rpc.format;
			// Clone the WebSocket cancellation token
			let shutdown = rpc.shutdown.clone();
			// Clone the WebSocket cancellation token
			let canceller = rpc.canceller.clone();
			// Clone the request limiter
			let semaphore = rpc.semaphore.clone();
			// Return the required values
			(id, format, shutdown, canceller, semaphore)
		};
		// Calculate the length of the message
		let len = match msg {
			Message::Text(ref msg) => {
				// If no format was specified, default to JSON
				if fmt.is_none() {
					fmt = Format::Json;
					rpc.write().await.format = fmt;
				}
				// Retrieve the length of the message
				msg.len()
			}
			Message::Binary(ref msg) => {
				// If no format was specified, default to Bincode
				if fmt.is_none() {
					fmt = Format::Bincode;
					rpc.write().await.format = fmt;
				}
				// Retrieve the length of the message
				msg.len()
			}
			_ => unreachable!(),
		};
		// Prepare span and otel context
		let span = span_for_request(&id);
		// Parse the request
		async move {
			let span = Span::current();
			let req_cx = RequestContext::default();
			let otel_cx = Arc::new(TelemetryContext::new().with_value(req_cx.clone()));
			// Parse the RPC request structure
			match fmt.req_ws(msg) {
				Ok(req) => {
					// Now that we know the method, we can update the span and create otel context
					span.record("rpc.method", &req.method);
					span.record("otel.name", format!("surrealdb.rpc/{}", req.method));
					span.record(
						"rpc.request_id",
						req.id.clone().map(Value::as_string).unwrap_or_default(),
					);
					let otel_cx = Arc::new(TelemetryContext::current_with_value(
						req_cx.with_method(&req.method).with_size(len),
					));
					// Parse the request RPC method type
					let method = Method::parse(&req.method);
					// Process the message
					tokio::select! {
						//
						biased;
						// Check if this has shutdown
						_ = canceller.cancelled() => (),
						// Wait for the message to be processed
						_ = async move {
							// Ping messages should be responded to immediately
							if method == Method::Ping {
								// Process ping messages immediately
								let res = Self::process_message(rpc.clone(), method, req.params).await;
								// Process the response
								res.into_response(req.id)
									.send(otel_cx.clone(), fmt, &chn)
									.with_context(otel_cx.as_ref().clone())
									.await;
							}
							// All other message types should be throttled
							else {
								// Don't start processing if we are gracefully shutting down
								if shutdown.is_cancelled() {
									// Process the response
									failure(req.id, Failure::custom("Server is shutting down"))
										.send(otel_cx.clone(), fmt, &chn)
										.with_context(otel_cx.as_ref().clone())
										.await;
								}
								// Otherwise process the request message
								else {
									// Acquire concurrent request rate limiter
									let permit = semaphore.acquire_owned().await.unwrap();
									// Process the message when the semaphore is acquired
									let res = Self::process_message(rpc.clone(), method, req.params).await;
									// Process the response
									res.into_response(req.id)
										.send(otel_cx.clone(), fmt, &chn)
										.with_context(otel_cx.as_ref().clone())
										.await;
									// Drop the rate limiter permit
									drop(permit);
								}
							}
						} => (),
					}
				}
				Err(err) => {
					// Process the response
					failure(None, err)
						.send(otel_cx.clone(), fmt, &chn)
						.with_context(otel_cx.as_ref().clone())
						.await
				}
			}
		}
		.instrument(span)
		.await;
	}

	pub async fn process_message(
		rpc: Arc<RwLock<Connection>>,
		method: Method,
		params: Array,
	) -> Result<Data, Failure> {
		debug!("Process RPC request");
		// Check that the method is a valid method
		if !method.is_valid() {
			return Err(Failure::METHOD_NOT_FOUND);
		}
		// Execute the specified method
		match method.needs_mutability() {
			true => rpc.write().await.execute_mutable(method, params).await.map_err(Into::into),
			false => rpc.read().await.execute_immutable(method, params).await.map_err(Into::into),
		}
	}
}

impl RpcContext for Connection {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.datastore
	}
	/// The current session for this RPC context
	fn session(&self) -> &Session {
		&self.session
	}
	/// Mutable access to the current session for this RPC context
	fn session_mut(&mut self) -> &mut Session {
		&mut self.session
	}
	/// The current parameters stored on this RPC context
	fn vars(&self) -> &BTreeMap<String, Value> {
		&self.vars
	}
	/// Mutable access to the current parameters stored on this RPC context
	fn vars_mut(&mut self) -> &mut BTreeMap<String, Value> {
		&mut self.vars
	}
	/// The version information for this RPC context
	fn version_data(&self) -> Data {
		format!("{PKG_NAME}-{}", *PKG_VERSION).into()
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are enabled on WebSockets
	const LQ_SUPPORT: bool = true;

	/// Handles the execution of a LIVE statement
	async fn handle_live(&self, lqid: &Uuid) {
		self.state.live_queries.write().await.insert(*lqid, self.id);
		trace!("Registered live query {} on websocket {}", lqid, self.id);
	}

	/// Handles the execution of a KILL statement
	async fn handle_kill(&self, lqid: &Uuid) {
		if let Some(id) = self.state.live_queries.write().await.remove(lqid) {
			trace!("Unregistered live query {} on websocket {}", lqid, id);
		}
	}

	/// Handles the cleanup of live queries
	async fn cleanup_lqs(&self) {
		let mut gc = Vec::new();
		// Find all live queries for to this connection
		self.state.live_queries.write().await.retain(|key, value| {
			if value == &self.id {
				trace!("Removing live query: {}", key);
				gc.push(*key);
				return false;
			}
			true
		});
		// Garbage collect the live queries on this connection
		if let Err(err) = self.kvs().delete_queries(gc).await {
			error!("Error handling RPC connection: {}", err);
		}
	}

	// ------------------------------
	// GraphQL
	// ------------------------------

	/// GraphQL queries are enabled on WebSockets
	#[cfg(surrealdb_unstable)]
	const GQL_SUPPORT: bool = true;

	#[cfg(surrealdb_unstable)]
	fn graphql_schema_cache(&self) -> &SchemaCache {
		&self.gql_schema
	}
}
