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
	pub(crate) id: Uuid,
	pub(crate) format: Format,
	pub(crate) session: Session,
	pub(crate) vars: BTreeMap<String, Value>,
	pub(crate) limiter: Arc<Semaphore>,
	pub(crate) canceller: CancellationToken,
	pub(crate) channels: (Sender<Message>, Receiver<Message>),
	pub(crate) state: Arc<RpcState>,
	pub(crate) datastore: Arc<Datastore>,
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
			format,
			session,
			vars: BTreeMap::new(),
			limiter: Arc::new(Semaphore::new(*WEBSOCKET_MAX_CONCURRENT_REQUESTS)),
			canceller: CancellationToken::new(),
			channels: channel::bounded(*WEBSOCKET_MAX_CONCURRENT_REQUESTS),
			state,
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
		// Get the Datastore
		let ds = rpc_lock.datastore.clone();
		// Log the succesful WebSocket connection
		trace!("WebSocket {} connected", id);
		// Split the socket into sending and receiving streams
		let (sender, receiver) = ws.split();
		// Create an internal channel for sending and receiving
		let internal_sender = rpc_lock.channels.0.clone();
		let internal_receiver = rpc_lock.channels.1.clone();
		// Drop the lock early so rpc is free to be written to.
		std::mem::drop(rpc_lock);

		if let Err(err) = telemetry::metrics::ws::on_connect() {
			error!("Error running metrics::ws::on_connect hook: {}", err);
		}

		// Add this WebSocket to the list
		state.web_sockets.write().await.insert(id, rpc.clone());

		// Spawn async tasks for the WebSocket
		let mut tasks = JoinSet::new();
		tasks.spawn(Self::ping(rpc.clone(), internal_sender.clone()));
		tasks.spawn(Self::read(rpc.clone(), receiver, internal_sender.clone()));
		tasks.spawn(Self::write(rpc.clone(), sender, internal_receiver.clone()));

		// Wait until all tasks finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				error!("Error handling RPC connection: {}", err);
			}
		}

		internal_sender.close();

		trace!("WebSocket {} disconnected", id);

		// Remove this WebSocket from the list
		state.web_sockets.write().await.remove(&id);

		// Remove all live queries
		let mut gc = Vec::new();
		state.live_queries.write().await.retain(|key, value| {
			if value == &id {
				trace!("Removing live query: {}", key);
				gc.push(*key);
				return false;
			}
			true
		});

		if let Err(err) = ds.delete_queries(gc).await {
			error!("Error handling RPC connection: {}", err);
		}

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
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
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
		mut internal_receiver: Receiver<Message>,
	) {
		// Clone the WebSocket cancellation token
		let canceller = rpc.read().await.canceller.clone();
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
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
		// Store spawned tasks so we can wait for them
		let mut tasks = JoinSet::new();
		// Clone the WebSocket cancellation token
		let canceller = rpc.read().await.canceller.clone();
		// Loop, and listen for messages to write
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
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
							tasks.spawn(Connection::handle_message(rpc.clone(), msg, internal_sender.clone()));
						}
						Message::Binary(_) => {
							tasks.spawn(Connection::handle_message(rpc.clone(), msg, internal_sender.clone()));
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
		// Abort all tasks
		tasks.shutdown().await;
	}

	/// Handle individual WebSocket messages
	async fn handle_message(rpc: Arc<RwLock<Connection>>, msg: Message, chn: Sender<Message>) {
		// Get the current output format
		let mut fmt = rpc.read().await.format;
		// Prepare Span and Otel context
		let span = span_for_request(&rpc.read().await.id);
		// Acquire concurrent request rate limiter
		let permit = rpc.read().await.limiter.clone().acquire_owned().await.unwrap();
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
					// Process the message
					let res =
						Connection::process_message(rpc.clone(), &req.method, req.params).await;
					// Process the response
					res.into_response(req.id)
						.send(otel_cx.clone(), fmt, &chn)
						.with_context(otel_cx.as_ref().clone())
						.await
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
		// Drop the rate limiter permit
		drop(permit);
	}

	pub async fn process_message(
		rpc: Arc<RwLock<Connection>>,
		method: &str,
		params: Array,
	) -> Result<Data, Failure> {
		debug!("Process RPC request");
		let method = Method::parse(method);
		if !method.is_valid() {
			return Err(Failure::METHOD_NOT_FOUND);
		}

		// if the write lock is a bottleneck then execute could be refactored into execute_mut and execute
		// rpc.write().await.execute(method, params).await.map_err(Into::into)
		match method.needs_mut() {
			true => rpc.write().await.execute(method, params).await.map_err(Into::into),
			false => rpc.read().await.execute_immut(method, params).await.map_err(Into::into),
		}
	}
}

impl RpcContext for Connection {
	fn kvs(&self) -> &Datastore {
		&self.datastore
	}

	fn session(&self) -> &Session {
		&self.session
	}

	fn session_mut(&mut self) -> &mut Session {
		&mut self.session
	}

	fn vars(&self) -> &BTreeMap<String, Value> {
		&self.vars
	}

	fn vars_mut(&mut self) -> &mut BTreeMap<String, Value> {
		&mut self.vars
	}

	fn version_data(&self) -> impl Into<Data> {
		format!("{PKG_NAME}-{}", *PKG_VERSION)
	}

	const LQ_SUPPORT: bool = true;

	async fn handle_live(&self, lqid: &Uuid) {
		self.state.live_queries.write().await.insert(*lqid, self.id);
		trace!("Registered live query {} on websocket {}", lqid, self.id);
	}

	async fn handle_kill(&self, lqid: &Uuid) {
		if let Some(id) = self.state.live_queries.write().await.remove(lqid) {
			trace!("Unregistered live query {} on websocket {}", lqid, id);
		}
	}

	const GQL_SUPPORT: bool = true;
	fn graphql_schema_cache(&self) -> &SchemaCache {
		&self.gql_schema
	}
}
