use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use opentelemetry::trace::FutureExt;
use opentelemetry::Context as TelemetryContext;
use std::collections::BTreeMap;
use std::sync::Arc;
use surrealdb::channel::{self, Receiver, Sender};
use tokio::sync::RwLock;
use tracing::Span;
use tracing_futures::Instrument;

use surrealdb::dbs::Session;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cnf::{MAX_CONCURRENT_CALLS, WEBSOCKET_PING_FREQUENCY};
use crate::dbs::DB;
use crate::rpc::res::success;
use crate::rpc::{WebSocketRef, CONN_CLOSED_ERR, LIVE_QUERIES, WEBSOCKETS};
use crate::telemetry;
use crate::telemetry::metrics::ws::RequestContext;
use crate::telemetry::traces::rpc::span_for_request;

use super::processor::Processor;
use super::request::parse_request;
use super::res::{failure, IntoRpcResponse, OutputFormat};

pub struct Connection {
	ws_id: Uuid,
	processor: Processor,
	graceful_shutdown: CancellationToken,
}

impl Connection {
	/// Instantiate a new RPC
	pub fn new(mut session: Session) -> Arc<RwLock<Connection>> {
		// Create a new RPC variables store
		let vars = BTreeMap::new();
		// Set the default output format
		let format = OutputFormat::Json;
		// Enable real-time mode
		session.rt = true;

		// Create a new RPC processor
		let processor = Processor::new(session, format, vars);

		// Create and store the RPC connection
		Arc::new(RwLock::new(Connection {
			ws_id: processor.ws_id,
			processor,
			graceful_shutdown: CancellationToken::new(),
		}))
	}

	/// Update the WebSocket ID. If the ID already exists, do not update it.
	pub async fn update_ws_id(&mut self, ws_id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
		if WEBSOCKETS.read().await.contains_key(&ws_id) {
			trace!("WebSocket ID '{}' is in use by another connection. Do not update it.", &ws_id);
			return Err("websocket ID is in use".into());
		}

		self.ws_id = ws_id;
		self.processor.ws_id = ws_id;
		Ok(())
	}

	/// Serve the RPC endpoint
	pub async fn serve(rpc: Arc<RwLock<Connection>>, ws: WebSocket) {
		// Split the socket into send and recv
		let (sender, receiver) = ws.split();
		// Create an internal channel between the receiver and the sender
		let (internal_sender, internal_receiver) = channel::new(MAX_CONCURRENT_CALLS);

		let ws_id = rpc.read().await.ws_id;

		trace!("WebSocket {} connected", ws_id);

		if let Err(err) = telemetry::metrics::ws::on_connect() {
			error!("Error running metrics::ws::on_connect hook: {}", err);
		}

		// Add this WebSocket to the list
		WEBSOCKETS.write().await.insert(
			ws_id,
			WebSocketRef(internal_sender.clone(), rpc.read().await.graceful_shutdown.clone()),
		);

		// Spawn async tasks for the WebSocket
		let mut tasks = JoinSet::new();
		tasks.spawn(Self::ping(rpc.clone(), internal_sender.clone()));
		tasks.spawn(Self::read(rpc.clone(), receiver, internal_sender.clone()));
		tasks.spawn(Self::write(rpc.clone(), sender, internal_receiver.clone()));
		tasks.spawn(Self::notifications(rpc.clone()));

		// Wait until all tasks finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				error!("Error handling RPC connection: {}", err);
			}
		}

		trace!("WebSocket {} disconnected", ws_id);

		// Remove this WebSocket from the list
		WEBSOCKETS.write().await.remove(&ws_id);

		// Remove all live queries
		let mut gc = Vec::new();
		LIVE_QUERIES.write().await.retain(|key, value| {
			if value == &ws_id {
				trace!("Removing live query: {}", key);
				gc.push(*key);
				return false;
			}
			true
		});

		// Garbage collect queries
		if let Err(e) = DB.get().unwrap().garbage_collect_dead_session(gc.as_slice()).await {
			error!("Failed to garbage collect dead sessions: {:?}", e);
		}

		if let Err(err) = telemetry::metrics::ws::on_disconnect() {
			error!("Error running metrics::ws::on_disconnect hook: {}", err);
		}
	}

	/// Send Ping messages to the client
	async fn ping(rpc: Arc<RwLock<Connection>>, internal_sender: Sender<Message>) {
		// Create the interval ticker
		let mut interval = tokio::time::interval(WEBSOCKET_PING_FREQUENCY);
		let cancel_token = rpc.read().await.graceful_shutdown.clone();
		loop {
			let is_shutdown = cancel_token.cancelled();
			tokio::select! {
				_ = interval.tick() => {
					let msg = Message::Ping(vec![]);

					// Send the message to the client and close the WebSocket connection if it fails
					if internal_sender.send(msg).await.is_err() {
						rpc.read().await.graceful_shutdown.cancel();
						break;
					}
				},
				_ = is_shutdown => break,
			}
		}
	}

	/// Read messages sent from the client
	async fn read(
		rpc: Arc<RwLock<Connection>>,
		mut receiver: SplitStream<WebSocket>,
		internal_sender: Sender<Message>,
	) {
		// Collect all spawned tasks so we can wait for them at the end
		let mut tasks = JoinSet::new();
		let cancel_token = rpc.read().await.graceful_shutdown.clone();
		loop {
			let is_shutdown = cancel_token.cancelled();
			tokio::select! {
				msg = receiver.next() => {
					if let Some(msg) = msg {
						match msg {
							// We've received a message from the client
							// Ping/Pong is automatically handled by the WebSocket library
							Ok(msg) => match msg {
								Message::Text(_) => {
									tasks.spawn(Connection::handle_msg(rpc.clone(), msg, internal_sender.clone()));
								}
								Message::Binary(_) => {
									tasks.spawn(Connection::handle_msg(rpc.clone(), msg, internal_sender.clone()));
								}
								Message::Close(_) => {
									// Respond with a close message
									if let Err(err) = internal_sender.send(Message::Close(None)).await {
										trace!("WebSocket error when replying to the Close frame: {:?}", err);
									};
									// Start the graceful shutdown of the WebSocket and close the channels
									rpc.read().await.graceful_shutdown.cancel();
									let _ = internal_sender.close();
									break;
								}
								_ => {
									// Ignore everything else
								}
							},
							Err(err) => {
								trace!("WebSocket error: {:?}", err);
								// Start the graceful shutdown of the WebSocket and close the channels
								rpc.read().await.graceful_shutdown.cancel();
								let _ = internal_sender.close();
								// Exit out of the loop
								break;
							}
						}
					}
				}
				_ = is_shutdown => break,
			}
		}

		// Wait for all tasks to finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				error!("Error while handling RPC message: {}", err);
			}
		}
	}

	/// Write messages to the client
	async fn write(
		rpc: Arc<RwLock<Connection>>,
		mut sender: SplitSink<WebSocket, Message>,
		mut internal_receiver: Receiver<Message>,
	) {
		let cancel_token = rpc.read().await.graceful_shutdown.clone();
		loop {
			let is_shutdown = cancel_token.cancelled();
			tokio::select! {
				// Wait for the next message to send
				msg = internal_receiver.next() => {
					if let Some(res) = msg {
						// Send the message to the client
						if let Err(err) = sender.send(res).await {
							if err.to_string() != CONN_CLOSED_ERR {
								debug!("WebSocket error: {:?}", err);
							}
							// Close the WebSocket connection
							rpc.read().await.graceful_shutdown.cancel();
							// Exit out of the loop
							break;
						}
					}
				},
				_ = is_shutdown => break,
			}
		}
	}

	/// Send live query notifications to the client
	async fn notifications(rpc: Arc<RwLock<Connection>>) {
		if let Some(channel) = DB.get().unwrap().notifications() {
			let cancel_token = rpc.read().await.graceful_shutdown.clone();
			loop {
				tokio::select! {
					msg = channel.recv() => {
						if let Ok(notification) = msg {
							// Find which WebSocket the notification belongs to
							if let Some(ws_id) = LIVE_QUERIES.read().await.get(&notification.id) {
								// Check to see if the WebSocket exists
								if let Some(WebSocketRef(ws, _)) = WEBSOCKETS.read().await.get(ws_id) {
									// Serialize the message to send
									let message = success(None, notification);
									// Get the current output format
									let format = rpc.read().await.processor.format.clone();
									// Send the notification to the client
									message.send(format, ws.clone()).await
								}
							}
						}
					},
					_ = cancel_token.cancelled() => break,
				}
			}
		}
	}

	/// Handle individual WebSocket messages
	async fn handle_msg(rpc: Arc<RwLock<Connection>>, msg: Message, chn: Sender<Message>) {
		// Get the current output format
		let mut out_fmt = rpc.read().await.processor.format.clone();
		// Prepare Span and Otel context
		let span = span_for_request(&rpc.read().await.ws_id);

		// Parse the request
		async move {
			let span = Span::current();
			let req_cx = RequestContext::default();
			let otel_cx = TelemetryContext::new().with_value(req_cx.clone());

			match parse_request(msg).await {
				Ok(req) => {
					if let Some(_out_fmt) = req.out_fmt {
						out_fmt = _out_fmt;
					}

					// Now that we know the method, we can update the span and create otel context
					span.record("rpc.method", &req.method);
					span.record("otel.name", format!("surrealdb.rpc/{}", req.method));
					span.record(
						"rpc.jsonrpc.request_id",
						req.id.clone().map(|v| v.as_string()).unwrap_or(String::new()),
					);
					let otel_cx = TelemetryContext::current_with_value(
						req_cx.with_method(&req.method).with_size(req.size),
					);

					// Process the request
					let res =
						rpc.write().await.processor.process_request(&req.method, req.params).await;

					// Process the response
					res.into_response(req.id).send(out_fmt, chn).with_context(otel_cx).await
				}
				Err(err) => {
					// Process the response
					failure(None, err).send(out_fmt, chn).with_context(otel_cx.clone()).await
				}
			}
		}
		.instrument(span)
		.await;
	}
}
