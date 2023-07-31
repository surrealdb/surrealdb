use crate::cnf::MAX_CONCURRENT_CALLS;
use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::cnf::WEBSOCKET_PING_FREQUENCY;
use crate::dbs::DB;
use crate::err::Error;
use crate::rpc::args::Take;
use crate::rpc::paths::{ID, METHOD, PARAMS};
use crate::rpc::res;
use crate::rpc::res::Data;
use crate::rpc::res::Failure;
use crate::rpc::res::IntoRpcResponse;
use crate::rpc::res::OutputFormat;
use crate::rpc::CONN_CLOSED_ERR;
use crate::telemetry::traces::rpc::span_for_request;
use axum::routing::get;
use axum::Extension;
use axum::Router;
use futures::{SinkExt, StreamExt};
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use http_body::Body as HttpBody;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use surrealdb::channel;
use surrealdb::channel::{Receiver, Sender};
use surrealdb::dbs::{QueryType, Response, Session};
use surrealdb::sql::serde::deserialize;
use surrealdb::sql::Array;
use surrealdb::sql::Object;
use surrealdb::sql::Strand;
use surrealdb::sql::Value;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tower_http::request_id::RequestId;
use tracing::Span;
use uuid::Uuid;

use axum::{
	extract::ws::{Message, WebSocket, WebSocketUpgrade},
	response::IntoResponse,
};

// Mapping of WebSocketID to WebSocket
pub(crate) struct WebSocketRef(pub(crate) Sender<Message>, pub(crate) CancellationToken);
type WebSockets = RwLock<HashMap<Uuid, WebSocketRef>>;
// Mapping of LiveQueryID to WebSocketID
type LiveQueries = RwLock<HashMap<Uuid, Uuid>>;

pub(super) static WEBSOCKETS: Lazy<WebSockets> = Lazy::new(WebSockets::default);
static LIVE_QUERIES: Lazy<LiveQueries> = Lazy::new(LiveQueries::default);

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/rpc", get(handler))
}

async fn handler(
	ws: WebSocketUpgrade,
	Extension(sess): Extension<Session>,
	Extension(req_id): Extension<RequestId>,
) -> impl IntoResponse {
	// finalize the upgrade process by returning upgrade callback.
	// we can customize the callback by sending additional info such as address.
	ws.on_upgrade(move |socket| handle_socket(socket, sess, req_id))
}

async fn handle_socket(ws: WebSocket, sess: Session, req_id: RequestId) {
	let rpc = Rpc::new(sess);

	// If the request ID is a valid UUID and is not already in use, use it as the WebSocket ID
	match req_id.header_value().to_str().map(Uuid::parse_str) {
		Ok(Ok(req_id)) if !WEBSOCKETS.read().await.contains_key(&req_id) => {
			rpc.write().await.ws_id = req_id
		}
		_ => (),
	}

	Rpc::serve(rpc, ws).await;
}

pub struct Rpc {
	session: Session,
	format: OutputFormat,
	ws_id: Uuid,
	vars: BTreeMap<String, Value>,
	graceful_shutdown: CancellationToken,
}

impl Rpc {
	/// Instantiate a new RPC
	pub fn new(mut session: Session) -> Arc<RwLock<Rpc>> {
		// Create a new RPC variables store
		let vars = BTreeMap::new();
		// Set the default output format
		let format = OutputFormat::Json;
		// Enable real-time mode
		session.rt = true;
		// Create and store the Rpc connection
		Arc::new(RwLock::new(Rpc {
			session,
			format,
			ws_id: Uuid::new_v4(),
			vars,
			graceful_shutdown: CancellationToken::new(),
		}))
	}

	/// Serve the RPC endpoint
	pub async fn serve(rpc: Arc<RwLock<Rpc>>, ws: WebSocket) {
		// Split the socket into send and recv
		let (sender, receiver) = ws.split();
		// Create an internal channel between the receiver and the sender
		let (internal_sender, internal_receiver) = channel::new(MAX_CONCURRENT_CALLS);

		let ws_id = rpc.read().await.ws_id;

		// Store this WebSocket in the list of WebSockets
		WEBSOCKETS.write().await.insert(
			ws_id,
			WebSocketRef(internal_sender.clone(), rpc.read().await.graceful_shutdown.clone()),
		);

		trace!("WebSocket {} connected", ws_id);

		// Wait until all tasks finish
		tokio::join!(
			Self::ping(rpc.clone(), internal_sender.clone()),
			Self::read(rpc.clone(), receiver, internal_sender.clone()),
			Self::write(rpc.clone(), sender, internal_receiver.clone()),
			Self::lq_notifications(rpc.clone()),
		);

		// Remove all live queries
		LIVE_QUERIES.write().await.retain(|key, value| {
			if value == &ws_id {
				trace!("Removing live query: {}", key);
				return false;
			}
			true
		});

		// Remove this WebSocket from the list of WebSockets
		WEBSOCKETS.write().await.remove(&ws_id);

		trace!("WebSocket {} disconnected", ws_id);
	}

	/// Send Ping messages to the client
	async fn ping(rpc: Arc<RwLock<Rpc>>, internal_sender: Sender<Message>) {
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
		rpc: Arc<RwLock<Rpc>>,
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
									tasks.spawn(Rpc::handle_msg(rpc.clone(), msg, internal_sender.clone()));
								}
								Message::Binary(_) => {
									tasks.spawn(Rpc::handle_msg(rpc.clone(), msg, internal_sender.clone()));
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
		rpc: Arc<RwLock<Rpc>>,
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
	async fn lq_notifications(rpc: Arc<RwLock<Rpc>>) {
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
									let message = res::success(None, notification);
									// Get the current output format
									let format = rpc.read().await.format.clone();
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
	async fn handle_msg(rpc: Arc<RwLock<Rpc>>, msg: Message, chn: Sender<Message>) {
		// Get the current output format
		let mut out_fmt = rpc.read().await.format.clone();
		let span = span_for_request(&rpc.read().await.ws_id);
		let _enter = span.enter();
		// Parse the request
		match Self::parse_request(msg).await {
			Ok((id, method, params, _out_fmt)) => {
				span.record(
					"rpc.jsonrpc.request_id",
					id.clone().map(|v| v.as_string()).unwrap_or(String::new()),
				);
				if let Some(_out_fmt) = _out_fmt {
					out_fmt = _out_fmt;
				}

				// Process the request
				let res = Self::process_request(rpc.clone(), &method, params).await;

				// Process the response
				res.into_response(id).send(out_fmt, chn).await
			}
			Err(err) => {
				// Process the response
				res::failure(None, err).send(out_fmt, chn).await
			}
		}
	}

	async fn parse_request(
		msg: Message,
	) -> Result<(Option<Value>, String, Array, Option<OutputFormat>), Failure> {
		let mut out_fmt = None;
		let req = match msg {
			// This is a binary message
			Message::Binary(val) => {
				// Use binary output
				out_fmt = Some(OutputFormat::Full);

				match deserialize(&val) {
					Ok(v) => v,
					Err(_) => {
						debug!("Error when trying to deserialize the request");
						return Err(Failure::PARSE_ERROR);
					}
				}
			}
			// This is a text message
			Message::Text(ref val) => {
				// Parse the SurrealQL object
				match surrealdb::sql::value(val) {
					// The SurrealQL message parsed ok
					Ok(v) => v,
					// The SurrealQL message failed to parse
					_ => return Err(Failure::PARSE_ERROR),
				}
			}
			// Unsupported message type
			_ => {
				debug!("Unsupported message type: {:?}", msg);
				return Err(res::Failure::custom("Unsupported message type"));
			}
		};
		// Fetch the 'id' argument
		let id = match req.pick(&*ID) {
			v if v.is_none() => None,
			v if v.is_null() => Some(v),
			v if v.is_uuid() => Some(v),
			v if v.is_number() => Some(v),
			v if v.is_strand() => Some(v),
			v if v.is_datetime() => Some(v),
			_ => return Err(Failure::INVALID_REQUEST),
		};
		// Fetch the 'method' argument
		let method = match req.pick(&*METHOD) {
			Value::Strand(v) => v.to_raw(),
			_ => return Err(Failure::INVALID_REQUEST),
		};

		// Now that we know the method, we can update the span
		Span::current().record("rpc.method", &method);
		Span::current().record("otel.name", format!("surrealdb.rpc/{}", method));

		// Fetch the 'params' argument
		let params = match req.pick(&*PARAMS) {
			Value::Array(v) => v,
			_ => Array::new(),
		};

		Ok((id, method, params, out_fmt))
	}

	async fn process_request(
		rpc: Arc<RwLock<Rpc>>,
		method: &str,
		params: Array,
	) -> Result<Data, Failure> {
		info!("Process RPC request");

		// Match the method to a function
		match method {
			// Handle a surrealdb ping message
			//
			// This is used to keep the WebSocket connection alive in environments where the WebSocket protocol is not enough.
			// For example, some browsers will wait for the TCP protocol to timeout before triggering an on_close event. This may take several seconds or even minutes in certain scenarios.
			// By sending a ping message every few seconds from the client, we can force a connection check and trigger a an on_close event if the ping can't be sent.
			//
			"ping" => Ok(Value::None.into()),
			// Retrieve the current auth record
			"info" => match params.len() {
				0 => rpc.read().await.info().await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Switch to a specific namespace and database
			"use" => match params.needs_two() {
				Ok((ns, db)) => {
					rpc.write().await.yuse(ns, db).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Signup to a specific authentication scope
			"signup" => match params.needs_one() {
				Ok(Value::Object(v)) => {
					rpc.write().await.signup(v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Signin as a root, namespace, database or scope user
			"signin" => match params.needs_one() {
				Ok(Value::Object(v)) => {
					rpc.write().await.signin(v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Invalidate the current authentication session
			"invalidate" => match params.len() {
				0 => rpc.write().await.invalidate().await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Authenticate using an authentication token
			"authenticate" => match params.needs_one() {
				Ok(Value::Strand(v)) => {
					rpc.write().await.authenticate(v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Kill a live query using a query id
			"kill" => match params.needs_one() {
				Ok(v) if v.is_uuid() => {
					rpc.read().await.kill(v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Setup a live query on a specific table
			"live" => match params.needs_one_or_two() {
				Ok((v, d)) if v.is_table() => {
					rpc.read().await.live(v, d).await.map(Into::into).map_err(Into::into)
				}
				Ok((v, d)) if v.is_strand() => {
					rpc.read().await.live(v, d).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Specify a connection-wide parameter
			"let" | "set" => match params.needs_one_or_two() {
				Ok((Value::Strand(s), v)) => {
					rpc.write().await.set(s, v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Unset and clear a connection-wide parameter
			"unset" => match params.needs_one() {
				Ok(Value::Strand(s)) => {
					rpc.write().await.unset(s).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Select a value or values from the database
			"select" => match params.needs_one() {
				Ok(v) => rpc.read().await.select(v).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Insert a value or values in the database
			"insert" => match params.needs_one_or_two() {
				Ok((v, o)) => {
					rpc.read().await.insert(v, o).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Create a value or values in the database
			"create" => match params.needs_one_or_two() {
				Ok((v, o)) => {
					rpc.read().await.create(v, o).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Update a value or values in the database using `CONTENT`
			"update" => match params.needs_one_or_two() {
				Ok((v, o)) => {
					rpc.read().await.update(v, o).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Update a value or values in the database using `MERGE`
			"change" | "merge" => match params.needs_one_or_two() {
				Ok((v, o)) => {
					rpc.read().await.change(v, o).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Update a value or values in the database using `PATCH`
			"modify" | "patch" => match params.needs_one_or_two() {
				Ok((v, o)) => {
					rpc.read().await.modify(v, o).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Delete a value or values from the database
			"delete" => match params.needs_one() {
				Ok(v) => rpc.read().await.delete(v).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Specify the output format for text requests
			"format" => match params.needs_one() {
				Ok(Value::Strand(v)) => {
					rpc.write().await.format(v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Get the current server version
			"version" => match params.len() {
				0 => Ok(format!("{PKG_NAME}-{}", *PKG_VERSION).into()),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Run a full SurrealQL query against the database
			"query" => match params.needs_one_or_two() {
				Ok((Value::Strand(s), o)) if o.is_none_or_null() => {
					rpc.read().await.query(s).await.map(Into::into).map_err(Into::into)
				}
				Ok((Value::Strand(s), Value::Object(o))) => {
					rpc.read().await.query_with(s, o).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			_ => Err(Failure::METHOD_NOT_FOUND),
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn format(&mut self, out: Strand) -> Result<Value, Error> {
		match out.as_str() {
			"json" | "application/json" => self.format = OutputFormat::Json,
			"cbor" | "application/cbor" => self.format = OutputFormat::Cbor,
			"pack" | "application/pack" => self.format = OutputFormat::Pack,
			_ => return Err(Error::InvalidType),
		};
		Ok(Value::None)
	}

	async fn yuse(&mut self, ns: Value, db: Value) -> Result<Value, Error> {
		if let Value::Strand(ns) = ns {
			self.session.ns = Some(ns.0);
		}
		if let Value::Strand(db) = db {
			self.session.db = Some(db.0);
		}
		Ok(Value::None)
	}

	async fn signup(&mut self, vars: Object) -> Result<Value, Error> {
		let kvs = DB.get().unwrap();
		surrealdb::iam::signup::signup(kvs, &mut self.session, vars)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}

	async fn signin(&mut self, vars: Object) -> Result<Value, Error> {
		let kvs = DB.get().unwrap();
		surrealdb::iam::signin::signin(kvs, &mut self.session, vars)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}
	async fn invalidate(&mut self) -> Result<Value, Error> {
		surrealdb::iam::clear::clear(&mut self.session)?;
		Ok(Value::None)
	}

	async fn authenticate(&mut self, token: Strand) -> Result<Value, Error> {
		let kvs = DB.get().unwrap();
		surrealdb::iam::verify::token(kvs, &mut self.session, &token.0).await?;
		Ok(Value::None)
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "SELECT * FROM $auth";
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, None).await?;
		// Extract the first value from the result
		let res = res.remove(0).result?.first();
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(&mut self, key: Strand, val: Value) -> Result<Value, Error> {
		match val {
			// Remove the variable if undefined
			Value::None => self.vars.remove(&key.0),
			// Store the variable if defined
			v => self.vars.insert(key.0, v),
		};
		Ok(Value::Null)
	}

	async fn unset(&mut self, key: Strand) -> Result<Value, Error> {
		self.vars.remove(&key.0);
		Ok(Value::Null)
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(&self, id: Value) -> Result<Value, Error> {
		// Specify the SQL query string
		let sql = "KILL $id";
		// Specify the query parameters
		let var = map! {
			String::from("id") => id,
			=> &self.vars
		};
		// Execute the query on the database
		let mut res = self.query_with(Strand::from(sql), Object::from(var)).await?;
		// Extract the first query result
		let response = res.remove(0);
		match response.result {
			Ok(v) => Ok(v),
			Err(e) => Err(Error::from(e)),
		}
	}

	async fn live(&self, tb: Value, diff: Value) -> Result<Value, Error> {
		// Specify the SQL query string
		let sql = match diff.is_true() {
			true => "LIVE SELECT DIFF FROM $tb",
			false => "LIVE SELECT * FROM $tb",
		};
		// Specify the query parameters
		let var = map! {
			String::from("tb") => tb.could_be_table(),
			=> &self.vars
		};
		// Execute the query on the database
		let mut res = self.query_with(Strand::from(sql), Object::from(var)).await?;
		// Extract the first query result
		let response = res.remove(0);
		match response.result {
			Ok(v) => Ok(v),
			Err(e) => Err(Error::from(e)),
		}
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, what: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "SELECT * FROM $what";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "INSERT INTO $what $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "CREATE $what CONTENT $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what CONTENT $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for changing
	// ------------------------------

	async fn change(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what MERGE $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for modifying
	// ------------------------------

	async fn modify(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what PATCH $data RETURN DIFF";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(&self, what: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "DELETE $what RETURN BEFORE";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(&self, sql: Strand) -> Result<Vec<Response>, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the query parameters
		let var = Some(self.vars.clone());
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var).await?;
		// Post-process hooks for web layer
		for response in &res {
			self.handle_live_query_results(response).await;
		}
		// Return the result to the client
		Ok(res)
	}

	async fn query_with(&self, sql: Strand, mut vars: Object) -> Result<Vec<Response>, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the query parameters
		let var = Some(mrg! { vars.0, &self.vars });
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var).await?;
		// Post-process hooks for web layer
		for response in &res {
			self.handle_live_query_results(response).await;
		}
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Private methods
	// ------------------------------

	async fn handle_live_query_results(&self, res: &Response) {
		match &res.query_type {
			QueryType::Live => {
				if let Ok(Value::Uuid(lqid)) = &res.result {
					// Match on Uuid type
					LIVE_QUERIES.write().await.insert(lqid.0, self.ws_id);
					trace!("Registered live query {} on websocket {}", lqid, self.ws_id);
				}
			}
			QueryType::Kill => {
				if let Ok(Value::Uuid(lqid)) = &res.result {
					let ws_id = LIVE_QUERIES.write().await.remove(&lqid.0);
					if let Some(ws_id) = ws_id {
						trace!("Unregistered live query {} on websocket {}", lqid, ws_id);
					}
				}
			}
			_ => {}
		}
	}
}
