use super::request::parse_request;
use super::response::{failure, success, Data, Failure, IntoRpcResponse, OutputFormat};
use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::cnf::{WEBSOCKET_MAX_CONCURRENT_REQUESTS, WEBSOCKET_PING_FREQUENCY};
use crate::dbs::DB;
use crate::err::Error;
use crate::rpc::args::Take;
use crate::rpc::{WebSocketRef, CONN_CLOSED_ERR, LIVE_QUERIES, WEBSOCKETS};
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
use surrealdb::dbs::QueryType;
use surrealdb::dbs::Response;
use surrealdb::dbs::Session;
use surrealdb::sql::Array;
use surrealdb::sql::Object;
use surrealdb::sql::Strand;
use surrealdb::sql::Value;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::Span;
use uuid::Uuid;

pub struct Connection {
	ws_id: Uuid,
	session: Session,
	format: OutputFormat,
	vars: BTreeMap<String, Value>,
	limiter: Arc<Semaphore>,
	canceller: CancellationToken,
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
		// Create and store the RPC connection
		Arc::new(RwLock::new(Connection {
			ws_id: Uuid::new_v4(),
			session,
			format,
			vars,
			limiter: Arc::new(Semaphore::new(*WEBSOCKET_MAX_CONCURRENT_REQUESTS)),
			canceller: CancellationToken::new(),
		}))
	}

	/// Update the WebSocket ID. If the ID already exists, do not update it.
	pub async fn update_ws_id(&mut self, ws_id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
		if WEBSOCKETS.read().await.contains_key(&ws_id) {
			trace!("WebSocket ID '{}' is in use by another connection. Do not update it.", &ws_id);
			return Err("websocket ID is in use".into());
		}
		self.ws_id = ws_id;
		Ok(())
	}

	/// Serve the RPC endpoint
	pub async fn serve(rpc: Arc<RwLock<Connection>>, ws: WebSocket) {
		// Split the socket into send and recv
		let (sender, receiver) = ws.split();
		// Create an internal channel between the receiver and the sender
		let (internal_sender, internal_receiver) =
			channel::bounded(*WEBSOCKET_MAX_CONCURRENT_REQUESTS);

		let ws_id = rpc.read().await.ws_id;

		trace!("WebSocket {} connected", ws_id);

		if let Err(err) = telemetry::metrics::ws::on_connect() {
			error!("Error running metrics::ws::on_connect hook: {}", err);
		}

		// Add this WebSocket to the list
		WEBSOCKETS.write().await.insert(
			ws_id,
			WebSocketRef(internal_sender.clone(), rpc.read().await.canceller.clone()),
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

		internal_sender.close();

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
				msg = internal_receiver.next() => {
					if let Some(res) = msg {
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
				// Wait for the next message to read
				msg = receiver.next() => {
					if let Some(msg) = msg {
						// Process the received WebSocket message
						match msg {
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
			}
		}
		// Wait for all tasks to finish
		while let Some(res) = tasks.join_next().await {
			if let Err(err) = res {
				error!("Error while handling RPC message: {}", err);
			}
		}
	}

	/// Send live query notifications to the client
	async fn notifications(rpc: Arc<RwLock<Connection>>) {
		if let Some(channel) = DB.get().unwrap().notifications() {
			let canceller = rpc.read().await.canceller.clone();
			loop {
				tokio::select! {
					//
					biased;
					// Check if this has shutdown
					_ = canceller.cancelled() => break,
					//
					msg = channel.recv() => {
						if let Ok(notification) = msg {
							// Find which WebSocket the notification belongs to
							if let Some(ws_id) = LIVE_QUERIES.read().await.get(&notification.id) {
								// Check to see if the WebSocket exists
								if let Some(WebSocketRef(ws, _)) = WEBSOCKETS.read().await.get(ws_id) {
									// Serialize the message to send
									let message = success(None, notification);
									// Get the current output format
									let format = rpc.read().await.format;
									// Send the notification to the client
									message.send(format, ws).await
								}
							}
						}
					},
				}
			}
		}
	}

	/// Handle individual WebSocket messages
	async fn handle_message(rpc: Arc<RwLock<Connection>>, msg: Message, chn: Sender<Message>) {
		// Get the current output format
		let mut out_fmt = rpc.read().await.format;
		// Prepare Span and Otel context
		let span = span_for_request(&rpc.read().await.ws_id);
		// Acquire concurrent request rate limiter
		let permit = rpc.read().await.limiter.clone().acquire_owned().await.unwrap();
		// Parse the request
		async move {
			let span = Span::current();
			let req_cx = RequestContext::default();
			let otel_cx = TelemetryContext::new().with_value(req_cx.clone());

			match parse_request(msg).await {
				Ok(req) => {
					if let Some(fmt) = req.out_fmt {
						if out_fmt != fmt {
							// Update the default format
							rpc.write().await.format = fmt;
							out_fmt = fmt;
						}
					}

					// Now that we know the method, we can update the span and create otel context
					span.record("rpc.method", &req.method);
					span.record("otel.name", format!("surrealdb.rpc/{}", req.method));
					span.record(
						"rpc.request_id",
						req.id.clone().map(Value::as_string).unwrap_or_default(),
					);
					let otel_cx = TelemetryContext::current_with_value(
						req_cx.with_method(&req.method).with_size(req.size),
					);
					// Process the message
					let res =
						Connection::process_message(rpc.clone(), &req.method, req.params).await;
					// Process the response
					res.into_response(req.id).send(out_fmt, &chn).with_context(otel_cx).await
				}
				Err(err) => {
					// Process the response
					failure(None, err).send(out_fmt, &chn).with_context(otel_cx).await
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
		// Match the method to a function
		match method {
			// Handle a surrealdb ping message
			//
			// This is used to keep the WebSocket connection alive in environments where the WebSocket protocol is not enough.
			// For example, some browsers will wait for the TCP protocol to timeout before triggering an on_close event. This may take several seconds or even minutes in certain scenarios.
			// By sending a ping message every few seconds from the client, we can force a connection check and trigger an on_close event if the ping can't be sent.
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
				Ok(v) => rpc.read().await.kill(v).await.map(Into::into).map_err(Into::into),
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
			"merge" => match params.needs_one_or_two() {
				Ok((v, o)) => {
					rpc.read().await.merge(v, o).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Update a value or values in the database using `PATCH`
			"patch" => match params.needs_one_two_or_three() {
				Ok((v, o, d)) => {
					rpc.read().await.patch(v, o, d).await.map(Into::into).map_err(Into::into)
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
				Ok((v, o)) if (v.is_strand() || v.is_query()) && o.is_none_or_null() => {
					rpc.read().await.query(v).await.map(Into::into).map_err(Into::into)
				}
				Ok((v, Value::Object(o))) if v.is_strand() || v.is_query() => {
					rpc.read().await.query_with(v, o).await.map(Into::into).map_err(Into::into)
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
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the query parameters
		let var = Some(self.vars.to_owned());
		// Compute the specified parameter
		match kvs.compute(val, &self.session, var).await? {
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
			String::from("id") => id, // NOTE: id can be parameter
			=> &self.vars
		};
		// Execute the query on the database
		let mut res = self.query_with(Value::from(sql), Object::from(var)).await?;
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
		let mut res = self.query_with(Value::from(sql), Object::from(var)).await?;
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
		let sql = if data.is_none_or_null() {
			"CREATE $what RETURN AFTER"
		} else {
			"CREATE $what CONTENT $data RETURN AFTER"
		};
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
		let sql = if data.is_none_or_null() {
			"UPDATE $what RETURN AFTER"
		} else {
			"UPDATE $what CONTENT $data RETURN AFTER"
		};
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
	// Methods for merging
	// ------------------------------

	async fn merge(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = if data.is_none_or_null() {
			"UPDATE $what RETURN AFTER"
		} else {
			"UPDATE $what MERGE $data RETURN AFTER"
		};
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
	// Methods for patching
	// ------------------------------

	async fn patch(&self, what: Value, data: Value, diff: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = match diff.is_true() {
			true => "UPDATE $what PATCH $data RETURN DIFF",
			false => "UPDATE $what PATCH $data RETURN AFTER",
		};
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

	async fn query(&self, sql: Value) -> Result<Vec<Response>, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the query parameters
		let var = Some(self.vars.clone());
		// Execute the query on the database
		let res = match sql {
			Value::Query(sql) => kvs.process(sql, &self.session, var).await?,
			Value::Strand(sql) => kvs.execute(&sql, &self.session, var).await?,
			_ => unreachable!(),
		};

		// Post-process hooks for web layer
		for response in &res {
			self.handle_live_query_results(response).await;
		}
		// Return the result to the client
		Ok(res)
	}

	async fn query_with(&self, sql: Value, mut vars: Object) -> Result<Vec<Response>, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the query parameters
		let var = Some(mrg! { vars.0, &self.vars });
		// Execute the query on the database
		let res = match sql {
			Value::Query(sql) => kvs.process(sql, &self.session, var).await?,
			Value::Strand(sql) => kvs.execute(&sql, &self.session, var).await?,
			_ => unreachable!(),
		};
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
