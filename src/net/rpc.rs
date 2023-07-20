use crate::cli::CF;
use crate::cnf::MAX_CONCURRENT_CALLS;
use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::cnf::WEBSOCKET_PING_FREQUENCY;
use crate::dbs::DB;
use crate::err::Error;
use crate::rpc::args::Take;
use crate::rpc::paths::{ID, METHOD, PARAMS};
use crate::rpc::res;
use crate::rpc::res::Failure;
use crate::rpc::res::Output;
use axum::routing::get;
use axum::Extension;
use axum::Router;
use futures::{SinkExt, StreamExt};
use http_body::Body as HttpBody;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use surrealdb::channel;
use surrealdb::channel::Sender;
use surrealdb::dbs::{QueryType, Response, Session};
use surrealdb::opt::auth::Root;
use surrealdb::sql::Array;
use surrealdb::sql::Object;
use surrealdb::sql::Strand;
use surrealdb::sql::Value;
use tokio::sync::RwLock;
use tracing::instrument;
use uuid::Uuid;

use axum::{
	extract::ws::{Message, WebSocket, WebSocketUpgrade},
	response::IntoResponse,
};

// Mapping of WebSocketID to WebSocket
type WebSockets = RwLock<HashMap<Uuid, Sender<Message>>>;
// Mapping of LiveQueryID to WebSocketID
type LiveQueries = RwLock<HashMap<Uuid, Uuid>>;

static WEBSOCKETS: Lazy<WebSockets> = Lazy::new(WebSockets::default);
static LIVE_QUERIES: Lazy<LiveQueries> = Lazy::new(LiveQueries::default);

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/rpc", get(handler))
}

async fn handler(ws: WebSocketUpgrade, Extension(sess): Extension<Session>) -> impl IntoResponse {
	// finalize the upgrade process by returning upgrade callback.
	// we can customize the callback by sending additional info such as address.
	ws.on_upgrade(move |socket| handle_socket(socket, sess))
}

async fn handle_socket(ws: WebSocket, sess: Session) {
	let rpc = Rpc::new(sess);
	Rpc::serve(rpc, ws).await
}

pub struct Rpc {
	session: Session,
	format: Output,
	uuid: Uuid,
	vars: BTreeMap<String, Value>,
}

impl Rpc {
	/// Instantiate a new RPC
	pub fn new(mut session: Session) -> Arc<RwLock<Rpc>> {
		// Create a new RPC variables store
		let vars = BTreeMap::new();
		// Set the default output format
		let format = Output::Json;
		// Create a unique WebSocket id
		let uuid = Uuid::new_v4();
		// Enable real-time live queries
		session.rt = true;
		// Create and store the Rpc connection
		Arc::new(RwLock::new(Rpc {
			session,
			format,
			uuid,
			vars,
		}))
	}

	/// Serve the RPC endpoint
	pub async fn serve(rpc: Arc<RwLock<Rpc>>, ws: WebSocket) {
		// Create a channel for sending messages
		let (chn, mut rcv) = channel::new(MAX_CONCURRENT_CALLS);
		// Split the socket into send and recv
		let (mut wtx, mut wrx) = ws.split();
		// Clone the channel for sending pings
		let png = chn.clone();
		// The WebSocket has connected
		Rpc::connected(rpc.clone(), chn.clone()).await;
		// Send Ping messages to the client
		tokio::task::spawn(async move {
			// Create the interval ticker
			let mut interval = tokio::time::interval(WEBSOCKET_PING_FREQUENCY);
			// Loop indefinitely
			loop {
				// Wait for the timer
				interval.tick().await;
				// Create the ping message
				let msg = Message::Ping(vec![]);
				// Send the message to the client
				if png.send(msg).await.is_err() {
					// Exit out of the loop
					break;
				}
			}
		});
		// Send messages to the client
		tokio::task::spawn(async move {
			// Wait for the next message to send
			while let Some(res) = rcv.next().await {
				// Send the message to the client
				if let Err(err) = wtx.send(res).await {
					// Output the WebSocket error to the logs
					trace!("WebSocket error: {:?}", err);
					// It's already failed, so ignore error
					let _ = wtx.close().await;
					// Exit out of the loop
					break;
				}
			}
		});
		// Send notifications to the client
		let moved_rpc = rpc.clone();
		tokio::task::spawn(async move {
			let rpc = moved_rpc;
			if let Some(channel) = DB.get().unwrap().notifications() {
				while let Ok(notification) = channel.recv().await {
					// Find which WebSocket the notification belongs to
					if let Some(ws_id) = LIVE_QUERIES.read().await.get(&notification.id) {
						// Check to see if the WebSocket exists
						if let Some(websocket) = WEBSOCKETS.read().await.get(ws_id) {
							// Serialize the message to send
							let message = res::success(None, notification);
							// Get the current output format
							let format = rpc.read().await.format.clone();
							// Send the notification to the client
							message.send(format, websocket.clone()).await;
						}
					}
				}
			}
		});
		// Get messages from the client
		while let Some(msg) = wrx.next().await {
			match msg {
				// We've received a message from the client
				// Ping is automatically handled by the WebSocket library
				Ok(msg) => match msg {
					Message::Text(_) => {
						tokio::task::spawn(Rpc::call(rpc.clone(), msg, chn.clone()));
					}
					Message::Binary(_) => {
						tokio::task::spawn(Rpc::call(rpc.clone(), msg, chn.clone()));
					}
					Message::Close(_) => {
						break;
					}
					Message::Pong(_) => {
						continue;
					}
					_ => {
						// Ignore everything else
					}
				},
				// There was an error receiving the message
				Err(err) => {
					// Output the WebSocket error to the logs
					trace!("WebSocket error: {:?}", err);
					// Exit out of the loop
					break;
				}
			}
		}
		// The WebSocket has disconnected
		Rpc::disconnected(rpc.clone()).await;
	}

	async fn connected(rpc: Arc<RwLock<Rpc>>, chn: Sender<Message>) {
		// Fetch the unique id of the WebSocket
		let id = rpc.read().await.uuid;
		// Log that the WebSocket has connected
		trace!("WebSocket {} connected", id);
		// Store this WebSocket in the list of WebSockets
		WEBSOCKETS.write().await.insert(id, chn);
	}

	async fn disconnected(rpc: Arc<RwLock<Rpc>>) {
		// Fetch the unique id of the WebSocket
		let id = rpc.read().await.uuid;
		// Log that the WebSocket has disconnected
		trace!("WebSocket {} disconnected", id);
		// Remove this WebSocket from the list of WebSockets
		WEBSOCKETS.write().await.remove(&id);
		// Remove all live queries
		LIVE_QUERIES.write().await.retain(|key, value| {
			if value == &id {
				trace!("Removing live query: {}", key);
				return false;
			}
			true
		});
	}

	/// Call RPC methods from the WebSocket
	async fn call(rpc: Arc<RwLock<Rpc>>, msg: Message, chn: Sender<Message>) {
		// Get the current output format
		let mut out = { rpc.read().await.format.clone() };
		// Clone the RPC
		let rpc = rpc.clone();
		// Parse the request
		let req = match msg {
			// This is a binary message
			Message::Binary(val) => {
				// Use binary output
				out = Output::Full;
				// Deserialize the input
				Value::from(val)
			}
			// This is a text message
			Message::Text(ref val) => {
				// Parse the SurrealQL object
				match surrealdb::sql::value(val) {
					// The SurrealQL message parsed ok
					Ok(v) => v,
					// The SurrealQL message failed to parse
					_ => return res::failure(None, Failure::PARSE_ERROR).send(out, chn).await,
				}
			}
			// Unsupported message type
			_ => return res::failure(None, Failure::INTERNAL_ERROR).send(out, chn).await,
		};
		// Log the received request
		trace!("RPC Received: {}", req);
		// Fetch the 'id' argument
		let id = match req.pick(&*ID) {
			v if v.is_none() => None,
			v if v.is_null() => Some(v),
			v if v.is_uuid() => Some(v),
			v if v.is_number() => Some(v),
			v if v.is_strand() => Some(v),
			v if v.is_datetime() => Some(v),
			_ => return res::failure(None, Failure::INVALID_REQUEST).send(out, chn).await,
		};
		// Fetch the 'method' argument
		let method = match req.pick(&*METHOD) {
			Value::Strand(v) => v.to_raw(),
			_ => return res::failure(id, Failure::INVALID_REQUEST).send(out, chn).await,
		};
		// Fetch the 'params' argument
		let params = match req.pick(&*PARAMS) {
			Value::Array(v) => v,
			_ => Array::new(),
		};
		// Match the method to a function
		let res = match &method[..] {
			// Handle a ping message
			"ping" => Ok(Value::None),
			// Retrieve the current auth record
			"info" => match params.len() {
				0 => rpc.read().await.info().await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Switch to a specific namespace and database
			"use" => match params.needs_two() {
				Ok((ns, db)) => rpc.write().await.yuse(ns, db).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Signup to a specific authentication scope
			"signup" => match params.needs_one() {
				Ok(Value::Object(v)) => rpc.write().await.signup(v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Signin as a root, namespace, database or scope user
			"signin" => match params.needs_one() {
				Ok(Value::Object(v)) => rpc.write().await.signin(v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Invalidate the current authentication session
			"invalidate" => match params.len() {
				0 => rpc.write().await.invalidate().await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Authenticate using an authentication token
			"authenticate" => match params.needs_one() {
				Ok(Value::Strand(v)) => rpc.write().await.authenticate(v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Kill a live query using a query id
			"kill" => match params.needs_one() {
				Ok(v) if v.is_uuid() => rpc.read().await.kill(v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Setup a live query on a specific table
			"live" => match params.needs_one_or_two() {
				Ok((v, d)) if v.is_table() => rpc.read().await.live(v, d).await,
				Ok((v, d)) if v.is_strand() => rpc.read().await.live(v, d).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Specify a connection-wide parameter
			"let" => match params.needs_one_or_two() {
				Ok((Value::Strand(s), v)) => rpc.write().await.set(s, v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Specify a connection-wide parameter
			"set" => match params.needs_one_or_two() {
				Ok((Value::Strand(s), v)) => rpc.write().await.set(s, v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Unset and clear a connection-wide parameter
			"unset" => match params.needs_one() {
				Ok(Value::Strand(s)) => rpc.write().await.unset(s).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Select a value or values from the database
			"select" => match params.needs_one() {
				Ok(v) => rpc.read().await.select(v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Insert a value or values in the database
			"insert" => match params.needs_one_or_two() {
				Ok((v, o)) => rpc.read().await.insert(v, o).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Create a value or values in the database
			"create" => match params.needs_one_or_two() {
				Ok((v, o)) => rpc.read().await.create(v, o).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Update a value or values in the database using `CONTENT`
			"update" => match params.needs_one_or_two() {
				Ok((v, o)) => rpc.read().await.update(v, o).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Update a value or values in the database using `MERGE`
			"change" | "merge" => match params.needs_one_or_two() {
				Ok((v, o)) => rpc.read().await.change(v, o).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Update a value or values in the database using `PATCH`
			"modify" | "patch" => match params.needs_one_or_two() {
				Ok((v, o)) => rpc.read().await.modify(v, o).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Delete a value or values from the database
			"delete" => match params.needs_one() {
				Ok(v) => rpc.read().await.delete(v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Specify the output format for text requests
			"format" => match params.needs_one() {
				Ok(Value::Strand(v)) => rpc.write().await.format(v).await,
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Get the current server version
			"version" => match params.len() {
				0 => Ok(format!("{PKG_NAME}-{}", *PKG_VERSION).into()),
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Run a full SurrealQL query against the database
			"query" => match params.needs_one_or_two() {
				Ok((Value::Strand(s), o)) if o.is_none_or_null() => {
					return match rpc.read().await.query(s).await {
						Ok(v) => res::success(id, v).send(out, chn).await,
						Err(e) => {
							res::failure(id, Failure::custom(e.to_string())).send(out, chn).await
						}
					};
				}
				Ok((Value::Strand(s), Value::Object(o))) => {
					return match rpc.read().await.query_with(s, o).await {
						Ok(v) => res::success(id, v).send(out, chn).await,
						Err(e) => {
							res::failure(id, Failure::custom(e.to_string())).send(out, chn).await
						}
					};
				}
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			_ => return res::failure(id, Failure::METHOD_NOT_FOUND).send(out, chn).await,
		};
		// Return the final response
		match res {
			Ok(v) => res::success(id, v).send(out, chn).await,
			Err(e) => res::failure(id, Failure::custom(e.to_string())).send(out, chn).await,
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn format(&mut self, out: Strand) -> Result<Value, Error> {
		match out.as_str() {
			"json" | "application/json" => self.format = Output::Json,
			"cbor" | "application/cbor" => self.format = Output::Cbor,
			"pack" | "application/pack" => self.format = Output::Pack,
			_ => return Err(Error::InvalidType),
		};
		Ok(Value::None)
	}

	#[instrument(skip_all, name = "rpc use", fields(websocket=self.uuid.to_string()))]
	async fn yuse(&mut self, ns: Value, db: Value) -> Result<Value, Error> {
		if let Value::Strand(ns) = ns {
			self.session.ns = Some(ns.0);
		}
		if let Value::Strand(db) = db {
			self.session.db = Some(db.0);
		}
		Ok(Value::None)
	}

	#[instrument(skip_all, name = "rpc signup", fields(websocket=self.uuid.to_string()))]
	async fn signup(&mut self, vars: Object) -> Result<Value, Error> {
		let kvs = DB.get().unwrap();
		surrealdb::iam::signup::signup(kvs, &mut self.session, vars)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}

	#[instrument(skip_all, name = "rpc signin", fields(websocket=self.uuid.to_string()))]
	async fn signin(&mut self, vars: Object) -> Result<Value, Error> {
		let kvs = DB.get().unwrap();
		let opts = CF.get().unwrap();
		let root = opts.pass.as_ref().map(|pass| Root {
			username: &opts.user,
			password: pass,
		});
		surrealdb::iam::signin::signin(kvs, &root, &mut self.session, vars)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}
	#[instrument(skip_all, name = "rpc invalidate", fields(websocket=self.uuid.to_string()))]
	async fn invalidate(&mut self) -> Result<Value, Error> {
		surrealdb::iam::clear::clear(&mut self.session)?;
		Ok(Value::None)
	}

	#[instrument(skip_all, name = "rpc auth", fields(websocket=self.uuid.to_string()))]
	async fn authenticate(&mut self, token: Strand) -> Result<Value, Error> {
		let kvs = DB.get().unwrap();
		surrealdb::iam::verify::token(kvs, &mut self.session, token.0).await?;
		Ok(Value::None)
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	#[instrument(skip_all, name = "rpc info", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc set", fields(websocket=self.uuid.to_string()))]
	async fn set(&mut self, key: Strand, val: Value) -> Result<Value, Error> {
		match val {
			// Remove the variable if undefined
			Value::None => self.vars.remove(&key.0),
			// Store the variable if defined
			v => self.vars.insert(key.0, v),
		};
		Ok(Value::Null)
	}

	#[instrument(skip_all, name = "rpc unset", fields(websocket=self.uuid.to_string()))]
	async fn unset(&mut self, key: Strand) -> Result<Value, Error> {
		self.vars.remove(&key.0);
		Ok(Value::Null)
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	#[instrument(skip_all, name = "rpc kill", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc live", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc select", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc insert", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc create", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc update", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc change", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc modify", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc delete", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc query", fields(websocket=self.uuid.to_string()))]
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

	#[instrument(skip_all, name = "rpc query_with", fields(websocket=self.uuid.to_string()))]
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
					LIVE_QUERIES.write().await.insert(lqid.0, self.uuid);
					trace!("Registered live query {} on websocket {}", lqid, self.uuid);
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
