use crate::cli::CF;
use crate::cnf::MAX_CONCURRENT_CALLS;
use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::cnf::WEBSOCKET_PING_FREQUENCY;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::session;
use crate::net::LOG;
use crate::rpc::args::Take;
use crate::rpc::paths::{ID, METHOD, PARAMS};
use crate::rpc::res;
use crate::rpc::res::Failure;
use crate::rpc::res::Output;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use surrealdb::channel;
use surrealdb::channel::Sender;
use surrealdb::sql::Array;
use surrealdb::sql::Object;
use surrealdb::sql::Strand;
use surrealdb::sql::Value;
use surrealdb::Session;
use tokio::sync::RwLock;
use warp::ws::{Message, WebSocket, Ws};
use warp::Filter;

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("rpc")
		.and(warp::path::end())
		.and(warp::ws())
		.and(session::build())
		.map(|ws: Ws, session: Session| ws.on_upgrade(move |ws| socket(ws, session)))
}

async fn socket(ws: WebSocket, session: Session) {
	let rpc = Rpc::new(session);
	Rpc::serve(rpc, ws).await
}

pub struct Rpc {
	session: Session,
	format: Output,
	vars: BTreeMap<String, Value>,
}

impl Rpc {
	// Instantiate a new RPC
	pub fn new(mut session: Session) -> Arc<RwLock<Rpc>> {
		// Create a new RPC variables store
		let vars = BTreeMap::new();
		// Set the default output format
		let format = Output::Json;
		// Enable real-time live queries
		session.rt = true;
		// Create and store the Rpc connection
		Arc::new(RwLock::new(Rpc {
			session,
			format,
			vars,
		}))
	}

	// Serve the RPC endpoint
	pub async fn serve(rpc: Arc<RwLock<Rpc>>, ws: WebSocket) {
		// Create a channel for sending messages
		let (chn, mut rcv) = channel::new(MAX_CONCURRENT_CALLS);
		// Split the socket into send and recv
		let (mut wtx, mut wrx) = ws.split();
		// Clone the channel for sending pings
		let png = chn.clone();
		// Send messages to the client
		tokio::task::spawn(async move {
			// Create the interval ticker
			let mut interval = tokio::time::interval(WEBSOCKET_PING_FREQUENCY);
			// Loop indefinitely
			loop {
				// Wait for the timer
				interval.tick().await;
				// Create the ping message
				let msg = Message::ping(vec![]);
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
					trace!(target: LOG, "WebSocket error: {:?}", err);
					// It's already failed, so ignore error
					let _ = wtx.close().await;
					// Exit out of the loop
					break;
				}
			}
		});
		// Get messages from the client
		while let Some(msg) = wrx.next().await {
			match msg {
				// We've received a message from the client
				Ok(msg) => match msg {
					msg if msg.is_ping() => {
						let _ = chn.send(Message::pong(vec![]));
					}
					msg if msg.is_text() => {
						tokio::task::spawn(Rpc::call(rpc.clone(), msg, chn.clone()));
					}
					msg if msg.is_binary() => {
						tokio::task::spawn(Rpc::call(rpc.clone(), msg, chn.clone()));
					}
					msg if msg.is_close() => {
						break;
					}
					msg if msg.is_pong() => {
						continue;
					}
					_ => {
						// Ignore everything else
					}
				},
				// There was an error receiving the message
				Err(err) => {
					// Output the WebSocket error to the logs
					trace!(target: LOG, "WebSocket error: {:?}", err);
					// Exit out of the loop
					break;
				}
			}
		}
	}

	// Call RPC methods from the WebSocket
	async fn call(rpc: Arc<RwLock<Rpc>>, msg: Message, chn: Sender<Message>) {
		// Get the current output format
		let mut out = { rpc.read().await.format.clone() };
		// Clone the RPC
		let rpc = rpc.clone();
		// Parse the request
		let req = match msg {
			// This is a binary message
			m if m.is_binary() => {
				// Use binary output
				out = Output::Full;
				// Deserialize the input
				Value::from(m.into_bytes())
			}
			// This is a text message
			m if m.is_text() => {
				// This won't panic due to the check above
				let val = m.to_str().unwrap();
				// Parse the SurrealQL object
				match surrealdb::sql::json(val) {
					// The SurrealQL message parsed ok
					Ok(v) => v,
					// The SurrealQL message failed to parse
					_ => return res::failure(None, Failure::PARSE_ERROR).send(out, chn).await,
				}
			}
			// Unsupported message type
			_ => return res::failure(None, Failure::INTERNAL_ERROR).send(out, chn).await,
		};
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
				Ok((Value::Strand(ns), Value::Strand(db))) => rpc.write().await.yuse(ns, db).await,
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
			"live" => match params.needs_one() {
				Ok(v) if v.is_strand() => rpc.read().await.live(v).await,
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
			// Delete a value or values from teh database
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
				0 => Ok(format!("{}-{}", PKG_NAME, *PKG_VERSION).into()),
				_ => return res::failure(id, Failure::INVALID_PARAMS).send(out, chn).await,
			},
			// Run a full SurrealQL query against the database
			"query" => match params.needs_one_or_two() {
				Ok((Value::Strand(s), o)) if o.is_none() => {
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
			"msgpack" | "application/msgpack" => self.format = Output::Pack,
			_ => return Err(Error::InvalidType),
		};
		Ok(Value::None)
	}

	async fn yuse(&mut self, ns: Strand, db: Strand) -> Result<Value, Error> {
		self.session.ns = Some(ns.0);
		self.session.db = Some(db.0);
		Ok(Value::None)
	}

	async fn signup(&mut self, vars: Object) -> Result<Value, Error> {
		crate::iam::signup::signup(&mut self.session, vars).await.map_err(Into::into)
	}

	async fn signin(&mut self, vars: Object) -> Result<Value, Error> {
		crate::iam::signin::signin(&mut self.session, vars).await.map_err(Into::into)
	}

	async fn invalidate(&mut self) -> Result<Value, Error> {
		crate::iam::clear::clear(&mut self.session).await?;
		Ok(Value::None)
	}

	async fn authenticate(&mut self, token: Strand) -> Result<Value, Error> {
		crate::iam::verify::token(&mut self.session, token.0).await?;
		Ok(Value::None)
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "SELECT * FROM $auth";
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, None, opt.strict).await?;
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
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "KILL $id";
		// Specify the query parameters
		let var = Some(map! {
			String::from("id") => id,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	async fn live(&self, tb: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "LIVE SELECT * FROM $tb";
		// Specify the query parameters
		let var = Some(map! {
			String::from("tb") => tb.could_be_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, what: Value) -> Result<Value, Error> {
		// Return a single result?
		let one = what.is_thing();
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "SELECT * FROM $what";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
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
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "CREATE $what CONTENT $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
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
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what CONTENT $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
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
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what MERGE $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
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
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what PATCH $data RETURN DIFF";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
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
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "DELETE $what";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
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

	async fn query(&self, sql: Strand) -> Result<impl Serialize, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the query parameters
		let var = Some(self.vars.clone());
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var, opt.strict).await?;
		// Return the result to the client
		Ok(res)
	}

	async fn query_with(&self, sql: Strand, mut vars: Object) -> Result<impl Serialize, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the query parameters
		let var = Some(mrg! { vars.0, &self.vars });
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var, opt.strict).await?;
		// Return the result to the client
		Ok(res)
	}
}
