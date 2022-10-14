use crate::cli::CF;
use crate::cnf::MAX_CONCURRENT_CALLS;
use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERS;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::session;
use crate::net::LOG;
use crate::rpc::args::Take;
use crate::rpc::paths::{ID, METHOD, PARAMS};
use crate::rpc::res::Failure;
use crate::rpc::res::Response;
use futures::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use surrealdb::channel;
use surrealdb::channel::Sender;
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
	vars: BTreeMap<String, Value>,
}

impl Rpc {
	// Instantiate a new RPC
	pub fn new(mut session: Session) -> Arc<RwLock<Rpc>> {
		// Create a new RPC variables store
		let vars = BTreeMap::new();
		// Enable real-time live queries
		session.rt = true;
		// Create and store the Rpc connection
		Arc::new(RwLock::new(Rpc {
			session,
			vars,
		}))
	}

	// Serve the RPC endpoint
	pub async fn serve(rpc: Arc<RwLock<Rpc>>, ws: WebSocket) {
		// Create a channel for sending messages
		let (chn, mut rcv) = channel::new(MAX_CONCURRENT_CALLS);
		// Split the socket into send and recv
		let (mut wtx, mut wrx) = ws.split();
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
				Ok(msg) => {
					if msg.is_text() {
						tokio::task::spawn(Rpc::call(rpc.clone(), msg, chn.clone()));
					}
				}
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
		// Clone the RPC
		let rpc = rpc.clone();
		// Convert the message
		let str = match msg.to_str() {
			Ok(v) => v,
			_ => return Response::failure(None, Failure::INTERNAL_ERROR).send(chn).await,
		};
		// Parse the request
		let req = match surrealdb::sql::json(str) {
			Ok(v) if v.is_some() => v,
			_ => return Response::failure(None, Failure::PARSE_ERROR).send(chn).await,
		};
		// Fetch the 'id' argument
		let id = match req.pick(&*ID) {
			Value::Uuid(v) => Some(v.to_raw()),
			Value::Strand(v) => Some(v.to_raw()),
			Value::Number(v) => Some(v.to_string()),
			_ => return Response::failure(None, Failure::INVALID_REQUEST).send(chn).await,
		};
		// Fetch the 'method' argument
		let method = match req.pick(&*METHOD) {
			Value::Strand(v) => v.to_raw(),
			_ => return Response::failure(id, Failure::INVALID_REQUEST).send(chn).await,
		};
		// Fetch the 'params' argument
		let params = match req.pick(&*PARAMS) {
			Value::Array(v) => v,
			_ => return Response::failure(id, Failure::INVALID_REQUEST).send(chn).await,
		};
		// Match the method to a function
		let res = match &method[..] {
			"ping" => Ok(Value::True),
			"info" => match params.len() {
				0 => rpc.read().await.info().await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"use" => match params.take_two() {
				(Value::Strand(ns), Value::Strand(db)) => rpc.write().await.yuse(ns, db).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"signup" => match params.take_one() {
				Value::Object(v) => rpc.write().await.signup(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"signin" => match params.take_one() {
				Value::Object(v) => rpc.write().await.signin(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"invalidate" => match params.len() {
				0 => rpc.write().await.invalidate().await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"authenticate" => match params.take_one() {
				Value::None => rpc.write().await.invalidate().await,
				Value::Strand(v) => rpc.write().await.authenticate(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"kill" => match params.take_one() {
				v if v.is_uuid() => rpc.read().await.kill(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"live" => match params.take_one() {
				v if v.is_strand() => rpc.read().await.live(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"let" => match params.take_two() {
				(Value::Strand(s), v) => rpc.write().await.set(s, v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"set" => match params.take_two() {
				(Value::Strand(s), v) => rpc.write().await.set(s, v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"query" => match params.take_two() {
				(Value::Strand(s), o) if o.is_none() => rpc.read().await.query(s).await,
				(Value::Strand(s), Value::Object(o)) => rpc.read().await.query_with(s, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"select" => match params.take_one() {
				v if v.is_thing() => rpc.read().await.select(v).await,
				v if v.is_strand() => rpc.read().await.select(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"create" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_none() => rpc.read().await.create(v, None).await,
				(v, o) if v.is_strand() && o.is_none() => rpc.read().await.create(v, None).await,
				(v, o) if v.is_thing() && o.is_object() => rpc.read().await.create(v, o).await,
				(v, o) if v.is_strand() && o.is_object() => rpc.read().await.create(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"update" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_none() => rpc.read().await.update(v, None).await,
				(v, o) if v.is_strand() && o.is_none() => rpc.read().await.update(v, None).await,
				(v, o) if v.is_thing() && o.is_object() => rpc.read().await.update(v, o).await,
				(v, o) if v.is_strand() && o.is_object() => rpc.read().await.update(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"change" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_none() => rpc.read().await.change(v, None).await,
				(v, o) if v.is_strand() && o.is_none() => rpc.read().await.change(v, None).await,
				(v, o) if v.is_thing() && o.is_object() => rpc.read().await.change(v, o).await,
				(v, o) if v.is_strand() && o.is_object() => rpc.read().await.change(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"modify" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_array() => rpc.read().await.modify(v, o).await,
				(v, o) if v.is_strand() && o.is_array() => rpc.read().await.modify(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"delete" => match params.take_one() {
				v if v.is_thing() => rpc.read().await.delete(v).await,
				v if v.is_strand() => rpc.read().await.delete(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			"version" => match params.len() {
				0 => Ok(format!("{}-{}", PKG_NAME, *PKG_VERS).into()),
				_ => return Response::failure(id, Failure::INVALID_PARAMS).send(chn).await,
			},
			_ => return Response::failure(id, Failure::METHOD_NOT_FOUND).send(chn).await,
		};
		// Return the final response
		match res {
			Ok(v) => Response::success(id, v).send(chn).await,
			Err(e) => Response::failure(id, Failure::custom(e.to_string())).send(chn).await,
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(&mut self, ns: Strand, db: Strand) -> Result<Value, Error> {
		self.session.ns = Some(ns.0);
		self.session.db = Some(db.0);
		Ok(Value::None)
	}

	async fn signup(&mut self, vars: Object) -> Result<Value, Error> {
		crate::iam::signup::signup(&mut self.session, vars)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}

	async fn signin(&mut self, vars: Object) -> Result<Value, Error> {
		crate::iam::signin::signin(&mut self.session, vars)
			.await
			.map(Into::into)
			.map_err(Into::into)
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
			// Remove the variable if the value is NULL
			v if v.is_null() => {
				self.vars.remove(&key.0);
				Ok(Value::Null)
			}
			// Store the variable if not NULL
			v => {
				self.vars.insert(key.0, v);
				Ok(Value::Null)
			}
		}
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
			String::from("tb") => tb.make_table(),
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
	// Methods for querying
	// ------------------------------

	async fn query(&self, sql: Strand) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the query parameters
		let var = Some(self.vars.clone());
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var, opt.strict).await?;
		// Extract the first query result
		let res = res.into_iter().collect::<Vec<Value>>().into();
		// Return the result to the client
		Ok(res)
	}

	async fn query_with(&self, sql: Strand, mut vars: Object) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the query parameters
		let var = Some(mrg! { vars.0, &self.vars });
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var, opt.strict).await?;
		// Extract the first query result
		let res = res.into_iter().collect::<Vec<Value>>().into();
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, what: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "SELECT * FROM $what";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
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
	// Methods for creating
	// ------------------------------

	async fn create(&self, what: Value, data: impl Into<Option<Value>>) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "CREATE $what CONTENT $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data.into().into(),
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
	// Methods for updating
	// ------------------------------

	async fn update(&self, what: Value, data: impl Into<Option<Value>>) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what CONTENT $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data.into().into(),
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
	// Methods for changing
	// ------------------------------

	async fn change(&self, what: Value, data: impl Into<Option<Value>>) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what MERGE $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data.into().into(),
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
	// Methods for modifying
	// ------------------------------

	async fn modify(&self, what: Value, data: impl Into<Option<Value>>) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what PATCH $data RETURN DIFF";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data.into().into(),
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
	// Methods for deleting
	// ------------------------------

	async fn delete(&self, what: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get local copy of options
		let opt = CF.get().unwrap();
		// Specify the SQL query string
		let sql = "DELETE $what";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var, opt.strict).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}
}
