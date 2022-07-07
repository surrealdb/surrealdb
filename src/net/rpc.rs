use crate::dbs::DB;
use crate::err::Error;
use crate::net::session;
use crate::rpc::args::Take;
use crate::rpc::paths::{ID, METHOD, PARAMS};
use crate::rpc::res::Failure;
use crate::rpc::res::Response;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use surrealdb::sql::Object;
use surrealdb::sql::Strand;
use surrealdb::sql::Value;
use surrealdb::Session;
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
	Rpc::new(ws, session).serve().await
}

pub struct Rpc {
	session: Session,
	vars: BTreeMap<String, Value>,
	tx: SplitSink<WebSocket, Message>,
	rx: SplitStream<WebSocket>,
}

impl Rpc {
	// Instantiate a new RPC
	pub fn new(ws: WebSocket, mut session: Session) -> Rpc {
		// Create a new RPC variables store
		let vars = BTreeMap::new();
		// Split the WebSocket connection
		let (tx, rx) = ws.split();
		// Enable real-time live queries
		session.rt = true;
		// Create and store the Rpc connection
		Rpc {
			session,
			vars,
			tx,
			rx,
		}
	}

	// Serve the RPC endpoint
	pub async fn serve(&mut self) {
		while let Some(msg) = self.rx.next().await {
			if let Ok(msg) = msg {
				match true {
					_ if msg.is_text() => {
						let res = self.call(msg).await;
						let res = serde_json::to_string(&res).unwrap();
						let res = Message::text(res);
						let _ = self.tx.send(res).await;
					}
					_ => (),
				}
			}
		}
	}

	// Call RPC methods from the WebSocket
	async fn call(&mut self, msg: Message) -> Response {
		// Convert the message
		let str = match msg.to_str() {
			Ok(v) => v,
			_ => return Response::failure(None, Failure::INTERNAL_ERROR),
		};
		// Parse the request
		let req = match surrealdb::sql::json(str) {
			Ok(v) if v.is_some() => v,
			_ => return Response::failure(None, Failure::PARSE_ERROR),
		};
		// Fetch the 'id' argument
		let id = match req.pick(&*ID) {
			Value::Uuid(v) => Some(v.to_raw()),
			Value::Strand(v) => Some(v.to_raw()),
			_ => return Response::failure(None, Failure::INVALID_REQUEST),
		};
		// Fetch the 'method' argument
		let method = match req.pick(&*METHOD) {
			Value::Strand(v) => v.to_raw(),
			_ => return Response::failure(id, Failure::INVALID_REQUEST),
		};
		// Fetch the 'params' argument
		let params = match req.pick(&*PARAMS) {
			Value::Array(v) => v,
			_ => return Response::failure(id, Failure::INVALID_REQUEST),
		};
		// Match the method to a function
		let res = match &method[..] {
			"ping" => Ok(Value::True),
			"info" => match params.len() {
				0 => self.info().await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"use" => match params.take_two() {
				(Value::Strand(ns), Value::Strand(db)) => self.yuse(ns, db).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"signup" => match params.take_one() {
				Value::Object(v) => self.signup(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"signin" => match params.take_one() {
				Value::Object(v) => self.signin(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"invalidate" => match params.len() {
				0 => self.invalidate().await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"authenticate" => match params.take_one() {
				Value::None => self.invalidate().await,
				Value::Strand(v) => self.authenticate(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"kill" => match params.take_one() {
				v if v.is_uuid() => self.kill(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"live" => match params.take_one() {
				v if v.is_strand() => self.live(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"set" => match params.take_two() {
				(Value::Strand(s), v) => self.set(s, v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"query" => match params.take_two() {
				(Value::Strand(s), Value::None) => self.query(s).await,
				(Value::Strand(s), Value::Object(o)) => self.query_with_vars(s, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"select" => match params.take_one() {
				v if v.is_thing() => self.select(v).await,
				v if v.is_strand() => self.select(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"create" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_none() => self.create(v).await,
				(v, o) if v.is_strand() && o.is_none() => self.create(v).await,
				(v, o) if v.is_thing() && o.is_object() => self.create_with(v, o).await,
				(v, o) if v.is_strand() && o.is_object() => self.create_with(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"update" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_none() => self.update(v).await,
				(v, o) if v.is_strand() && o.is_none() => self.update(v).await,
				(v, o) if v.is_thing() && o.is_object() => self.update_with(v, o).await,
				(v, o) if v.is_strand() && o.is_object() => self.update_with(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"change" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_none() => self.change(v).await,
				(v, o) if v.is_strand() && o.is_none() => self.change(v).await,
				(v, o) if v.is_thing() && o.is_object() => self.change_with(v, o).await,
				(v, o) if v.is_strand() && o.is_object() => self.change_with(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"modify" => match params.take_two() {
				(v, o) if v.is_thing() && o.is_none() => self.modify(v).await,
				(v, o) if v.is_strand() && o.is_none() => self.modify(v).await,
				(v, o) if v.is_thing() && o.is_object() => self.modify_with(v, o).await,
				(v, o) if v.is_strand() && o.is_object() => self.modify_with(v, o).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			"delete" => match params.take_one() {
				v if v.is_thing() => self.delete(v).await,
				v if v.is_strand() => self.delete(v).await,
				_ => return Response::failure(id, Failure::INVALID_PARAMS),
			},
			_ => return Response::failure(id, Failure::METHOD_NOT_FOUND),
		};
		// Return the final response
		match res {
			Ok(v) => Response::success(id, v),
			Err(e) => Response::failure(id, Failure::custom(e.to_string())),
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

	async fn signup(&self, vars: Object) -> Result<Value, Error> {
		crate::iam::signup::signup(vars).await.map(Into::into).map_err(Into::into)
	}

	async fn signin(&self, vars: Object) -> Result<Value, Error> {
		crate::iam::signin::signin(vars).await.map(Into::into).map_err(Into::into)
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
	// Methods for live queries
	// ------------------------------

	async fn kill(&self, id: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "KILL $id";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("id") => id,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	async fn live(&self, tb: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "LIVE SELECT * FROM $tb";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("tb") => tb.make_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn set(&mut self, key: Strand, val: Value) -> Result<Value, Error> {
		match val {
			// Remove the variable if the value is NULL
			v if v.is_null() => {
				self.vars.remove(&key.0);
				Ok(Value::Null)
			}
			// Store the value if the value is not NULL
			v => {
				self.vars.insert(key.0, v);
				Ok(Value::Null)
			}
		}
	}

	async fn query(&self, sql: Strand) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the query paramaters
		let var = Some(self.vars.clone());
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.into_iter().collect::<Vec<Value>>().into();
		// Return the result to the client
		Ok(res)
	}

	async fn query_with_vars(&self, sql: Strand, mut vars: Object) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the query paramaters
		let var = Some(mrg! { vars.0, &self.vars });
		// Execute the query on the database
		let res = kvs.execute(&sql, &self.session, var).await?;
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
		// Specify the SQL query string
		let sql = "SELECT * FROM $what";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(&self, what: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "CREATE $what RETURN AFTER";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	async fn create_with(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "CREATE $what CONTENT $data RETURN AFTER";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(&self, what: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what RETURN AFTER";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	async fn update_with(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what CONTENT $data RETURN AFTER";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for changing
	// ------------------------------

	async fn change(&self, what: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what RETURN AFTER";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	async fn change_with(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what MERGE $data RETURN AFTER";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for modifying
	// ------------------------------

	async fn modify(&self, what: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what RETURN DIFF";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}

	async fn modify_with(&self, what: Value, data: Value) -> Result<Value, Error> {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Specify the SQL query string
		let sql = "UPDATE $what PATCH $data RETURN DIFF";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
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
		// Specify the SQL query string
		let sql = "DELETE $what";
		// Specify the query paramaters
		let var = Some(map! {
			String::from("what") => what.make_table_or_thing(),
			=> &self.vars
		});
		// Merge in any session variables
		// var.extend(self.vars.into_iter().map(|(k, v)| (k.clone(), v.clone())));
		// Execute the query on the database
		let mut res = kvs.execute(sql, &self.session, var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		// Return the result to the client
		Ok(res)
	}
}
