use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERSION;
use crate::dbs::DB;
use crate::err::Error;
use crate::rpc::args::Take;
use crate::rpc::LIVE_QUERIES;
use std::collections::BTreeMap;

use surrealdb::dbs::QueryType;
use surrealdb::dbs::Response;
use surrealdb::sql::Object;
use surrealdb::sql::Strand;
use surrealdb::sql::Value;
use surrealdb::{dbs::Session, sql::Array};
use uuid::Uuid;

use super::res::{Data, Failure, OutputFormat};

pub struct Processor {
	pub ws_id: Uuid,
	session: Session,
	pub format: OutputFormat,
	vars: BTreeMap<String, Value>,
}

impl Processor {
	pub fn new(session: Session, format: OutputFormat, vars: BTreeMap<String, Value>) -> Self {
		Self {
			ws_id: Uuid::new_v4(),
			session,
			format,
			vars,
		}
	}

	pub async fn process_request(&mut self, method: &str, params: Array) -> Result<Data, Failure> {
		debug!("Process RPC request");

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
				0 => self.info().await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Switch to a specific namespace and database
			"use" => match params.needs_two() {
				Ok((ns, db)) => self.yuse(ns, db).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Signup to a specific authentication scope
			"signup" => match params.needs_one() {
				Ok(Value::Object(v)) => self.signup(v).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Signin as a root, namespace, database or scope user
			"signin" => match params.needs_one() {
				Ok(Value::Object(v)) => self.signin(v).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Invalidate the current authentication session
			"invalidate" => match params.len() {
				0 => self.invalidate().await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Authenticate using an authentication token
			"authenticate" => match params.needs_one() {
				Ok(Value::Strand(v)) => {
					self.authenticate(v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Kill a live query using a query id
			"kill" => match params.needs_one() {
				Ok(v) => self.kill(v).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Setup a live query on a specific table
			"live" => match params.needs_one_or_two() {
				Ok((v, d)) if v.is_table() => {
					self.live(v, d).await.map(Into::into).map_err(Into::into)
				}
				Ok((v, d)) if v.is_strand() => {
					self.live(v, d).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Specify a connection-wide parameter
			"let" | "set" => match params.needs_one_or_two() {
				Ok((Value::Strand(s), v)) => {
					self.set(s, v).await.map(Into::into).map_err(Into::into)
				}
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Unset and clear a connection-wide parameter
			"unset" => match params.needs_one() {
				Ok(Value::Strand(s)) => self.unset(s).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Select a value or values from the database
			"select" => match params.needs_one() {
				Ok(v) => self.select(v).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Insert a value or values in the database
			"insert" => match params.needs_one_or_two() {
				Ok((v, o)) => self.insert(v, o).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Create a value or values in the database
			"create" => match params.needs_one_or_two() {
				Ok((v, o)) => self.create(v, o).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Update a value or values in the database using `CONTENT`
			"update" => match params.needs_one_or_two() {
				Ok((v, o)) => self.update(v, o).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Update a value or values in the database using `MERGE`
			"merge" => match params.needs_one_or_two() {
				Ok((v, o)) => self.merge(v, o).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Update a value or values in the database using `PATCH`
			"patch" => match params.needs_one_two_or_three() {
				Ok((v, o, d)) => self.patch(v, o, d).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Delete a value or values from the database
			"delete" => match params.needs_one() {
				Ok(v) => self.delete(v).await.map(Into::into).map_err(Into::into),
				_ => Err(Failure::INVALID_PARAMS),
			},
			// Specify the output format for text requests
			"format" => match params.needs_one() {
				Ok(Value::Strand(v)) => self.format(v).await.map(Into::into).map_err(Into::into),
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
					self.query(v).await.map(Into::into).map_err(Into::into)
				}
				Ok((v, Value::Object(o))) if v.is_strand() || v.is_query() => {
					self.query_with(v, o).await.map(Into::into).map_err(Into::into)
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
