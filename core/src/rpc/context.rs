use std::collections::BTreeMap;

use crate::{
	dbs::{Response, Session},
	err::Error,
	kvs::Datastore,
	rpc::args::Take,
	sql::{param, Array, Object, Value},
};

use super::{method::Method, response::Data};

pub struct RpcContext<'a> {
	pub vars: BTreeMap<String, Value>,
	pub session: Session,
	pub kvs: &'a Datastore,
	pub lq_handler: Option<()>,
}

impl<'a> RpcContext<'a> {
	pub async fn execute(&mut self, method: Method, params: Array) -> Result<Value, Error> {
		match method {
			Method::Ping => Ok(Value::None.into()),
			Method::Info => self.info().await,
			Method::Use => self.yuse(params).await,
			Method::Signup => self.signup(params).await,
			Method::Signin => self.signin(params).await,
			Method::Invalidate => self.invalidate().await,
			Method::Authenticate => self.authenticate(params).await,
			Method::Kill => todo!(),
			Method::Live => todo!(),
			Method::Set => self.set(params).await,
			Method::Unset => self.unset(params).await,
			Method::Select => self.select(params).await,
			Method::Insert => self.insert(params).await,
			Method::Create => self.create(params).await,
			Method::Update => self.update(params).await,
			Method::Merge => self.merge(params).await,
			Method::Patch => self.patch(params).await,
			Method::Delete => self.delete(params).await,
			Method::Version => todo!(),
			Method::Query => todo!(),
			Method::Relate => todo!(),
			Method::Unknown => todo!(),
		}
		.map(Into::into)
	}
}

impl<'a> RpcContext<'a> {
	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(&mut self, params: Array) -> Result<Value, Error> {
		let (ns, db) = params.needs_two().or(Err(Error::Thrown("Invalid Params".to_string())))?;
		if let Value::Strand(ns) = ns {
			self.session.ns = Some(ns.0);
		}
		if let Value::Strand(db) = db {
			self.session.db = Some(db.0);
		}
		Ok(Value::None)
	}

	async fn signup(&mut self, params: Array) -> Result<Value, Error> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		crate::iam::signup::signup(self.kvs, &mut self.session, v)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}

	async fn signin(&mut self, params: Array) -> Result<Value, Error> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		crate::iam::signin::signin(self.kvs, &mut self.session, v)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}

	async fn invalidate(&mut self) -> Result<Value, Error> {
		crate::iam::clear::clear(&mut self.session)?;
		Ok(Value::None)
	}

	async fn authenticate(&mut self, params: Array) -> Result<Value, Error> {
		let Ok(Value::Strand(token)) = params.needs_one() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		crate::iam::verify::token(self.kvs, &mut self.session, &token.0).await?;
		Ok(Value::None)
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<Value, Error> {
		// Specify the SQL query string
		let sql = "SELECT * FROM $auth";
		// Execute the query on the database
		let mut res = self.kvs.execute(sql, &self.session, None).await?;
		// Extract the first value from the result
		let res = res.remove(0).result?.first();
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(&mut self, params: Array) -> Result<Value, Error> {
		let Ok((Value::Strand(key), val)) = params.needs_one_or_two() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Specify the query parameters
		let var = Some(map! {
			key.0.clone() => Value::None,
			=> &self.vars
		});
		// Compute the specified parameter
		match self.kvs.compute(val, &self.session, var).await? {
			// Remove the variable if undefined
			Value::None => self.vars.remove(&key.0),
			// Store the variable if defined
			v => self.vars.insert(key.0, v),
		};
		Ok(Value::Null)
	}

	async fn unset(&mut self, params: Array) -> Result<Value, Error> {
		let Ok(Value::Strand(key)) = params.needs_one() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		self.vars.remove(&key.0);
		Ok(Value::Null)
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	// async fn kill(&self, id: Value) -> Result<Value, Error> {
	// 	// Specify the SQL query string
	// 	let sql = "KILL $id";
	// 	// Specify the query parameters
	// 	let var = map! {
	// 		String::from("id") => id,
	// 		=> &self.vars
	// 	};
	// 	// Execute the query on the database
	// 	let mut res = self.query_with(Value::from(sql), Object::from(var)).await?;
	// 	// Extract the first query result
	// 	let response = res.remove(0);
	// 	match response.result {
	// 		Ok(v) => Ok(v),
	// 		Err(e) => Err(Error::from(e)),
	// 	}
	// }

	// async fn live(&self, tb: Value, diff: Value) -> Result<Value, Error> {
	// 	// Specify the SQL query string
	// 	let sql = match diff.is_true() {
	// 		true => "LIVE SELECT DIFF FROM $tb",
	// 		false => "LIVE SELECT * FROM $tb",
	// 	};
	// 	// Specify the query parameters
	// 	let var = map! {
	// 		String::from("tb") => tb.could_be_table(),
	// 		=> &self.vars
	// 	};
	// 	// Execute the query on the database
	// 	let mut res = self.query_with(Value::from(sql), Object::from(var)).await?;
	// 	// Extract the first query result
	// 	let response = res.remove(0);
	// 	match response.result {
	// 		Ok(v) => Ok(v),
	// 		Err(e) => Err(Error::from(e)),
	// 	}
	// }

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, params: Array) -> Result<Value, Error> {
		let Ok(what) = params.needs_one() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Return a single result?
		let one = what.is_thing();
		// Specify the SQL query string
		let sql = "SELECT * FROM $what";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = self.kvs.execute(sql, &self.session, var).await?;
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

	async fn insert(&self, params: Array) -> Result<Value, Error> {
		let Ok((what, data)) = params.needs_two() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Return a single result?
		let one = what.is_thing();
		// Specify the SQL query string
		let sql = "INSERT INTO $what $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = self.kvs.execute(sql, &self.session, var).await?;
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

	async fn create(&self, params: Array) -> Result<Value, Error> {
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Return a single result?
		let one = what.is_thing();
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
		let mut res = self.kvs.execute(sql, &self.session, var).await?;
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

	async fn update(&self, params: Array) -> Result<Value, Error> {
		let Ok((what, data)) = params.needs_two() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Return a single result?
		let one = what.is_thing();
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
		let mut res = self.kvs.execute(sql, &self.session, var).await?;
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

	async fn merge(&self, params: Array) -> Result<Value, Error> {
		let Ok((what, data)) = params.needs_two() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Return a single result?
		let one = what.is_thing();
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
		let mut res = self.kvs.execute(sql, &self.session, var).await?;
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

	async fn patch(&self, params: Array) -> Result<Value, Error> {
		let Ok((what, data, diff)) = params.needs_one_two_or_three() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Return a single result?
		let one = what.is_thing();
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
		let mut res = self.kvs.execute(sql, &self.session, var).await?;
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

	async fn delete(&self, params: Array) -> Result<Value, Error> {
		let Ok(what) = params.needs_one() else {
			return Err(Error::Thrown("Invalid Params".to_string()));
		};
		// Return a single result?
		let one = what.is_thing();
		// Specify the SQL query string
		let sql = "DELETE $what RETURN BEFORE";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars
		});
		// Execute the query on the database
		let mut res = self.kvs.execute(sql, &self.session, var).await?;
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

	// async fn query(&self, sql: Value) -> Result<Vec<Response>, Error> {
	// 	// Get a database reference
	// 	let kvs = DB.get().unwrap();
	// 	// Specify the query parameters
	// 	let var = Some(self.vars.clone());
	// 	// Execute the query on the database
	// 	let res = match sql {
	// 		Value::Query(sql) => kvs.process(sql, &self.session, var).await?,
	// 		Value::Strand(sql) => kvs.execute(&sql, &self.session, var).await?,
	// 		_ => unreachable!(),
	// 	};

	// 	// Post-process hooks for web layer
	// 	for response in &res {
	// 		self.handle_live_query_results(response).await;
	// 	}
	// 	// Return the result to the client
	// 	Ok(res)
	// }

	// async fn query_with(&self, sql: Value, mut vars: Object) -> Result<Vec<Response>, Error> {
	// 	// Get a database reference
	// 	let kvs = DB.get().unwrap();
	// 	// Specify the query parameters
	// 	let var = Some(mrg! { vars.0, &self.vars });
	// 	// Execute the query on the database
	// 	let res = match sql {
	// 		Value::Query(sql) => kvs.process(sql, &self.session, var).await?,
	// 		Value::Strand(sql) => kvs.execute(&sql, &self.session, var).await?,
	// 		_ => unreachable!(),
	// 	};
	// 	// Post-process hooks for web layer
	// 	for response in &res {
	// 		self.handle_live_query_results(response).await;
	// 	}
	// 	// Return the result to the client
	// 	Ok(res)
	// }

	// ------------------------------
	// Private methods
	// ------------------------------

	// async fn handle_live_query_results(&self, res: &Response) {
	// 	match &res.query_type {
	// 		QueryType::Live => {
	// 			if let Ok(Value::Uuid(lqid)) = &res.result {
	// 				// Match on Uuid type
	// 				LIVE_QUERIES.write().await.insert(lqid.0, self.id);
	// 				trace!("Registered live query {} on websocket {}", lqid, self.id);
	// 			}
	// 		}
	// 		QueryType::Kill => {
	// 			if let Ok(Value::Uuid(lqid)) = &res.result {
	// 				if let Some(id) = LIVE_QUERIES.write().await.remove(&lqid.0) {
	// 					trace!("Unregistered live query {} on websocket {}", lqid, id);
	// 				}
	// 			}
	// 		}
	// 		_ => {}
	// 	}
	// }
}
