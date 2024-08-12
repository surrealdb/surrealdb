use std::collections::BTreeMap;

use uuid::Uuid;

use crate::{
	dbs::{capabilities::MethodTarget, QueryType, Response, Session},
	kvs::Datastore,
	rpc::args::Take,
	sql::{Array, Function, Model, Statement, Strand, Value},
};

use super::{method::Method, response::Data, rpc_error::RpcError};

#[allow(async_fn_in_trait)]
pub trait RpcContext {
	fn kvs(&self) -> &Datastore;
	fn session(&self) -> &Session;
	fn session_mut(&mut self) -> &mut Session;
	fn vars(&self) -> &BTreeMap<String, Value>;
	fn vars_mut(&mut self) -> &mut BTreeMap<String, Value>;
	fn version_data(&self) -> impl Into<Data>;

	const LQ_SUPPORT: bool = false;
	fn handle_live(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unreachable!() }
	}
	fn handle_kill(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unreachable!() }
	}

	async fn execute(&mut self, method: Method, params: Array) -> Result<Data, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		if !self.kvs().allows_rpc_method(&MethodTarget {
			method: method.clone(),
		}) {
			return Err(RpcError::MethodNotAllowed);
		}

		match method {
			Method::Ping => Ok(Value::None.into()),
			Method::Info => self.info().await.map(Into::into).map_err(Into::into),
			Method::Use => self.yuse(params).await.map(Into::into).map_err(Into::into),
			Method::Signup => self.signup(params).await.map(Into::into).map_err(Into::into),
			Method::Signin => self.signin(params).await.map(Into::into).map_err(Into::into),
			Method::Invalidate => self.invalidate().await.map(Into::into).map_err(Into::into),
			Method::Authenticate => {
				self.authenticate(params).await.map(Into::into).map_err(Into::into)
			}
			Method::Kill => self.kill(params).await.map(Into::into).map_err(Into::into),
			Method::Live => self.live(params).await.map(Into::into).map_err(Into::into),
			Method::Set => self.set(params).await.map(Into::into).map_err(Into::into),
			Method::Unset => self.unset(params).await.map(Into::into).map_err(Into::into),
			Method::Select => self.select(params).await.map(Into::into).map_err(Into::into),
			Method::Insert => self.insert(params).await.map(Into::into).map_err(Into::into),
			Method::Create => self.create(params).await.map(Into::into).map_err(Into::into),
			Method::Upsert => self.upsert(params).await.map(Into::into).map_err(Into::into),
			Method::Update => self.update(params).await.map(Into::into).map_err(Into::into),
			Method::Merge => self.merge(params).await.map(Into::into).map_err(Into::into),
			Method::Patch => self.patch(params).await.map(Into::into).map_err(Into::into),
			Method::Delete => self.delete(params).await.map(Into::into).map_err(Into::into),
			Method::Version => self.version(params).await.map(Into::into).map_err(Into::into),
			Method::Query => self.query(params).await.map(Into::into).map_err(Into::into),
			Method::Relate => self.relate(params).await.map(Into::into).map_err(Into::into),
			Method::Run => self.run(params).await.map(Into::into).map_err(Into::into),
			Method::Unknown => Err(RpcError::MethodNotFound),
		}
	}

	async fn execute_immut(&self, method: Method, params: Array) -> Result<Data, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		if !self.kvs().allows_rpc_method(&MethodTarget {
			method: method.clone(),
		}) {
			return Err(RpcError::MethodNotAllowed);
		}

		match method {
			Method::Ping => Ok(Value::None.into()),
			Method::Info => self.info().await.map(Into::into).map_err(Into::into),
			Method::Select => self.select(params).await.map(Into::into).map_err(Into::into),
			Method::Insert => self.insert(params).await.map(Into::into).map_err(Into::into),
			Method::Create => self.create(params).await.map(Into::into).map_err(Into::into),
			Method::Upsert => self.upsert(params).await.map(Into::into).map_err(Into::into),
			Method::Update => self.update(params).await.map(Into::into).map_err(Into::into),
			Method::Merge => self.merge(params).await.map(Into::into).map_err(Into::into),
			Method::Patch => self.patch(params).await.map(Into::into).map_err(Into::into),
			Method::Delete => self.delete(params).await.map(Into::into).map_err(Into::into),
			Method::Version => self.version(params).await.map(Into::into).map_err(Into::into),
			Method::Query => self.query(params).await.map(Into::into).map_err(Into::into),
			Method::Relate => self.relate(params).await.map(Into::into).map_err(Into::into),
			Method::Run => self.run(params).await.map(Into::into).map_err(Into::into),
			Method::Unknown => Err(RpcError::MethodNotFound),
			_ => Err(RpcError::MethodNotFound),
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		// For both ns+db, string = change, null = unset, none = do nothing
		// We need to be able to adjust either ns or db without affecting the other
		// To be able to select a namespace, and then list resources in that namespace, as an example
		let (ns, db) = params.needs_two()?;
		let unset_ns = matches!(ns, Value::Null);
		let unset_db = matches!(db, Value::Null);

		// If we unset the namespace, we must also unset the database
		if unset_ns && !unset_db {
			return Err(RpcError::InvalidParams);
		}

		if unset_ns {
			self.session_mut().ns = None;
		} else if let Value::Strand(ns) = ns {
			self.session_mut().ns = Some(ns.0);
		}

		if unset_db {
			self.session_mut().db = None;
		} else if let Value::Strand(db) = db {
			self.session_mut().db = Some(db.0);
		}

		Ok(Value::None)
	}

	async fn signup(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let mut tmp_session = self.session().clone();
		let out: Result<Value, RpcError> =
			crate::iam::signup::signup(self.kvs(), &mut tmp_session, v)
				.await
				.map(Into::into)
				.map_err(Into::into);
		*self.session_mut() = tmp_session;

		out
	}

	async fn signin(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let mut tmp_session = self.session().clone();
		let out: Result<Value, RpcError> =
			crate::iam::signin::signin(self.kvs(), &mut tmp_session, v)
				.await
				.map(Into::into)
				.map_err(Into::into);
		*self.session_mut() = tmp_session;
		out
	}

	async fn invalidate(&mut self) -> Result<impl Into<Data>, RpcError> {
		crate::iam::clear::clear(self.session_mut())?;
		Ok(Value::None)
	}

	async fn authenticate(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Strand(token)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let mut tmp_session = self.session().clone();
		crate::iam::verify::token(self.kvs(), &mut tmp_session, &token.0).await?;
		*self.session_mut() = tmp_session;
		Ok(Value::None)
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<impl Into<Data>, RpcError> {
		// Specify the SQL query string
		let sql = "SELECT * FROM $auth";
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), None).await?;
		// Extract the first value from the result
		let res = res.remove(0).result?.first();
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((Value::Strand(key), val)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the query parameters
		let var = Some(map! {
			key.0.clone() => Value::None,
			=> &self.vars()
		});
		// Compute the specified parameter
		match self.kvs().compute(val, self.session(), var).await? {
			// Remove the variable if undefined
			Value::None => self.vars_mut().remove(&key.0),
			// Store the variable if defined
			v => self.vars_mut().insert(key.0, v),
		};
		Ok(Value::Null)
	}

	async fn unset(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Strand(key)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		self.vars_mut().remove(&key.0);
		Ok(Value::Null)
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let id = params.needs_one()?;
		// Specify the SQL query string
		let sql = "KILL $id";
		// Specify the query parameters
		let var = map! {
			String::from("id") => id,
			=> &self.vars()
		};
		// Execute the query on the database
		// let mut res = self.query_with(Value::from(sql), Object::from(var)).await?;
		let mut res = self.query_inner(Value::from(sql), Some(var)).await?;
		// Extract the first query result
		let response = res.remove(0);
		response.result.map_err(Into::into)
	}

	async fn live(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let (tb, diff) = params.needs_one_or_two()?;
		// Specify the SQL query string
		let sql = match diff.is_true() {
			true => "LIVE SELECT DIFF FROM $tb",
			false => "LIVE SELECT * FROM $tb",
		};
		// Specify the query parameters
		let var = map! {
			String::from("tb") => tb.could_be_table(),
			=> &self.vars()
		};
		// Execute the query on the database
		let mut res = self.query_inner(Value::from(sql), Some(var)).await?;
		// Extract the first query result
		let response = res.remove(0);
		response.result.map_err(Into::into)
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(what) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		// Return a single result?
		let one = what.is_thing();
		// Specify the SQL query string
		let sql = "SELECT * FROM $what";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
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

	async fn insert(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((what, data)) = params.needs_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Return a single result?
		let one = what.is_thing();
		// Specify the SQL query string
		let sql = "INSERT INTO $what $data RETURN AFTER";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
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

	async fn create(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
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
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Return a single result?
		let one = what.is_thing();
		// Specify the SQL query string
		let sql = if data.is_none_or_null() {
			"UPSERT $what RETURN AFTER"
		} else {
			"UPSERT $what CONTENT $data RETURN AFTER"
		};
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			String::from("data") => data,
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
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

	async fn update(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
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
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
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

	async fn merge(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
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
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
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

	async fn patch(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((what, data, diff)) = params.needs_one_two_or_three() else {
			return Err(RpcError::InvalidParams);
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
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((from, kind, to, data)) = params.needs_three_or_four() else {
			return Err(RpcError::InvalidParams);
		};
		// Return a single result?
		let one = kind.is_thing();
		// Specify the SQL query string
		let sql = if data.is_none_or_null() {
			"RELATE $from->$kind->$to"
		} else {
			"RELATE $from->$kind->$to CONTENT $data"
		};
		// Specify the query parameters
		let var = Some(map! {
			String::from("from") => from,
			String::from("kind") => kind.could_be_table(),
			String::from("to") => to,
			String::from("data") => data,
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
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

	async fn delete(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(what) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		// Return a single result?
		let one = what.is_thing();
		// Specify the SQL query string
		let sql = "DELETE $what RETURN BEFORE";
		// Specify the query parameters
		let var = Some(map! {
			String::from("what") => what.could_be_table(),
			=> &self.vars()
		});
		// Execute the query on the database
		let mut res = self.kvs().execute(sql, self.session(), var).await?;
		// Extract the first query result
		let res = match one {
			true => res.remove(0).result?.first(),
			false => res.remove(0).result?,
		};
		// Return the result to the client
		Ok(res)
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		match params.len() {
			0 => Ok(self.version_data()),
			_ => Err(RpcError::InvalidParams),
		}
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((query, o)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		if !(query.is_query() || query.is_strand()) {
			return Err(RpcError::InvalidParams);
		}

		let o = match o {
			Value::Object(v) => Some(v),
			Value::None | Value::Null => None,
			_ => return Err(RpcError::InvalidParams),
		};

		// Specify the query parameters
		let vars = match o {
			Some(mut v) => Some(mrg! {v.0, &self.vars()}),
			None => Some(self.vars().clone()),
		};
		self.query_inner(query, vars).await
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((Value::Strand(Strand(func_name)), version, args)) = params.needs_one_two_or_three()
		else {
			return Err(RpcError::InvalidParams);
		};

		let version = match version {
			Value::Strand(Strand(v)) => Some(v),
			Value::None | Value::Null => None,
			_ => return Err(RpcError::InvalidParams),
		};

		let args = match args {
			Value::Array(Array(arr)) => arr,
			Value::None | Value::Null => vec![],
			_ => return Err(RpcError::InvalidParams),
		};

		let func: Value = match &func_name[0..4] {
			"fn::" => Function::Custom(func_name.chars().skip(4).collect(), args).into(),
			"ml::" => Model {
				name: func_name.chars().skip(4).collect(),
				version: version.ok_or(RpcError::InvalidParams)?,
				args,
			}
			.into(),
			_ => Function::Normal(func_name, args).into(),
		};

		let mut res = self
			.kvs()
			.process(Statement::Value(func).into(), self.session(), Some(self.vars().clone()))
			.await?;
		res.remove(0).result.map_err(Into::into)
	}

	// ------------------------------
	// Private methods
	// ------------------------------

	async fn query_inner(
		&self,
		query: Value,
		vars: Option<BTreeMap<String, Value>>,
	) -> Result<Vec<Response>, RpcError> {
		// If no live query handler force realtime off
		if !Self::LQ_SUPPORT && self.session().rt {
			return Err(RpcError::BadLQConfig);
		}
		// Execute the query on the database
		let res = match query {
			Value::Query(sql) => self.kvs().process(sql, self.session(), vars).await?,
			Value::Strand(sql) => self.kvs().execute(&sql, self.session(), vars).await?,
			_ => unreachable!(),
		};

		// Post-process hooks for web layer
		for response in &res {
			// This error should be unreachable because we shouldn't proceed if there's no handler
			self.handle_live_query_results(response).await;
		}
		// Return the result to the client
		Ok(res)
	}

	async fn handle_live_query_results(&self, res: &Response) {
		match &res.query_type {
			QueryType::Live => {
				if let Ok(Value::Uuid(lqid)) = &res.result {
					self.handle_live(&lqid.0).await;
				}
			}
			QueryType::Kill => {
				if let Ok(Value::Uuid(lqid)) = &res.result {
					self.handle_kill(&lqid.0).await;
				}
			}
			_ => {}
		}
	}
}
