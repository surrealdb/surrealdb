use std::collections::BTreeMap;

use crate::{
	dbs::{QueryType, Response, Session},
	kvs::Datastore,
	rpc::args::Take,
	sql::{Array, Uuid, Value},
};

use super::{method::Method, response::Data, rpc_error::RpcError};

pub struct RpcContext<'a> {
	pub kvs: &'a Datastore,
	pub session: Session,
	pub vars: BTreeMap<String, Value>,
	pub version: String,
	pub lq_handler: Option<Box<dyn LqHandler + Send + Sync>>,
}

pub trait LqHandler {
	fn live(&self, lqid: &Uuid);
	fn kill(&self, lqid: &Uuid);
}

impl<'a> RpcContext<'a> {
	pub fn new(
		kvs: &'a Datastore,
		session: Session,
		vars: BTreeMap<String, Value>,
		lq_handler: Option<Box<dyn LqHandler + Send + Sync>>,
		version: String,
	) -> Self {
		Self {
			kvs,
			session,
			vars,
			lq_handler,
			version,
		}
	}
}

impl<'a> RpcContext<'a> {
	pub async fn execute(&mut self, method: Method, params: Array) -> Result<Data, RpcError> {
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
			Method::Update => self.update(params).await.map(Into::into).map_err(Into::into),
			Method::Merge => self.merge(params).await.map(Into::into).map_err(Into::into),
			Method::Patch => self.patch(params).await.map(Into::into).map_err(Into::into),
			Method::Delete => self.delete(params).await.map(Into::into).map_err(Into::into),
			Method::Version => self.version(params).await.map(Into::into).map_err(Into::into),
			Method::Query => self.query(params).await.map(Into::into).map_err(Into::into),
			Method::Relate => todo!(),
			Method::Unknown => todo!(),
		}
	}
}
macro_rules! mrg {
	($($m:expr, $x:expr)+) => {{
		$($m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)+
		$($m)+
	}};
}
impl<'a> RpcContext<'a> {
	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(&mut self, params: Array) -> Result<Value, RpcError> {
		let (ns, db) = params.needs_two()?;
		if let Value::Strand(ns) = ns {
			self.session.ns = Some(ns.0);
		}
		if let Value::Strand(db) = db {
			self.session.db = Some(db.0);
		}
		Ok(Value::None)
	}

	async fn signup(&mut self, params: Array) -> Result<Value, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		crate::iam::signup::signup(self.kvs, &mut self.session, v)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}

	async fn signin(&mut self, params: Array) -> Result<Value, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		crate::iam::signin::signin(self.kvs, &mut self.session, v)
			.await
			.map(Into::into)
			.map_err(Into::into)
	}

	async fn invalidate(&mut self) -> Result<Value, RpcError> {
		crate::iam::clear::clear(&mut self.session)?;
		Ok(Value::None)
	}

	async fn authenticate(&mut self, params: Array) -> Result<Value, RpcError> {
		let Ok(Value::Strand(token)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		crate::iam::verify::token(self.kvs, &mut self.session, &token.0).await?;
		Ok(Value::None)
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<Value, RpcError> {
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

	async fn set(&mut self, params: Array) -> Result<Value, RpcError> {
		let Ok((Value::Strand(key), val)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
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

	async fn unset(&mut self, params: Array) -> Result<Value, RpcError> {
		let Ok(Value::Strand(key)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		self.vars.remove(&key.0);
		Ok(Value::Null)
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(&mut self, params: Array) -> Result<Value, RpcError> {
		let id = params.needs_one()?;
		// Specify the SQL query string
		let sql = "KILL $id";
		// Specify the query parameters
		let var = map! {
			String::from("id") => id,
			=> &self.vars
		};
		// Execute the query on the database
		// let mut res = self.query_with(Value::from(sql), Object::from(var)).await?;
		let mut res = self.query_inner(Value::from(sql), Some(var)).await?;
		// Extract the first query result
		let response = res.remove(0);
		response.result.map_err(Into::into)
	}

	async fn live(&mut self, params: Array) -> Result<Value, RpcError> {
		let (tb, diff) = params.needs_one_or_two()?;
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
		let mut res = self.query_inner(Value::from(sql), Some(var)).await?;
		// Extract the first query result
		let response = res.remove(0);
		response.result.map_err(Into::into)
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, params: Array) -> Result<Value, RpcError> {
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

	async fn insert(&self, params: Array) -> Result<Value, RpcError> {
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

	async fn create(&self, params: Array) -> Result<Value, RpcError> {
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

	async fn update(&self, params: Array) -> Result<Value, RpcError> {
		let Ok((what, data)) = params.needs_two() else {
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

	async fn merge(&self, params: Array) -> Result<Value, RpcError> {
		let Ok((what, data)) = params.needs_two() else {
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

	async fn patch(&self, params: Array) -> Result<Value, RpcError> {
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

	async fn delete(&self, params: Array) -> Result<Value, RpcError> {
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
	// Methods for getting info
	// ------------------------------

	async fn version(&mut self, params: Array) -> Result<Value, RpcError> {
		match params.len() {
			0 => Ok(self.version.clone().into()),
			_ => Err(RpcError::InvalidParams),
		}
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(&mut self, params: Array) -> Result<Vec<Response>, RpcError> {
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
			Some(mut v) => Some(mrg! {v.0, &self.vars}),
			None => Some(self.vars.clone()),
		};
		self.query_inner(query, vars).await
	}

	// ------------------------------
	// Private methods
	// ------------------------------

	async fn query_inner(
		&mut self,
		query: Value,
		vars: Option<BTreeMap<String, Value>>,
	) -> Result<Vec<Response>, RpcError> {
		// If no live query handler force realtime off
		if self.lq_handler.is_none() && self.session.rt {
			self.session.rt = false;
		}
		// Execute the query on the database
		let res = match query {
			Value::Query(sql) => self.kvs.process(sql, &self.session, vars).await?,
			Value::Strand(sql) => self.kvs.execute(&sql, &self.session, vars).await?,
			_ => unreachable!(),
		};

		// Post-process hooks for web layer
		for response in &res {
			// This error should be unreachable because we shouldn't proceed if there's no handler
			self.handle_live_query_results(response).await;
			info!("response: {response:?}");
		}
		// Return the result to the client
		Ok(res)
	}

	async fn handle_live_query_results(&self, res: &Response) {
		let Some(ref handler) = self.lq_handler else {
			return;
		};
		match &res.query_type {
			QueryType::Live => {
				if let Ok(Value::Uuid(lqid)) = &res.result {
					handler.live(lqid);
				}
			}
			QueryType::Kill => {
				if let Ok(Value::Uuid(lqid)) = &res.result {
					handler.kill(lqid);
				}
			}
			_ => {}
		}
	}
}
