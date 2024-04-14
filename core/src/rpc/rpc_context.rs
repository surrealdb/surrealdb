use std::collections::BTreeMap;

use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::Read;
use crate::{
	dbs::{QueryType, Response, Session},
	kvs::Datastore,
	rpc::args::Take,
	sql::{Array, Function, Model, Statement, Strand, Value},
	syn,
};
use uuid::Uuid;

use super::{method::Method, response::Data, rpc_error::RpcError};

macro_rules! mrg {
	($($m:expr, $x:expr)+) => {{
		$($m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)+
		$($m)+
	}};
}

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
		match method {
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
			m => self.execute_immut(m, params).await,
		}
	}

	async fn execute_immut(&self, method: Method, params: Array) -> Result<Data, RpcError> {
		match method {
			Method::Ping => Ok(Value::None.into()),
			Method::Info => self.info().await.map(Into::into).map_err(Into::into),
			Method::Select => self.select(params).await.map(Into::into).map_err(Into::into),
			Method::Insert => self.insert(params).await.map(Into::into).map_err(Into::into),
			Method::Create => self.create(params).await.map(Into::into).map_err(Into::into),
			Method::Update => self.update(params).await.map(Into::into).map_err(Into::into),
			Method::Merge => self.merge(params).await.map(Into::into).map_err(Into::into),
			Method::Patch => self.patch(params).await.map(Into::into).map_err(Into::into),
			Method::Delete => self.delete(params).await.map(Into::into).map_err(Into::into),
			Method::Version => self.version(params).await.map(Into::into).map_err(Into::into),
			Method::Query => self.query(params).await.map(Into::into).map_err(Into::into),
			Method::Relate => self.relate(params).await.map(Into::into).map_err(Into::into),
			Method::Run => self.run(params).await.map(Into::into).map_err(Into::into),
			Method::_InfoStructure => {
				self.info_structure(params).await.map(Into::into).map_err(Into::into)
			}
			Method::_Validate => self.validate(params).await.map(Into::into).map_err(Into::into),
			Method::Unknown => Err(RpcError::MethodNotFound),
			_ => Err(RpcError::MethodNotFound),
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let (ns, db) = params.needs_two()?;
		if let Value::Strand(ns) = ns {
			self.session_mut().ns = Some(ns.0);
		}
		if let Value::Strand(db) = db {
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
	// Methods for relating
	// ------------------------------

	async fn relate(&self, _params: Array) -> Result<impl Into<Data>, RpcError> {
		let out: Result<Value, RpcError> = Err(RpcError::MethodNotFound);
		out
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
		let out = res.remove(0).result?;

		Ok(out)
	}

	// ------------------------------
	// Private utility methods
	// ------------------------------

	async fn info_structure(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok((Value::Strand(Strand(t)), extra)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		let info_type = InfoType::parse(t, extra)?;
		let mut tx = self.kvs().transaction(Read, Optimistic).await?;
		let ns = self.session().ns.clone();
		let db = self.session().db.clone();

		// TODO(raphaeldarley): fix is_allowed
		let out = match &info_type {
			InfoType::Root => {
				// Allowed to run?
				// opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Create the result set
				let mut res = Object::default();
				// Process the namespaces
				res.insert("namespaces".to_owned(), process_arr(tx.all_ns().await?));
				// Process the users
				res.insert("users".to_owned(), process_arr(tx.all_root_users().await?));
				// Ok all good
				Value::from(res).ok()
			}
			InfoType::Ns => {
				// Allowed to run?
				// opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// get ns
				let Some(ns) = ns else {
					return Err(RpcError::InvalidParams);
				};
				// Create the result set
				let mut res = Object::default();
				// Process the databases
				res.insert("databases".to_owned(), process_arr(tx.all_db(&ns).await?));
				// Process the users
				res.insert("users".to_owned(), process_arr(tx.all_ns_users(&ns).await?));
				// Process the tokens
				res.insert("tokens".to_owned(), process_arr(tx.all_ns_tokens(&ns).await?));
				// Ok all good
				Value::from(res).ok()
			}
			InfoType::Db => {
				// Allowed to run?
				// opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// get ns and db
				let Some(ns) = ns else {
					return Err(RpcError::InvalidParams);
				};
				let Some(db) = db else {
					return Err(RpcError::InvalidParams);
				};
				// Create the result set
				let mut res = Object::default();
				// Process the users
				res.insert("users".to_owned(), process_arr(tx.all_db_users(&ns, &db).await?));
				// Process the tokens
				res.insert("tokens".to_owned(), process_arr(tx.all_db_tokens(&ns, &db).await?));
				// Process the functions
				res.insert(
					"functions".to_owned(),
					process_arr(tx.all_db_functions(&ns, &db).await?),
				);
				// Process the models
				res.insert("models".to_owned(), process_arr(tx.all_db_models(&ns, &db).await?));
				// Process the params
				res.insert("params".to_owned(), process_arr(tx.all_db_params(&ns, &db).await?));
				// Process the scopes
				res.insert("scopes".to_owned(), process_arr(tx.all_sc(&ns, &db).await?));
				// Process the tables
				res.insert("tables".to_owned(), process_arr(tx.all_tb(&ns, &db).await?));
				// Process the analyzers
				res.insert(
					"analyzers".to_owned(),
					process_arr(tx.all_db_analyzers(&ns, &db).await?),
				);
				// Ok all good
				Value::from(res).ok()
			}
			InfoType::Sc(sc) => {
				// Allowed to run?
				// opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// get ns and db
				let Some(ns) = ns else {
					return Err(RpcError::InvalidParams);
				};
				let Some(db) = db else {
					return Err(RpcError::InvalidParams);
				};
				// Create the result set
				let mut res = Object::default();
				// Process the tokens
				res.insert("tokens".to_owned(), process_arr(tx.all_sc_tokens(&ns, &db, sc).await?));
				// Ok all good
				Value::from(res).ok()
			}
			InfoType::Tb(tb) => {
				// Allowed to run?
				// opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// get ns and db
				let Some(ns) = ns else {
					return Err(RpcError::InvalidParams);
				};
				let Some(db) = db else {
					return Err(RpcError::InvalidParams);
				};
				// Create the result set
				let mut res = Object::default();
				// Process the events
				res.insert("events".to_owned(), process_arr(tx.all_tb_events(&ns, &db, tb).await?));
				// Process the fields
				res.insert("fields".to_owned(), process_arr(tx.all_tb_fields(&ns, &db, tb).await?));
				// Process the tables
				res.insert("tables".to_owned(), process_arr(tx.all_tb_views(&ns, &db, tb).await?));
				// Process the indexes
				res.insert(
					"indexes".to_owned(),
					process_arr(tx.all_tb_indexes(&ns, &db, tb).await?),
				);
				// Process the live queries
				res.insert("lives".to_owned(), process_arr(tx.all_tb_lives(&ns, &db, tb).await?));
				// Ok all good
				Value::from(res).ok()
			}
		};

		// let out: Result<Value, RpcError> = Err(RpcError::MethodNotFound);
		out.map_err(Into::into)
	}

	async fn validate(&self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Strand(Strand(query))) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let q = syn::parse(&query)?;
		Ok(Value::Query(q))
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

enum InfoType {
	Root,
	Ns,
	Db,
	Sc(Ident),
	Tb(Ident),
}

impl InfoType {
	fn parse(text: impl AsRef<str>, extra: Value) -> Result<InfoType, RpcError> {
		match (text.as_ref(), extra) {
			("root", Value::None) => Ok(InfoType::Root),
			("ns", Value::None) => Ok(InfoType::Ns),
			("namespace", Value::None) => Ok(InfoType::Ns),
			("db", Value::None) => Ok(InfoType::Db),
			("database", Value::None) => Ok(InfoType::Db),
			("sc", Value::Strand(sc)) => Ok(InfoType::Sc(Ident(sc.0))),
			("scope", Value::Strand(sc)) => Ok(InfoType::Sc(Ident(sc.0))),
			("tb", Value::Strand(tb)) => Ok(InfoType::Tb(Ident(tb.0))),
			("table", Value::Strand(tb)) => Ok(InfoType::Tb(Ident(tb.0))),
			_ => Err(RpcError::InvalidParams),
		}
	}
}

use crate::sql::{Ident, Object};
use std::sync::Arc;

pub(crate) trait InfoStructure {
	fn structure(self) -> Value;
}

fn process_arr<T>(a: Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}
