use std::mem;
use std::sync::Arc;

use anyhow::{Result, ensure};

#[cfg(not(target_family = "wasm"))]
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::capabilities::MethodTarget;
use crate::dbs::{QueryType, Response, Variables};
use crate::err::Error;
use crate::kvs::{LockType, TransactionType};
use crate::rpc::args::extract_args;
use crate::rpc::{Data, Method, RpcContext, RpcError};
use crate::sql::{
	Ast, CreateStatement, Data as SqlData, DeleteStatement, Expr, Fields, Function, FunctionCall,
	Ident, InsertStatement, KillStatement, LiveStatement, Model, Output, Param, RelateStatement,
	SelectStatement, TopLevelExpr, UpdateStatement, UpsertStatement,
};
use crate::val::{Array, Object, RecordIdKey, Strand, Value};

/// utility function converting a `Value::Strand` into a `Expr::Table`
fn value_to_table(value: Value) -> Expr {
	match value {
		Value::Strand(s) => Expr::Table(Ident::from_strand(s)),
		x => x.into_literal().into(),
	}
}

/// returns if the expression returns a singular value when selected.
///
/// As this rpc is some what convuluted the singular conditions is not the same
/// for all cases.
fn singular(value: &Value) -> bool {
	match value {
		Value::Object(_) => true,
		Value::RecordId(t) => !matches!(t.key, RecordIdKey::Range(_)),
		_ => false,
	}
}

#[expect(async_fn_in_trait)]
pub trait RpcProtocolV1: RpcContext {
	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation
	async fn execute(&self, method: Method, params: Array) -> Result<Data, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		if !self.kvs().allows_rpc_method(&MethodTarget {
			method,
		}) {
			warn!("Capabilities denied RPC method call attempt, target: '{method}'");
			return Err(RpcError::MethodNotAllowed);
		}
		// Execute the desired method
		match method {
			Method::Ping => Ok(Data::Other(Value::None)),
			Method::Info => self.info().await,
			Method::Use => self.yuse(params).await,
			Method::Signup => self.signup(params).await,
			Method::Signin => self.signin(params).await,
			Method::Authenticate => self.authenticate(params).await,
			Method::Invalidate => self.invalidate().await,
			Method::Reset => self.reset().await,
			Method::Kill => self.kill(params).await,
			Method::Live => self.live(params).await,
			Method::Set => self.set(params).await,
			Method::Unset => self.unset(params).await,
			Method::Select => self.select(params).await,
			Method::Insert => self.insert(params).await,
			Method::Create => self.create(params).await,
			Method::Upsert => self.upsert(params).await,
			Method::Update => self.update(params).await,
			Method::Merge => self.merge(params).await,
			Method::Patch => self.patch(params).await,
			Method::Delete => self.delete(params).await,
			Method::Version => self.version(params).await,
			Method::Query => self.query(params).await,
			Method::Relate => self.relate(params).await,
			Method::Run => self.run(params).await,
			Method::GraphQL => self.graphql(params).await,
			Method::InsertRelation => self.insert_relation(params).await,
			_ => Err(RpcError::MethodNotFound),
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// For both ns+db, string = change, null = unset, none = do nothing
		// We need to be able to adjust either ns or db without affecting the other
		// To be able to select a namespace, and then list resources in that namespace,
		// as an example
		let (ns, db) = extract_args::<(Value, Value)>(params.0.clone())
			.ok_or(RpcError::InvalidParams("Expected (ns, db)".to_string()))?;
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().as_ref().clone();
		// Update the selected namespace
		match ns {
			Value::None => (),
			Value::Null => session.ns = None,
			Value::Strand(ns) => {
				let tx =
					self.kvs().transaction(TransactionType::Write, LockType::Optimistic).await?;
				tx.get_or_add_ns(&ns, self.kvs().is_strict_mode()).await?;
				tx.commit().await?;

				session.ns = Some(ns.into_string())
			}
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected ns to be string, got {unexpected:?}"
				)));
			}
		}
		// Update the selected database
		match db {
			Value::None => (),
			Value::Null => session.db = None,
			Value::Strand(db) => {
				let ns = session.ns.clone().unwrap();
				let tx =
					self.kvs().transaction(TransactionType::Write, LockType::Optimistic).await?;
				tx.ensure_ns_db(&ns, &db, self.kvs().is_strict_mode()).await?;
				tx.commit().await?;
				session.db = Some(db.into_string())
			}
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected db to be string, got {unexpected:?}"
				)));
			}
		}
		// Clear any residual database
		if self.session().ns.is_none() && self.session().db.is_some() {
			session.db = None;
		}
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return nothing
		Ok(Data::Other(Value::None))
	}

	// TODO(gguillemas): Update this method in 3.0.0 to return an object instead of
	// a string. This will allow returning refresh tokens as well as any additional
	// credential resulting from signing up.
	async fn signup(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Some(Value::Object(params)) = extract_args(params.0) else {
			return Err(RpcError::InvalidParams("Expected (params:object)".to_string()));
		};
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().clone().as_ref().clone();
		// Attempt signup, mutating the session
		let out: Result<Value> = crate::iam::signup::signup(self.kvs(), &mut session, params)
			.await
			// TODO: Null byte validity
			.map(|v| {
				v.token
					.clone()
					.map(|x| Value::Strand(Strand::new(x).unwrap()))
					.unwrap_or(Value::None)
			});

		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return the signup result
		out.map(Data::Other).map_err(Into::into)
	}

	// TODO(gguillemas): Update this method in 3.0.0 to return an object instead of
	// a string. This will allow returning refresh tokens as well as any additional
	// credential resulting from signing in.
	async fn signin(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Some(Value::Object(params)) = extract_args(params.0) else {
			return Err(RpcError::InvalidParams("Expected (params:object)".to_string()));
		};
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().clone().as_ref().clone();
		// Attempt signin, mutating the session
		let out: Result<Value> = crate::iam::signin::signin(self.kvs(), &mut session, params)
			.await
			// TODO: Null byte validity
			.map(|v| Strand::new(v.token.clone()).unwrap().into());
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return the signin result
		out.map(Data::Other).map_err(From::from)
	}

	async fn authenticate(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Some(Value::Strand(token)) = extract_args(params.0) else {
			return Err(RpcError::InvalidParams("Expected (token:string)".to_string()));
		};
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().as_ref().clone();
		// Attempt authentication, mutating the session
		let out: Result<Value> =
			crate::iam::verify::token(self.kvs(), &mut session, token.as_str())
				.await
				.map(|_| Value::None);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return nothing on success
		out.map(Data::Other).map_err(From::from)
	}

	async fn invalidate(&self) -> Result<Data, RpcError> {
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().as_ref().clone();
		// Clear the current session
		crate::iam::clear::clear(&mut session)?;
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return nothing on success
		Ok(Data::Other(Value::None))
	}

	async fn reset(&self) -> Result<Data, RpcError> {
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().as_ref().clone();
		// Reset the current session
		crate::iam::reset::reset(&mut session);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Cleanup live queries
		self.cleanup_lqs().await;
		// Return nothing on success
		Ok(Data::Other(Value::None))
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<Data, RpcError> {
		let what = vec![Expr::Param(Param::from_strand(strand!("auth").to_owned()))];

		// TODO: Check if this can be replaced by just evaluating the param or a
		// `$auth.*` expression
		// Specify the SQL query string
		let sql = SelectStatement {
			expr: Fields::all(),
			what,
			with: None,
			cond: None,
			omit: None,
			only: false,
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
			fetch: None,
			version: None,
			timeout: None,
			parallel: false,
			explain: None,
			tempfiles: false,
		};
		let ast = Ast::single_expr(Expr::Select(Box::new(sql)));
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), None).await?;
		// Extract the first value from the result
		// TODO: Move first here into the actual expression.
		Ok(Data::Other(res.remove(0).result?.first()))
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let Some((Value::Strand(key), val)) = extract_args::<(Value, Option<Value>)>(params.0)
		else {
			return Err(RpcError::InvalidParams("Expected (key:string, value:Value)".to_string()));
		};
		// TODO(3.0.0): The value inversion PR has removed the ability to set a value
		// from an expression.
		// Maybe reintroduce somehow.

		let mutex = self.lock();
		let guard = mutex.acquire().await.unwrap();
		let mut session = self.session().as_ref().clone();

		if session.expired() {
			return Err(anyhow::Error::new(Error::ExpiredSession).into());
		}

		match val {
			None | Some(Value::None) => session.variables.remove(key.as_str()),
			Some(val) => {
				crate::rpc::check_protected_param(&key)?;
				session.variables.insert(key.into_string(), val)
			}
		}
		self.set_session(Arc::new(session));

		mem::drop(guard);

		// Return nothing
		Ok(Data::Other(Value::Null))
	}

	async fn unset(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let Some(Value::Strand(key)) = extract_args(params.0) else {
			return Err(RpcError::InvalidParams("Expected (key)".to_string()));
		};

		// Get the context lock
		let mutex = self.lock().clone();
		let guard = mutex.acquire().await;
		let mut session = self.session().as_ref().clone();
		session.variables.remove(key.as_str());
		self.set_session(Arc::new(session));
		mem::drop(guard);

		Ok(Data::Other(Value::Null))
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (id,) = extract_args::<(Value,)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (id)".to_string()))?;
		// Specify the SQL query string
		let ast = Ast {
			expressions: vec![TopLevelExpr::Kill(KillStatement {
				id: id.into_literal().into(),
			})],
		};
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, QueryForm::Parsed(ast), var).await?;
		// Extract the first query result
		Ok(Data::Other(res.remove(0).result?))
	}

	async fn live(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, diff) = extract_args::<(Value, Option<Value>)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (what, diff)".to_string()))?;

		// If value is a strand, handle it as if it was a table.
		let what = match what {
			Value::Strand(x) => Expr::Table(Ident::from_strand(x)),
			x => x.into_literal().into(),
		};

		// Specify the SQL query string
		let sql = LiveStatement {
			fields: if diff.unwrap_or(Value::None).is_true() {
				Fields::none()
			} else {
				Fields::all()
			},
			what,
			cond: None,
			fetch: None,
		};
		let ast = Ast {
			expressions: vec![TopLevelExpr::Live(Box::new(sql))],
		};
		// Specify the query parameters
		let vars = Some(self.session().variables.clone());

		let res = run_query(self, QueryForm::Parsed(ast), vars).await?;

		// Extract the first query result
		Ok(Data::Other(res.into_iter().next().unwrap().result?))
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what,) = extract_args::<(Value,)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (what:Value)".to_string()))?;

		// If the what is a single record with a non range value, make it return only a
		// single result.
		let only = match what {
			Value::RecordId(ref x) => !x.key.is_range(),
			_ => false,
		};

		// If value is a strand, handle it as if it was a table.
		let what = match what {
			Value::Strand(x) => Expr::Table(Ident::from_strand(x)),
			x => x.into_literal().into(),
		};

		// Specify the SQL query string
		let sql = SelectStatement {
			only,
			expr: Fields::all(),
			what: vec![what],
			with: None,
			cond: None,
			omit: None,
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
			fetch: None,
			version: None,
			timeout: None,
			parallel: false,
			explain: None,
			tempfiles: false,
		};
		let ast = Ast::single_expr(Expr::Select(Box::new(sql)));

		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(Value, Value)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let into = match what {
			Value::Strand(x) => Some(Expr::Table(Ident::from_strand(x))),
			x => {
				if x.is_nullish() {
					None
				} else {
					Some(x.into_literal().into())
				}
			}
		};

		// Specify the SQL query string
		let sql = InsertStatement {
			into,
			data: SqlData::SingleExpression(data.into_literal().into()),
			output: Some(Output::After),
			..Default::default()
		};
		let ast = Ast::single_expr(Expr::Insert(Box::new(sql)));
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	async fn insert_relation(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(Value, Value)>(params.0.clone())
			.ok_or(RpcError::InvalidParams("Expected (what, data)".to_string()))?;

		let what = match what {
			Value::Null | Value::None => None,
			Value::Strand(x) => Some(Expr::Table(Ident::from_strand(x))),
			x => Some(x.into_literal().into()),
		};

		let data = SqlData::SingleExpression(data.into_literal().into());

		// Specify the SQL query string
		let sql = InsertStatement {
			relation: true,
			into: what,
			data,
			output: Some(Output::After),
			ignore: false,
			update: None,
			timeout: None,
			parallel: false,
			version: None,
		};
		let ast = Ast::single_expr(Expr::Insert(Box::new(sql)));
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(Value, Option<Value>)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let only = match what {
			Value::Strand(_) | Value::Table(_) => true,
			Value::RecordId(ref x) => !matches!(x.key, RecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(x.into_literal().into()));

		// Specify the SQL query string
		let sql = CreateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			timeout: None,
			parallel: false,
			version: None,
		};
		let ast = Ast::single_expr(Expr::Create(Box::new(sql)));
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), None).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(Value, Option<Value>)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let only = match what {
			Value::RecordId(ref x) => !matches!(x.key, RecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(x.into_literal().into()));

		// Specify the SQL query string
		let sql = UpsertStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			with: None,
			cond: None,
			timeout: None,
			parallel: false,
			explain: None,
		};
		let ast = Ast::single_expr(Expr::Upsert(Box::new(sql)));
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(Value, Option<Value>)>(params.0.clone())
			.ok_or(RpcError::InvalidParams("Expected (what, data)".to_string()))?;

		let only = match what {
			Value::RecordId(ref x) => !matches!(x.key, RecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(x.into_literal().into()));
		// Specify the SQL query string
		let sql = UpdateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			with: None,
			cond: None,
			timeout: None,
			parallel: false,
			explain: None,
		};
		let ast = Ast::single_expr(Expr::Update(Box::new(sql)));
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for merging
	// ------------------------------

	async fn merge(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(Value, Option<Value>)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let only = match what {
			Value::RecordId(ref x) => !matches!(x.key, RecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::MergeExpression(x.into_literal().into()));
		// Specify the SQL query string
		let sql = UpdateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			..Default::default()
		};
		let ast = Ast::single_expr(Expr::Update(Box::new(sql)));
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for patching
	// ------------------------------

	async fn patch(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data, diff) = extract_args::<(Value, Option<Value>, Option<Value>)>(params.0)
			.ok_or(RpcError::InvalidParams(
				"Expected (what:Value, data:Value, diff:Value)".to_string(),
			))?;

		// Process the method arguments
		let only = match what {
			Value::RecordId(ref x) => !matches!(x.key, RecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::PatchExpression(x.into_literal().into()));

		let diff = matches!(diff, Some(Value::Bool(true)));

		// Specify the SQL query string
		let expr = Expr::Update(Box::new(UpdateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: if diff {
				Some(Output::Diff)
			} else {
				Some(Output::After)
			},
			with: None,
			cond: None,
			timeout: None,
			parallel: false,
			explain: None,
		}));
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(Ast::single_expr(expr), &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (from, kind, with, data) = extract_args::<(Value, Value, Value, Option<Value>)>(
			params.0,
		)
		.ok_or(RpcError::InvalidParams(
			"Expected (from:Value, kind:Value, with:Value, data:Value)".to_string(),
		))?;

		// Returns if selecting on this value returns a single result.
		let only = singular(&from) && singular(&with);

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(x.into_literal().into()));

		// Specify the SQL query string
		let expr = Expr::Relate(Box::new(RelateStatement {
			only,
			from: from.into_literal().into(),
			through: value_to_table(kind),
			to: with.into_literal().into(),
			data,
			output: Some(Output::After),
			uniq: false,
			timeout: None,
			parallel: false,
		}));
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(Ast::single_expr(expr), &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what,) = extract_args::<(Value,)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (what:Value)".to_string()))?;
		// Specify the SQL query string
		let sql = Expr::Delete(Box::new(DeleteStatement {
			only: singular(&what),
			what: vec![value_to_table(what)],
			output: Some(Output::Before),
			with: None,
			cond: None,
			timeout: None,
			parallel: false,
			explain: None,
		}));
		let ast = Ast::single_expr(sql);
		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result.or_else(|e| match e.downcast_ref() {
			Some(Error::SingleOnlyOutput) => Ok(Value::None),
			_ => Err(e),
		})?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(&self, params: Array) -> Result<Data, RpcError> {
		match params.len() {
			0 => Ok(self.version_data()),
			_ => Err(RpcError::InvalidParams("Expected 0 arguments".to_string())),
		}
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (query, vars) = extract_args::<(Value, Option<Value>)>(params.0)
			.ok_or(RpcError::InvalidParams("Expected (query:string, vars:object)".to_string()))?;

		let Value::Strand(query) = query else {
			return Err(RpcError::InvalidParams("Expected query to be string".to_string()));
		};

		// Specify the query variables
		let vars = match vars {
			Some(Value::Object(v)) => {
				let v: Object = v;
				Some(self.session().variables.merged(v))
			}
			None | Some(Value::None | Value::Null) => Some(self.session().variables.clone()),
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected vars to be object, got {unexpected:?}"
				)));
			}
		};

		let res = run_query(self, QueryForm::Text(&query), vars).await?;
		Ok(Data::Query(res))
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(&self, params: Array) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (name, version, args) = extract_args::<(Value, Option<Value>, Option<Value>)>(params.0)
			.ok_or(RpcError::InvalidParams(
				"Expected (name:string, version:string, args:array)".to_string(),
			))?;
		// Parse the function name argument
		let name = match name {
			Value::Strand(v) => v.into_string(),
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected name to be string, got {unexpected:?}"
				)));
			}
		};
		// Parse any function version argument
		let version = match version {
			Some(Value::Strand(v)) => Some(v.into_string()),
			None | Some(Value::None | Value::Null) => None,
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected version to be string, got {unexpected:?}"
				)));
			}
		};
		// Parse the function arguments if specified
		let args = match args {
			Some(Value::Array(Array(args))) => {
				args.into_iter().map(|x| x.into_literal().into()).collect::<Vec<Expr>>()
			}
			None | Some(Value::None | Value::Null) => vec![],
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected args to be array, got {unexpected:?}"
				)));
			}
		};

		let name = if let Some(rest) = name.strip_prefix("fn::") {
			Function::Custom(rest.to_owned())
		} else if let Some(rest) = name.strip_prefix("ml::") {
			let name = rest.to_owned();
			Function::Model(Model {
				name,
				version: version.ok_or(RpcError::InvalidParams(
					"Expected version to be set for model function".to_string(),
				))?,
			})
		} else {
			Function::Normal(name)
		};

		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: name,
			arguments: args,
		}));
		let ast = Ast::single_expr(expr);

		// Specify the query parameters
		let var = Some(self.session().variables.clone());
		// Execute the function on the database
		let mut res = self.kvs().process(ast, &self.session(), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(Data::Other(res))
	}

	// ------------------------------
	// Methods for querying with GraphQL
	// ------------------------------

	#[cfg(target_family = "wasm")]
	async fn graphql(&self, _: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	#[cfg(not(target_family = "wasm"))]
	async fn graphql(&self, _params: Array) -> Result<Data, RpcError> {
		//use crate::gql;

		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		if !self.kvs().get_capabilities().allows_experimental(&ExperimentalTarget::GraphQL) {
			return Err(RpcError::BadGQLConfig);
		}

		// TODO(3.0.0): Reimplement GraphQL.
		Err(RpcError::from(anyhow::Error::new(Error::Unimplemented("graphql".to_owned()))))

		/*
		if !Self::GQL_SUPPORT {
			return Err(RpcError::BadGQLConfig);
		}

		let Ok((query, options)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0)));
		};

		enum GraphQLFormat {
			Json,
		}

		// Default to compressed output
		let mut pretty = false;
		// Default to graphql json format
		let mut format = GraphQLFormat::Json;
		// Process any secondary config options
		match options {
			// A config object was passed
			SqlValue::Object(o) => {
				for (k, v) in o {
					match (k.as_str(), v) {
						("pretty", SqlValue::Bool(b)) => pretty = b,
						("format", SqlValue::Strand(s)) => match s.as_str() {
							"json" => format = GraphQLFormat::Json,
							_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0))),
						},
						_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0))),
					}
				}
			}
			// The config argument was not supplied
			SqlValue::None => (),
			// An invalid config argument was received
			_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0))),
		}
		// Process the graphql query argument
		let req = match query {
			// It is a string, so parse the query
			SqlValue::Strand(s) => match format {
				GraphQLFormat::Json => {
					let tmp: BatchRequest =
						serde_json::from_str(s.as_str()).map_err(|_| RpcError::ParseError)?;
					tmp.into_single().map_err(|_| RpcError::ParseError)?
				}
			},
			// It is an object, so build the query
			SqlValue::Object(mut o) => {
				// We expect a `query` key with the graphql query
				let mut tmp = match o.remove("query") {
					Some(SqlValue::Strand(s)) => async_graphql::Request::new(s),
					_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0))),
				};
				// We can accept a `variables` key with graphql variables
				match o.remove("variables").or(o.remove("vars")) {
					Some(obj @ SqlValue::Object(_)) => {
						let gql_vars = gql::schema::sql_value_to_gql_value(obj.into())
							.map_err(|_| RpcError::InvalidRequest)?;

						tmp = tmp.variables(async_graphql::Variables::from_value(gql_vars));
					}
					Some(_) => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0))),
					None => {}
				}
				// We can accept an `operation` key with a graphql operation name
				match o.remove("operationName").or(o.remove("operation")) {
					Some(SqlValue::Strand(s)) => tmp = tmp.operation_name(s),
					Some(_) => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0))),
					None => {}
				}
				// Return the graphql query object
				tmp
			}
			// We received an invalid graphql query
			_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.0))),
		};
		// Process and cache the graphql schema
		let schema = self
			.graphql_schema_cache()
			.get_schema(&self.session())
			.await
			.map_err(|e| RpcError::Thrown(e.to_string()))?;
		// Execute the request against the schema
		let res = schema.execute(req).await;
		// Serialize the graphql response
		let out = if pretty {
			let mut buf = Vec::new();
			let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
			let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
			res.serialize(&mut ser).ok().and_then(|_| String::from_utf8(buf).ok())
		} else {
			serde_json::to_string(&res).ok()
		}
		.ok_or(RpcError::Thrown("Serialization Error".to_string()))?;
		// Output the graphql response
		Ok(Value::Strand(out.into()).into())
			*/
	}
}

enum QueryForm<'a> {
	Text(&'a str),
	Parsed(Ast),
}

async fn run_query<T>(
	this: &T,
	query: QueryForm<'_>,
	vars: Option<Variables>,
) -> Result<Vec<Response>>
where
	T: RpcContext + ?Sized,
{
	let session = this.session();
	ensure!(T::LQ_SUPPORT || !session.rt, RpcError::BadLQConfig);

	let res = match query {
		QueryForm::Text(query) => this.kvs().execute(query, &session, vars).await?,
		QueryForm::Parsed(ast) => this.kvs().process(ast, &session, vars).await?,
	};
	// Post-process hooks for web layer
	for response in &res {
		// This error should be unreachable because we shouldn't proceed if there's no
		// handler
		match &response.query_type {
			QueryType::Live => {
				if let Ok(Value::Uuid(lqid)) = &response.result {
					this.handle_live(&lqid.0).await;
				}
			}
			QueryType::Kill => {
				if let Ok(Value::Uuid(lqid)) = &response.result {
					this.handle_kill(&lqid.0).await;
				}
			}
			_ => {}
		}
	}
	// Return the result to the client
	Ok(res)
}
