use std::mem;
use std::sync::Arc;

use anyhow::{Result, ensure};
use uuid::Uuid;

use crate::catalog::providers::{CatalogProvider, NamespaceProvider};
#[cfg(not(target_family = "wasm"))]
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::capabilities::MethodTarget;
use crate::dbs::{QueryResult, QueryType};
use crate::err::Error;
use crate::kvs::{LockType, TransactionType};
use crate::rpc::args::extract_args;
use crate::rpc::{DbResult, Method, RpcContext, RpcError};
use crate::sql::{
	Ast, CreateStatement, Data as SqlData, DeleteStatement, Expr, Fields, Function, FunctionCall,
	InsertStatement, KillStatement, LiveStatement, Model, Output, RelateStatement, SelectStatement,
	TopLevelExpr, UpdateStatement, UpsertStatement,
};
use crate::types::{PublicArray, PublicRecordIdKey, PublicUuid, PublicValue, PublicVariables};

/// utility function converting a `Value::String` into a `Expr::Table`
fn value_to_table(value: PublicValue) -> Expr {
	match value {
		PublicValue::String(s) => Expr::Table(s),
		x => Expr::from_public_value(x),
	}
}

/// returns if the expression returns a singular value when selected.
///
/// As this rpc is some what convuluted the singular conditions is not the same
/// for all cases.
fn singular(value: &PublicValue) -> bool {
	match value {
		PublicValue::Object(_) => true,
		PublicValue::RecordId(t) => !matches!(t.key, PublicRecordIdKey::Range(_)),
		_ => false,
	}
}

#[expect(async_fn_in_trait)]
pub trait RpcProtocolV1: RpcContext {
	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation
	async fn execute(
		&self,
		session: Option<Uuid>,
		method: Method,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		if !self.kvs().allows_rpc_method(&MethodTarget {
			method,
		}) {
			warn!("Capabilities denied RPC method call attempt, target: '{method}'");
			return Err(RpcError::MethodNotAllowed);
		}
		// Execute the desired method
		match method {
			Method::Ping => Ok(DbResult::Other(PublicValue::None)),
			Method::Info => self.info(session).await,
			Method::Use => self.yuse(session, params).await,
			Method::Signup => self.signup(session, params).await,
			Method::Signin => self.signin(session, params).await,
			Method::Authenticate => self.authenticate(session, params).await,
			Method::Invalidate => self.invalidate(session).await,
			Method::Reset => self.reset(session).await,
			Method::Kill => self.kill(session, params).await,
			Method::Live => self.live(session, params).await,
			Method::Set => self.set(session, params).await,
			Method::Unset => self.unset(session, params).await,
			Method::Select => self.select(session, params).await,
			Method::Insert => self.insert(session, params).await,
			Method::Create => self.create(session, params).await,
			Method::Upsert => self.upsert(session, params).await,
			Method::Update => self.update(session, params).await,
			Method::Merge => self.merge(session, params).await,
			Method::Patch => self.patch(session, params).await,
			Method::Delete => self.delete(session, params).await,
			Method::Version => self.version(params).await,
			Method::Query => self.query(session, params).await,
			Method::Relate => self.relate(session, params).await,
			Method::Run => self.run(session, params).await,
			Method::GraphQL => self.graphql(session, params).await,
			Method::InsertRelation => self.insert_relation(session, params).await,
			Method::Sessions => self.sessions().await,
			_ => Err(RpcError::MethodNotFound),
		}
	}

	async fn sessions(&self) -> Result<DbResult, RpcError> {
		Ok(DbResult::Other(PublicValue::Array(
			self.list_sessions().into_iter().map(|x| PublicValue::Uuid(PublicUuid(x))).collect(),
		)))
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// For both ns+db, string = change, null = unset, none = do nothing
		// We need to be able to adjust either ns or db without affecting the other
		// To be able to select a namespace, and then list resources in that namespace,
		// as an example
		let (ns, db) = extract_args::<(PublicValue, PublicValue)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (ns, db)".to_string()))?;
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.get_session(session_id.as_ref()).as_ref().clone();
		// Update the selected namespace
		match ns {
			PublicValue::None => (),
			PublicValue::Null => session.ns = None,
			PublicValue::String(ns) => {
				let kvs = self.kvs();
				let tx = kvs.transaction(TransactionType::Write, LockType::Optimistic).await?;
				tx.get_or_add_ns(None, &ns, self.kvs().is_strict_mode()).await?;
				tx.commit().await?;

				session.ns = Some(ns)
			}
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected ns to be string, got {unexpected:?}"
				)));
			}
		}
		// Update the selected database
		match db {
			PublicValue::None => (),
			PublicValue::Null => session.db = None,
			PublicValue::String(db) => {
				let ns = session.ns.clone().unwrap();
				let tx =
					self.kvs().transaction(TransactionType::Write, LockType::Optimistic).await?;
				tx.ensure_ns_db(None, &ns, &db, self.kvs().is_strict_mode()).await?;
				tx.commit().await?;
				session.db = Some(db)
			}
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected db to be string, got {unexpected:?}"
				)));
			}
		}
		// Clear any residual database
		if self.get_session(session_id.as_ref()).ns.is_none()
			&& self.get_session(session_id.as_ref()).db.is_some()
		{
			session.db = None;
		}
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return nothing
		Ok(DbResult::Other(PublicValue::None))
	}

	// TODO(gguillemas): Update this method in 3.0.0 to return an object instead of
	// a string. This will allow returning refresh tokens as well as any additional
	// credential resulting from signing up.
	async fn signup(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Process the method arguments
		let Some(PublicValue::Object(params)) = extract_args(params.into_vec()) else {
			return Err(RpcError::InvalidParams("Expected (params:object)".to_string()));
		};
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.get_session(session_id.as_ref()).clone().as_ref().clone();
		// Attempt signup, mutating the session
		let out: Result<PublicValue> =
			crate::iam::signup::signup(self.kvs(), &mut session, params.into())
				.await
				.map(|v| v.token.clone().map(PublicValue::String).unwrap_or(PublicValue::None));

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return the signup result
		out.map(DbResult::Other).map_err(Into::into)
	}

	// TODO(gguillemas): Update this method in 3.0.0 to return an object instead of
	// a string. This will allow returning refresh tokens as well as any additional
	// credential resulting from signing in.
	async fn signin(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Process the method arguments
		let Some(PublicValue::Object(params)) = extract_args(params.into_vec()) else {
			return Err(RpcError::InvalidParams("Expected (params:object)".to_string()));
		};
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.get_session(session_id.as_ref()).clone().as_ref().clone();
		// Attempt signin, mutating the session
		let out: Result<PublicValue> =
			crate::iam::signin::signin(self.kvs(), &mut session, params.into())
				.await
				.map(|v| PublicValue::String(v.token.clone()));
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return the signin result
		out.map(DbResult::Other).map_err(From::from)
	}

	async fn authenticate(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		tracing::debug!("authenticate");
		// Process the method arguments
		let Some(PublicValue::String(token)) = extract_args(params.into_vec()) else {
			return Err(RpcError::InvalidParams("Expected (token:string)".to_string()));
		};
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.get_session(session_id.as_ref()).as_ref().clone();
		// Attempt authentication, mutating the session
		let out: Result<PublicValue> =
			crate::iam::verify::token(self.kvs(), &mut session, token.as_str())
				.await
				.map(|_| PublicValue::None);

		tracing::debug!("authenticate out: {out:?}");
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return nothing on success
		out.map(DbResult::Other).map_err(From::from)
	}

	async fn invalidate(&self, session_id: Option<Uuid>) -> Result<DbResult, RpcError> {
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.get_session(session_id.as_ref()).as_ref().clone();
		// Clear the current session
		crate::iam::clear::clear(&mut session)?;
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return nothing on success
		Ok(DbResult::Other(PublicValue::None))
	}

	async fn reset(&self, session_id: Option<Uuid>) -> Result<DbResult, RpcError> {
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		if let Some(session_id) = session_id {
			self.del_session(&session_id);
		} else {
			// Clone the current session
			let mut session = self.get_session(session_id.as_ref()).as_ref().clone();
			// Reset the current session
			crate::iam::reset::reset(&mut session);
			// Store the updated session
			self.set_session(session_id, Arc::new(session));
		}
		// Drop the mutex guard
		mem::drop(guard);
		// Cleanup live queries
		self.cleanup_lqs(session_id.as_ref()).await;
		// Return nothing on success
		Ok(DbResult::Other(PublicValue::None))
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self, session_id: Option<Uuid>) -> Result<DbResult, RpcError> {
		let session = self.get_session(session_id.as_ref());
		let vars = Some(session.variables.clone());
		let mut res = self.kvs().execute("SELECT * FROM $auth", &session, vars).await?;

		let result = res.remove(0).result?;

		let first = result.first().unwrap_or_default();
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let Some((PublicValue::String(key), val)) =
			extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
		else {
			return Err(RpcError::InvalidParams("Expected (key:string, value:Value)".to_string()));
		};

		let mutex = self.lock();
		let guard = mutex.acquire().await.unwrap();
		let mut session = self.get_session(session_id.as_ref()).as_ref().clone();

		if session.expired() {
			return Err(anyhow::Error::new(Error::ExpiredSession).into());
		}

		match val {
			None | Some(PublicValue::None) => session.variables.remove(key.as_str()),
			Some(val) => {
				crate::rpc::check_protected_param(&key)?;
				session.variables.insert(key, val)
			}
		}
		self.set_session(session_id, Arc::new(session));

		mem::drop(guard);

		// Return nothing
		Ok(DbResult::Other(PublicValue::Null))
	}

	async fn unset(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let Some(PublicValue::String(key)) = extract_args(params.into_vec()) else {
			return Err(RpcError::InvalidParams("Expected (key)".to_string()));
		};

		// Get the context lock
		let mutex = self.lock().clone();
		let guard = mutex.acquire().await;
		let mut session = self.get_session(session_id.as_ref()).as_ref().clone();
		session.variables.remove(key.as_str());
		self.set_session(session_id, Arc::new(session));
		mem::drop(guard);

		Ok(DbResult::Other(PublicValue::Null))
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (id,) = extract_args::<(PublicValue,)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (id)".to_string()))?;

		// Specify the SQL query string
		let ast = Ast {
			expressions: vec![TopLevelExpr::Kill(KillStatement {
				id: Expr::from_public_value(id),
			})],
		};
		// Specify the query parameters
		let vars = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, session_id, QueryForm::Parsed(ast), vars).await?;
		// Extract the first query result
		Ok(DbResult::Other(res.remove(0).result?))
	}

	async fn live(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, diff) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (what, diff)".to_string()))?;

		// If value is a strand, handle it as if it was a table.
		let what = match what {
			PublicValue::String(x) => Expr::Table(x),
			x => Expr::from_public_value(x),
		};

		// Specify the SQL query string
		let sql = LiveStatement {
			fields: if diff.unwrap_or(PublicValue::None).is_true() {
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
		let vars = Some(self.get_session(session_id.as_ref()).variables.clone());

		let res = run_query(self, session_id, QueryForm::Parsed(ast), vars).await?;

		// Extract the first query result
		Ok(DbResult::Other(res.into_iter().next().unwrap().result?))
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what,) = extract_args::<(PublicValue,)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (what:Value)".to_string()))?;

		// If the what is a single record with a non range value, make it return only a
		// single result.
		let only = match what {
			PublicValue::RecordId(ref x) => !x.key.is_range(),
			_ => false,
		};

		// If value is a string, handle it as if it was a table.
		let what = match what {
			PublicValue::String(x) => Expr::Table(x),
			x => Expr::from_public_value(x),
		};

		// Specify the SQL query string
		let sql = SelectStatement {
			only,
			expr: Fields::all(),
			what: vec![what],
			with: None,
			cond: None,
			omit: vec![],
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
		let vars = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), vars).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, PublicValue)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let into = match what {
			PublicValue::String(x) => Some(Expr::Table(x)),
			x => {
				if x.is_nullish() {
					None
				} else {
					Some(Expr::from_public_value(x))
				}
			}
		};

		// Specify the SQL query string
		let sql = InsertStatement {
			into,
			data: SqlData::SingleExpression(Expr::from_public_value(data)),
			output: Some(Output::After),
			..Default::default()
		};
		let ast = Ast::single_expr(Expr::Insert(Box::new(sql)));
		// Specify the query parameters
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	async fn insert_relation(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, PublicValue)>(params.to_vec())
			.ok_or(RpcError::InvalidParams("Expected (what, data)".to_string()))?;

		let what = match what {
			PublicValue::Null | PublicValue::None => None,
			PublicValue::String(x) => Some(Expr::Table(x)),
			x => Some(Expr::from_public_value(x)),
		};

		let data = SqlData::SingleExpression(Expr::from_public_value(data));

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
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let only = match what {
			PublicValue::String(_) => true,
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
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
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));

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
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), None).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
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
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));

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
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (what, data)".to_string()))?;

		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
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
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));
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
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for merging
	// ------------------------------

	async fn merge(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(RpcError::InvalidParams("Expected (what:Value, data:Value)".to_string()))?;

		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
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
			.map(|x| SqlData::MergeExpression(Expr::from_public_value(x)));
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
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for patching
	// ------------------------------

	async fn patch(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what, data, diff) =
			extract_args::<(PublicValue, Option<PublicValue>, Option<PublicValue>)>(
				params.into_vec(),
			)
			.ok_or(RpcError::InvalidParams(
				"Expected (what:Value, data:Value, diff:Value)".to_string(),
			))?;

		// Process the method arguments
		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
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
			.map(|x| SqlData::PatchExpression(Expr::from_public_value(x)));

		let diff = matches!(diff, Some(PublicValue::Bool(true)));

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
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self
			.kvs()
			.process(Ast::single_expr(expr), &self.get_session(session_id.as_ref()), var)
			.await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (from, kind, with, data) =
			extract_args::<(PublicValue, PublicValue, PublicValue, Option<PublicValue>)>(
				params.to_vec(),
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
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));

		// Specify the SQL query string
		let expr = Expr::Relate(Box::new(RelateStatement {
			only,
			from: Expr::from_public_value(from),
			through: value_to_table(kind),
			to: Expr::from_public_value(with),
			data,
			output: Some(Output::After),
			uniq: false,
			timeout: None,
			parallel: false,
		}));
		// Specify the query parameters
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self
			.kvs()
			.process(Ast::single_expr(expr), &self.get_session(session_id.as_ref()), var)
			.await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (what,) = extract_args::<(PublicValue,)>(params.into_vec())
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
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(&self, params: PublicArray) -> Result<DbResult, RpcError> {
		match params.len() {
			0 => Ok(self.version_data()),
			_ => Err(RpcError::InvalidParams("Expected 0 arguments".to_string())),
		}
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (query, vars) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(RpcError::InvalidParams(
			"Expected (query:string, vars:object)".to_string(),
		))?;

		let PublicValue::String(query) = query else {
			return Err(RpcError::InvalidParams("Expected query to be string".to_string()));
		};

		// Specify the query variables
		let vars = match vars {
			Some(PublicValue::Object(v)) => {
				let mut merged = self.get_session(session_id.as_ref()).variables.clone();
				merged.extend(v.into());
				Some(merged)
			}
			None | Some(PublicValue::None | PublicValue::Null) => {
				Some(self.get_session(session_id.as_ref()).variables.clone())
			}
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected vars to be object, got {unexpected:?}"
				)));
			}
		};

		let res = run_query(self, session_id, QueryForm::Text(&query), vars).await?;
		Ok(DbResult::Query(res))
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Process the method arguments
		let (name, version, args) =
			extract_args::<(PublicValue, Option<PublicValue>, Option<PublicValue>)>(
				params.into_vec(),
			)
			.ok_or(RpcError::InvalidParams(
				"Expected (name:string, version:string, args:array)".to_string(),
			))?;
		// Parse the function name argument
		let name = match name {
			PublicValue::String(v) => v,
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected name to be string, got {unexpected:?}"
				)));
			}
		};
		// Parse any function version argument
		let version = match version {
			Some(PublicValue::String(v)) => Some(v),
			None | Some(PublicValue::None | PublicValue::Null) => None,
			unexpected => {
				return Err(RpcError::InvalidParams(format!(
					"Expected version to be string, got {unexpected:?}"
				)));
			}
		};
		// Parse the function arguments if specified
		let args = match args {
			Some(PublicValue::Array(args)) => {
				args.into_iter().map(Expr::from_public_value).collect::<Vec<Expr>>()
			}
			None | Some(PublicValue::None | PublicValue::Null) => vec![],
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
		let var = Some(self.get_session(session_id.as_ref()).variables.clone());
		// Execute the function on the database
		let mut res = self.kvs().process(ast, &self.get_session(session_id.as_ref()), var).await?;
		// Extract the first query result
		let res = res.remove(0).result?;
		Ok(DbResult::Other(res))
	}

	// ------------------------------
	// Methods for querying with GraphQL
	// ------------------------------

	#[cfg(target_family = "wasm")]
	async fn graphql(
		&self,
		_session_id: Option<Uuid>,
		_: PublicArray,
	) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	#[cfg(not(target_family = "wasm"))]
	async fn graphql(
		&self,
		session_id: Option<Uuid>,
		_params: PublicArray,
	) -> Result<DbResult, RpcError> {
		//use crate::gql;

		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.get_session(session_id.as_ref()).au.as_ref()) {
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
			return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec())));
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
						("format", SqlValue::String(s)) => match s.as_str() {
							"json" => format = GraphQLFormat::Json,
							_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec()))),
						},
						_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec()))),
					}
				}
			}
			// The config argument was not supplied
			SqlValue::None => (),
			// An invalid config argument was received
			_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec()))),
		}
		// Process the graphql query argument
		let req = match query {
			// It is a string, so parse the query
			SqlValue::String(s) => match format {
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
					Some(SqlValue::String(s)) => async_graphql::Request::new(s),
					_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec()))),
				};
				// We can accept a `variables` key with graphql variables
				match o.remove("variables").or(o.remove("vars")) {
					Some(obj @ SqlValue::Object(_)) => {
						let gql_vars = gql::schema::sql_value_to_gql_value(obj.into())
							.map_err(|_| RpcError::InvalidRequest)?;

						tmp = tmp.variables(async_graphql::Variables::from_value(gql_vars));
					}
					Some(_) => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec()))),
					None => {}
				}
				// We can accept an `operation` key with a graphql operation name
				match o.remove("operationName").or(o.remove("operation")) {
					Some(SqlValue::String(s)) => tmp = tmp.operation_name(s),
					Some(_) => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec()))),
					None => {}
				}
				// Return the graphql query object
				tmp
			}
			// We received an invalid graphql query
			_ => return Err(RpcError::InvalidParams(format!("Expected (query, options) got {:?}", params.into_vec()))),
		};
		// Process and cache the graphql schema
		let schema = self
			.graphql_schema_cache()
			.get_schema(&self.get_session(session_id.as_ref()))
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
		Ok(PublicValue::String(out.into()).into())
			*/
	}
}

enum QueryForm<'a> {
	Text(&'a str),
	Parsed(Ast),
}

async fn run_query<T>(
	this: &T,
	session_id: Option<Uuid>,
	query: QueryForm<'_>,
	vars: Option<PublicVariables>,
) -> Result<Vec<QueryResult>>
where
	T: RpcContext + ?Sized,
{
	let session = this.get_session(session_id.as_ref());
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
				if let Ok(PublicValue::Uuid(lqid)) = &response.result {
					this.handle_live(&lqid.0, session_id).await;
				}
			}
			QueryType::Kill => {
				if let Ok(PublicValue::Uuid(lqid)) = &response.result {
					this.handle_kill(&lqid.0).await;
				}
			}
			_ => {}
		}
	}
	// Return the result to the client
	Ok(res)
}
