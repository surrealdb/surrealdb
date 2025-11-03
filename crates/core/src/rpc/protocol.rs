use std::mem;
use std::sync::Arc;

use anyhow::{Result, ensure};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::catalog::providers::{CatalogProvider, NamespaceProvider};
use crate::dbs::capabilities::MethodTarget;
use crate::dbs::{QueryResult, QueryType, Session};
use crate::err::Error;
use crate::iam::token::Token;
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::rpc::args::extract_args;
use crate::rpc::{DbResult, Method, RpcError};
use crate::sql::{
	Ast, CreateStatement, Data as SqlData, DeleteStatement, Expr, Fields, Function, FunctionCall,
	InsertStatement, KillStatement, LiveStatement, Model, Output, RelateStatement, SelectStatement,
	TopLevelExpr, UpdateStatement, UpsertStatement,
};
use crate::types::{
	PublicArray, PublicRecordIdKey, PublicUuid, PublicValue, PublicVariables, SurrealValue,
};

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
pub trait RpcProtocol {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore;
	/// Retrieves the modification lock for this RPC context
	fn lock(&self) -> Arc<Semaphore>;
	/// The version information for this RPC context
	fn version_data(&self) -> DbResult;

	// ------------------------------
	// Sessions
	// ------------------------------

	/// The current session for this RPC context
	fn get_session(&self, id: Option<&Uuid>) -> Arc<Session>;
	/// Mutable access to the current session for this RPC context
	fn set_session(&self, id: Option<Uuid>, session: Arc<Session>);
	/// Deletes a session
	fn del_session(&self, id: &Uuid);
	// Lists all sessions
	fn list_sessions(&self) -> Vec<Uuid>;

	// ------------------------------
	// Transactions
	// ------------------------------

	/// Retrieves a transaction by ID
	async fn get_tx(&self, _id: Uuid) -> Result<Arc<crate::kvs::Transaction>, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Stores a transaction
	async fn set_tx(&self, _id: Uuid, _tx: Arc<crate::kvs::Transaction>) -> Result<(), RpcError> {
		Err(RpcError::MethodNotFound)
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled by default
	const LQ_SUPPORT: bool = false;

	/// Handles the execution of a LIVE statement
	fn handle_live(
		&self,
		_lqid: &Uuid,
		_session_id: Option<Uuid>,
	) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_live function must be implemented if LQ_SUPPORT = true") }
	}
	/// Handles the execution of a KILL statement
	fn handle_kill(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_kill function must be implemented if LQ_SUPPORT = true") }
	}

	/// Handles the cleanup of live queries
	fn cleanup_lqs(
		&self,
		session_id: Option<&Uuid>,
	) -> impl std::future::Future<Output = ()> + Send;

	/// Handles the cleanup of all live queries
	fn cleanup_all_lqs(&self) -> impl std::future::Future<Output = ()> + Send;

	// ------------------------------
	// Fetch sessions
	// ------------------------------

	async fn sessions(&self) -> Result<DbResult, RpcError> {
		Ok(DbResult::Other(PublicValue::Array(
			self.list_sessions().into_iter().map(|x| PublicValue::Uuid(PublicUuid(x))).collect(),
		)))
	}

	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation
	async fn execute(
		&self,
		txn: Option<Uuid>,
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
			Method::Info => self.info(txn, session).await,
			Method::Use => self.yuse(session, params).await,
			Method::Signup => self.signup(session, params).await,
			Method::Signin => self.signin(session, params).await,
			Method::Authenticate => self.authenticate(session, params).await,
			Method::Refresh => self.refresh(session, params).await,
			Method::Invalidate => self.invalidate(session).await,
			Method::Revoke => self.revoke(params).await,
			Method::Reset => self.reset(session).await,
			Method::Kill => self.kill(txn, session, params).await,
			Method::Live => self.live(txn, session, params).await,
			Method::Set => self.set(session, params).await,
			Method::Unset => self.unset(session, params).await,
			Method::Query => self.query(txn, session, params).await,
			Method::Version => self.version(txn, params).await,
			Method::Begin => self.begin(txn, session).await,
			Method::Commit => self.commit(txn, session, params).await,
			Method::Cancel => self.cancel(txn, session, params).await,
			Method::Sessions => self.sessions().await,
			// Deprecated methods
			Method::Select => self.select(txn, session, params).await,
			Method::Insert => self.insert(txn, session, params).await,
			Method::Create => self.create(txn, session, params).await,
			Method::Upsert => self.upsert(txn, session, params).await,
			Method::Update => self.update(txn, session, params).await,
			Method::Merge => self.merge(txn, session, params).await,
			Method::Patch => self.patch(txn, session, params).await,
			Method::Delete => self.delete(txn, session, params).await,
			Method::Relate => self.relate(txn, session, params).await,
			Method::Run => self.run(txn, session, params).await,
			Method::InsertRelation => self.insert_relation(txn, session, params).await,
			_ => Err(RpcError::MethodNotFound),
		}
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
				let ns = session.ns.clone().expect("namespace should be set");
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
				.map(SurrealValue::into_value);

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return the signup result
		out.map(DbResult::Other).map_err(Into::into)
	}

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
				.map(SurrealValue::into_value);
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
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return nothing on success
		out.map(DbResult::Other).map_err(From::from)
	}

	/// Refreshes an access token using a refresh token.
	///
	/// This RPC method implements the token refresh flow, allowing clients to
	/// obtain a new access token without re-authenticating. The method:
	///
	/// 1. Validates the provided token contains both access and refresh components
	/// 2. Uses the refresh token to authenticate and create new tokens
	/// 3. Revokes the old refresh token (single-use security model)
	/// 4. Updates the session with the new authentication state
	/// 5. Returns the new token pair to the client
	///
	/// # Arguments
	///
	/// * `session_id` - Optional session identifier for stateful connections
	/// * `params` - Array containing the token with both access and refresh components
	///
	/// # Returns
	///
	/// A new token containing fresh access and refresh tokens.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The token parameter is missing or invalid
	/// - The token doesn't contain a refresh component
	/// - The refresh token is invalid, expired, or already revoked
	async fn refresh(
		&self,
		session_id: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, RpcError> {
		tracing::debug!("refresh");
		// Process the method arguments
		let unexpected = || RpcError::InvalidParams("Expected (token:Token)".to_string());
		let Some(value) = extract_args(params.into_vec()) else {
			return Err(unexpected());
		};
		let Ok(token) = Token::from_value(value) else {
			return Err(unexpected());
		};
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.get_session(session_id.as_ref()).as_ref().clone();
		// Attempt token refresh, which will:
		// - Validate the refresh token
		// - Revoke the old refresh token
		// - Create new access and refresh tokens
		// - Update the session with the new authentication state
		let out: Result<PublicValue> =
			token.refresh(self.kvs(), &mut session).await.map(Token::into_value);
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);
		// Return the new token pair
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

	/// Revokes a refresh token, preventing it from being used to obtain new access tokens.
	///
	/// This RPC method explicitly invalidates a refresh token without affecting the
	/// current session. This is useful for:
	///
	/// - Logout operations where you want to prevent future token refreshes
	/// - Security events requiring immediate token invalidation
	/// - Explicit token lifecycle management
	///
	/// Unlike `invalidate()`, which clears the entire session, `revoke()` only
	/// invalidates the specific refresh token, allowing other sessions using
	/// different tokens to remain active.
	///
	/// # Arguments
	///
	/// * `params` - Array containing the token with the refresh token to revoke
	///
	/// # Returns
	///
	/// Returns nothing on success.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The token parameter is missing or invalid
	/// - The token doesn't contain a refresh component
	/// - The token doesn't contain valid namespace/database/access information
	async fn revoke(&self, params: PublicArray) -> Result<DbResult, RpcError> {
		tracing::debug!("revoke");
		// Process the method arguments
		let unexpected = || RpcError::InvalidParams("Expected (token:Token)".to_string());
		let Some(value) = extract_args(params.into_vec()) else {
			return Err(unexpected());
		};
		let Ok(token) = Token::from_value(value) else {
			return Err(unexpected());
		};
		// Revoke the refresh token by removing the grant record from the database.
		// This prevents the refresh token from being used to obtain new access tokens.
		token.revoke_refresh_token(self.kvs()).await?;
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

	async fn info(
		&self,
		_txn: Option<Uuid>,
		session_id: Option<Uuid>,
	) -> Result<DbResult, RpcError> {
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
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
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
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), vars).await?;
		// Extract the first query result
		Ok(DbResult::Other(res.remove(0).result?))
	}

	async fn live(
		&self,
		txn: Option<Uuid>,
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

		let (diff, fields) = if diff.unwrap_or_default().is_true() {
			(true, Fields::none())
		} else {
			(false, Fields::all())
		};

		// Specify the SQL query string
		let sql = LiveStatement {
			fields,
			diff,
			what,
			cond: None,
			fetch: None,
		};
		let ast = Ast {
			expressions: vec![TopLevelExpr::Live(Box::new(sql))],
		};
		// Specify the query parameters
		let vars = Some(self.get_session(session_id.as_ref()).variables.clone());

		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), vars).await?;

		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(
		&self,
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), vars).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(
		&self,
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	async fn insert_relation(
		&self,
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(
		&self,
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), None).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(
		&self,
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(
		&self,
		_txn: Option<Uuid>,
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
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for merging
	// ------------------------------

	async fn merge(
		&self,
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for patching
	// ------------------------------

	async fn patch(
		&self,
		_txn: Option<Uuid>,
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
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(
		&self,
		_txn: Option<Uuid>,
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
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(
		&self,
		txn: Option<Uuid>,
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(&self, _txn: Option<Uuid>, params: PublicArray) -> Result<DbResult, RpcError> {
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
		txn: Option<Uuid>,
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

		let res = run_query(self, txn, session_id, QueryForm::Text(&query), vars).await?;
		Ok(DbResult::Query(res))
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(
		&self,
		_txn: Option<Uuid>,
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
		let mut res = run_query(self, None, session_id, QueryForm::Parsed(ast), var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for transactions
	// ------------------------------

	/// Begin a new transaction
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		_session_id: Option<Uuid>,
	) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Commit a transaction
	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Option<Uuid>,
		_params: PublicArray,
	) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Cancel a transaction
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Option<Uuid>,
		_params: PublicArray,
	) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}
}

enum QueryForm<'a> {
	Text(&'a str),
	Parsed(Ast),
}

async fn run_query<T>(
	this: &T,
	txn: Option<Uuid>,
	session_id: Option<Uuid>,
	query: QueryForm<'_>,
	vars: Option<PublicVariables>,
) -> Result<Vec<QueryResult>>
where
	T: RpcProtocol + ?Sized,
{
	let session = this.get_session(session_id.as_ref());
	ensure!(T::LQ_SUPPORT || !session.rt, RpcError::BadLQConfig);

	// If a transaction UUID is provided, retrieve it and execute with it
	let res = if let Some(txn_id) = txn {
		// Retrieve the transaction - fail if not found
		let tx = this.get_tx(txn_id).await?;
		// Execute with the existing transaction by passing it through context
		match query {
			QueryForm::Text(query) => {
				this.kvs().execute_with_transaction(query, &session, vars, tx).await?
			}
			QueryForm::Parsed(ast) => {
				this.kvs().process_with_transaction(ast, &session, vars, tx).await?
			}
		}
	} else {
		// No transaction - execute normally
		match query {
			QueryForm::Text(query) => this.kvs().execute(query, &session, vars).await?,
			QueryForm::Parsed(ast) => this.kvs().process(ast, &session, vars).await?,
		}
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
