#[cfg(not(target_family = "wasm"))]
use async_graphql::BatchRequest;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
use crate::dbs::capabilities::ExperimentalTarget;
use crate::err::Error;
use crate::protocol::FromFlatbuffers;
#[cfg(not(target_family = "wasm"))]
use crate::protocol::flatbuffers::surreal_db::protocol::rpc::GraphQlParams;
use crate::protocol::flatbuffers::surreal_db::protocol::rpc::UseParams;
use crate::protocol::flatbuffers::surreal_db::protocol::rpc::{
	AuthenticateParams, Command, CreateParams, DeleteParams, InsertParams, KillParams, LiveParams,
	QueryParams, RelateParams, Request, RunParams, SelectParams, SetParams, SigninParams,
	SignupParams, UnsetParams, UpdateParams, UpsertParams, VersionParams,
};
use crate::rpc::Data;
use crate::rpc::Method;
use crate::rpc::RpcContext;
use crate::rpc::RpcError;
use crate::rpc::statement_options::StatementOptions;
use crate::sql::Uuid;
use crate::{
	dbs::{QueryType, Response, capabilities::MethodTarget},
	expr::{
		Array, Fields, Function, Model, Output, Query, Strand, Value,
		statements::{
			CreateStatement, DeleteStatement, InsertStatement, KillStatement, LiveStatement,
			RelateStatement, SelectStatement, UpdateStatement, UpsertStatement,
		},
	},
	rpc::args::Take,
};
use anyhow::Result;

#[expect(async_fn_in_trait)]
pub trait RpcProtocolV3: RpcContext {
	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation
	async fn execute<'rpc>(&'rpc self, request: Request<'rpc>) -> Result<Data, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		// TODO(STU): DO NOT MERGE: Put this back.
		// if !self.kvs().allows_rpc_method(&MethodTarget {
		// 	method,
		// }) {
		// 	warn!("Capabilities denied RPC method call attempt, target: '{method}'");
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// Execute the desired method

		match request.command_type() {
			Command::Health => Ok(Value::None.into()),
			Command::Version => {
				let params =
					request.command_as_version().expect("Version command should have parameters");
				self.version(params).await
			}
			Command::Info => self.info().await,
			Command::Use => {
				self.yuse(request.command_as_use().expect("Variant must contain UseParams")).await
			}
			Command::Signup => {
				let params =
					request.command_as_signup().expect("Variant must contain SignupParams");
				self.signup(params).await
			}
			Command::Signin => {
				let params =
					request.command_as_signin().expect("Variant must contain SigninParams");
				self.signin(params).await
			}
			Command::Authenticate => {
				let params = request
					.command_as_authenticate()
					.expect("Variant must contain AuthenticateParams");
				self.authenticate(params).await
			}
			Command::Invalidate => self.invalidate().await,
			Command::Reset => self.reset().await,
			Command::Kill => {
				let params = request.command_as_kill().expect("Variant must contain KillParams");
				self.kill(params).await
			}
			Command::Live => {
				let params = request.command_as_live().expect("Variant must contain LiveParams");
				self.live(params).await
			}
			Command::Set => {
				let params = request.command_as_set().expect("Variant must contain SetParams");
				self.set(params).await
			}
			Command::Unset => {
				let params = request.command_as_unset().expect("Variant must contain UnsetParams");
				self.unset(params).await
			}
			Command::Select => {
				let params =
					request.command_as_select().expect("Variant must contain SelectParams");
				self.select(params).await
			}
			Command::Insert => {
				let params =
					request.command_as_insert().expect("Variant must contain InsertParams");
				self.insert(params).await
			}
			Command::Create => {
				let params =
					request.command_as_create().expect("Variant must contain CreateParams");
				self.create(params).await
			}
			Command::Upsert => {
				let params =
					request.command_as_upsert().expect("Variant must contain UpsertParams");
				self.upsert(params).await
			}
			Command::Update => {
				let params =
					request.command_as_update().expect("Variant must contain UpdateParams");
				self.update(params).await
			}
			Command::Delete => {
				let params =
					request.command_as_delete().expect("Variant must contain DeleteParams");
				self.delete(params).await
			}
			Command::Query => {
				let params = request.command_as_query().expect("Variant must contain QueryParams");
				self.query(params).await
			}
			Command::Relate => {
				let params =
					request.command_as_relate().expect("Variant must contain RelateParams");
				self.relate(params).await
			}
			Command::Run => {
				let params = request.command_as_run().expect("Variant must contain RunParams");
				self.run(params).await
			}
			Command::GraphQl => {
				let params =
					request.command_as_graph_ql().expect("Variant must contain GraphqlParams");
				self.graphql(params).await
			}
			_ => Err(RpcError::MethodNotFound),
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse<'rpc>(&'rpc self, params: UseParams<'rpc>) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// For both ns+db, string = change, null = unset, none = do nothing
		// We need to be able to adjust either ns or db without affecting the other

		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().as_ref().clone();
		// Update the selected namespace

		let namespace = params.namespace().map(|ns| ns.to_string());
		let database = params.database().map(|db| db.to_string());

		match (namespace, database) {
			(Some(ns), None) => {
				// Set the namespace, but leave the database unset
				session.ns = Some(ns);
				session.db = None;
			}
			(Some(ns), Some(db)) => {
				// Set both the namespace and the database
				session.ns = Some(ns);
				session.db = Some(db);
			}
			_ => {
				// Unset the namespace and database
				session.ns = None;
				session.db = None;
			}
		}

		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		drop(guard);
		// Return nothing
		Ok(Value::None.into())
	}

	async fn signup(&self, params: SignupParams<'_>) -> Result<Data, RpcError> {
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().clone().as_ref().clone();
		// Attempt signup, mutating the session
		let out: Result<Value> =
			crate::iam::signup::signup(self.kvs(), &mut session, params).await.map(Value::from);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		std::mem::drop(guard);
		// Return the signup result
		out.map(Into::into).map_err(Into::into)
	}

	async fn signin(&self, params: SigninParams<'_>) -> Result<Data, RpcError> {
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().clone().as_ref().clone();
		// Attempt signin, mutating the session
		let out: Result<Value> =
			crate::iam::signin::signin(self.kvs(), &mut session, params).await.map(Value::from);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		std::mem::drop(guard);
		// Return the signin result
		out.map(Into::into).map_err(Into::into)
	}

	async fn authenticate(&self, params: AuthenticateParams<'_>) -> Result<Data, RpcError> {
		let token = params.token().ok_or_else(|| RpcError::InvalidParams)?;

		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().as_ref().clone();
		// Attempt authentication, mutating the session
		let out: Result<Value> =
			crate::iam::verify::token(self.kvs(), &mut session, token).await.map(|_| Value::None);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		std::mem::drop(guard);
		// Return nothing on success
		out.map_err(Into::into).map(Into::into)
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
		std::mem::drop(guard);
		// Return nothing on success
		Ok(Value::None.into())
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
		std::mem::drop(guard);
		// Cleanup live queries
		self.cleanup_lqs().await;
		// Return nothing on success
		Ok(Value::None.into())
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<Data, RpcError> {
		// Specify the SQL query string
		let plan = SelectStatement {
			expr: Fields::all(),
			what: vec![Value::Param("auth".into())].into(),
			..Default::default()
		}
		.into();
		// Execute the query on the database
		let mut res = self.kvs().process_plan(plan, &self.session(), None).await?;
		// Extract the first value from the result
		Ok(res.remove(0).result?.first().into())
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(&self, params: SetParams<'_>) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		};

		let Some(val) = params.value() else {
			return Err(RpcError::InvalidParams);
		};
		let Some(key) = params.key() else {
			return Err(RpcError::InvalidParams);
		};

		let val = Value::from_fb(val).map_err(|e| RpcError::InvalidParams)?;

		// Specify the query parameters
		let var = Some(map! {
			key.to_string() => Value::None,
		});
		// Compute the specified parameter
		match self.kvs().compute(val.into(), &self.session(), var).await? {
			// Remove the variable if undefined
			Value::None => {
				// Get the context lock
				let mutex = self.lock().clone();
				// Lock the context for update
				let guard = mutex.acquire().await;
				// Clone the parameters
				let mut session = self.session().as_ref().clone();
				// Remove the set parameter
				session.parameters.remove(key);
				// Store the updated session
				self.set_session(Arc::new(session));
				// Drop the mutex guard
				std::mem::drop(guard);
			}
			// Store the variable if defined
			v => {
				// Get the context lock
				let mutex = self.lock().clone();
				// Lock the context for update
				let guard = mutex.acquire().await;
				// Clone the parameters
				let mut session = self.session().as_ref().clone();
				// Remove the set parameter
				session.parameters.insert(key.to_string(), v);
				// Store the updated session
				self.set_session(Arc::new(session));
				// Drop the mutex guard
				std::mem::drop(guard);
			}
		};
		// Return nothing
		Ok(Value::Null.into())
	}

	async fn unset(&self, params: UnsetParams<'_>) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}

		let Some(key) = params.key() else {
			return Err(RpcError::InvalidParams);
		};

		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the parameters
		let mut session = self.session().as_ref().clone();
		// Remove the set parameter
		session.parameters.remove(key);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		std::mem::drop(guard);
		// Return nothing
		Ok(Value::Null.into())
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(&self, params: KillParams<'_>) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}

		let Some(live_uuid) = params.live_uuid() else {
			return Err(RpcError::InvalidParams);
		};

		// Specify the SQL query string
		let sql = KillStatement {
			id: Value::Uuid(live_uuid.parse()?),
		}
		.into();
		// Specify the query parameters
		let var = Some(self.session().parameters.clone());
		// Execute the query on the database
		let mut res = self.query_inner(SqlValue::Query(sql), var).await?;
		// Extract the first query result
		Ok(res.remove(0).result?.into())
	}

	async fn live(&self, params: LiveParams<'_>) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Prepare options
		let mut opts = StatementOptions::default();
		// Specify the query parameters
		todo!("STU: Implement live queries in v3 protocol");
		// let mut vars = proto_variables_to_expr_variables(&vars)?;
		// let vars = mrg! {vars, &self.session().parameters};
		// // Specify the SQL query string
		// let sql = LiveStatement {
		// 	id: Uuid::new_v4(),
		// 	node: Uuid::new_v4(),
		// 	what: SqlValue::Table(table.into()),
		// 	expr: if diff {
		// 		Fields::default()
		// 	} else {
		// 		fields.unwrap_or(Fields::all())
		// 	},
		// 	cond: cond.try_into()?,
		// 	fetch: fetchs,
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.query_inner(SqlValue::Query(sql), vars).await?;
		// // Extract the first query result
		// Ok(res.remove(0).result?.into())
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, params: SelectParams<'_>) -> Result<Data, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Prepare options
		let mut opts = StatementOptions::default();
		// Apply user options
		if let Some(options) = options {
			opts.process_options(options, self.kvs().get_capabilities())?;
		}
		// Specify the query parameters
		let var = Some(opts.merge_vars(&self.session().parameters));
		// Specify the SQL query string
		let sql = SelectStatement {
			only: opts.only,
			expr: opts.fields.unwrap_or_else(Fields::all),
			what: vec![what.could_be_table()].into(),
			start: opts.start,
			limit: opts.limit,
			cond: opts.cond,
			timeout: opts.timeout,
			version: opts.version,
			fetch: opts.fetch,
			..Default::default()
		}
		.into();
		// Execute the query on the database
		let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e.downcast_ref() {
				Some(Error::SingleOnlyOutput) => Ok(Value::None),
				_ => Err(RpcError::InternalError(e)),
			})?
			.into())
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(&self, params: InsertParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement insert in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((what, data, opts_value)) = params.needs_two_or_three() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Prepare options
		// let mut opts = StatementOptions::default();
		// // Insert data
		// opts.with_data_content(data);
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	opts.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Extract the data from the Option
		// let Some(data) = opts.data_expr() else {
		// 	return Err(RpcError::from(anyhow::Error::new(Error::unreachable(
		// 		"Data content was previously set, so it cannot be Option::None",
		// 	))));
		// };
		// // Specify the query parameters
		// let var = Some(opts.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = InsertStatement {
		// 	into: match what.is_none_or_null() {
		// 		false => Some(what.could_be_table()),
		// 		true => None,
		// 	},
		// 	data,
		// 	output: opts.output,
		// 	relation: opts.relation,
		// 	timeout: opts.timeout,
		// 	version: opts.version,
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(res
		// 	.remove(0)
		// 	.result
		// 	.or_else(|e| match e.downcast_ref() {
		// 		Some(Error::SingleOnlyOutput) => Ok(Value::None),
		// 		_ => Err(RpcError::InternalError(e)),
		// 	})?
		// 	.into())
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(&self, params: CreateParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement create in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((what, data, opts_value)) = params.needs_one_two_or_three() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Prepare options
		// let mut opts = StatementOptions::default();
		// // Set the default output
		// opts.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	opts.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	opts.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// let what = what.could_be_table();
		// // Specify the query parameters
		// let var = Some(opts.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = CreateStatement {
		// 	only: opts.only,
		// 	what: vec![what.could_be_table()].into(),
		// 	data: opts.data_expr(),
		// 	output: opts.output,
		// 	timeout: opts.timeout,
		// 	version: opts.version,
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(res
		// 	.remove(0)
		// 	.result
		// 	.or_else(|e| match e.downcast_ref() {
		// 		Some(Error::SingleOnlyOutput) => Ok(Value::None),
		// 		_ => Err(RpcError::InternalError(e)),
		// 	})?
		// 	.into())
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(&self, params: UpsertParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement upsert in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((what, data, opts_value)) = params.needs_one_two_or_three() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Prepare options
		// let mut opts = StatementOptions::default();
		// // Set the default output
		// opts.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	opts.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	opts.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(opts.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = UpsertStatement {
		// 	only: opts.only,
		// 	what: vec![what.could_be_table()].into(),
		// 	data: opts.data_expr(),
		// 	output: opts.output,
		// 	cond: opts.cond,
		// 	timeout: opts.timeout,
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(res
		// 	.remove(0)
		// 	.result
		// 	.or_else(|e| match e.downcast_ref() {
		// 		Some(Error::SingleOnlyOutput) => Ok(Value::None),
		// 		_ => Err(RpcError::InternalError(e)),
		// 	})?
		// 	.into())
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(&self, params: UpdateParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement update in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((what, data, opts_value)) = params.needs_one_two_or_three() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Prepare options
		// let mut opts = StatementOptions::default();
		// // Set the default output
		// opts.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	opts.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	opts.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(opts.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = UpdateStatement {
		// 	only: opts.only,
		// 	what: vec![what.could_be_table()].into(),
		// 	data: opts.data_expr(),
		// 	output: opts.output,
		// 	cond: opts.cond,
		// 	timeout: opts.timeout,
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(res
		// 	.remove(0)
		// 	.result
		// 	.or_else(|e| match e.downcast_ref() {
		// 		Some(Error::SingleOnlyOutput) => Ok(Value::None),
		// 		_ => Err(RpcError::InternalError(e)),
		// 	})?
		// 	.into())
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(&self, params: RelateParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement relate in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((from, kind, with, data, opts_value)) = params.needs_three_four_or_five() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Prepare options
		// let mut opts = StatementOptions::default();
		// // Set the default output
		// opts.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	opts.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	opts.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(opts.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = RelateStatement {
		// 	only: opts.only,
		// 	from,
		// 	kind: kind.could_be_table(),
		// 	with,
		// 	data: opts.data_expr(),
		// 	output: opts.output,
		// 	timeout: opts.timeout,
		// 	uniq: opts.unique,
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(res
		// 	.remove(0)
		// 	.result
		// 	.or_else(|e| match e.downcast_ref() {
		// 		Some(Error::SingleOnlyOutput) => Ok(Value::None),
		// 		_ => Err(RpcError::InternalError(e)),
		// 	})?
		// 	.into())
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(&self, params: DeleteParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement delete in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((what, opts_value)) = params.needs_one_or_two() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Prepare options
		// let mut opts = StatementOptions::default();
		// // Set the default output
		// opts.with_output(Output::Before);
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	opts.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(opts.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = DeleteStatement {
		// 	only: opts.only,
		// 	what: vec![what.could_be_table()].into(),
		// 	output: opts.output,
		// 	timeout: opts.timeout,
		// 	cond: opts.cond,
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(res
		// 	.remove(0)
		// 	.result
		// 	.or_else(|e| match e.downcast_ref() {
		// 		Some(Error::SingleOnlyOutput) => Ok(Value::None),
		// 		_ => Err(RpcError::InternalError(e)),
		// 	})?
		// 	.into())
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(&self, params: VersionParams<'_>) -> Result<Data, RpcError> {
		Ok(self.version_data())
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(&self, params: QueryParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement query in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((query, vars)) = params.needs_one_or_two() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Check the query input type
		// if !(query.is_query() || query.is_strand()) {
		// 	return Err(RpcError::InvalidParams);
		// }
		// // Specify the query variables
		// let vars = match vars {
		// 	SqlValue::Object(v) => {
		// 		let mut v: crate::expr::Object = v.into();
		// 		Some(mrg! {v.0, self.session().parameters})
		// 	}
		// 	SqlValue::None | SqlValue::Null => Some(self.session().parameters.clone()),
		// 	_ => return Err(RpcError::InvalidParams),
		// };
		// // Execute the specified query
		// self.query_inner(query, vars).await.map(Into::into)
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(&self, params: RunParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement run in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// // Process the method arguments
		// let Ok((name, version, args)) = params.needs_one_two_or_three() else {
		// 	return Err(RpcError::InvalidParams);
		// };
		// // Parse the function name argument
		// let name = match name {
		// 	SqlValue::Strand(Strand(v)) => v,
		// 	_ => return Err(RpcError::InvalidParams),
		// };
		// // Parse any function version argument
		// let version = match version {
		// 	SqlValue::Strand(Strand(v)) => Some(v),
		// 	SqlValue::None | SqlValue::Null => None,
		// 	_ => return Err(RpcError::InvalidParams),
		// };
		// // Parse the function arguments if specified
		// let args = match args {
		// 	SqlValue::Array(Array(arr)) => arr,
		// 	SqlValue::None | SqlValue::Null => vec![],
		// 	_ => return Err(RpcError::InvalidParams),
		// };
		// // Specify the function to run
		// let func: Query = match &name[0..4] {
		// 	"fn::" => Function::Custom(name.chars().skip(4).collect(), args).into(),
		// 	"ml::" => Model {
		// 		name: name.chars().skip(4).collect(),
		// 		version: version.ok_or(RpcError::InvalidParams)?,
		// 		args,
		// 	}
		// 	.into(),
		// 	_ => Function::Normal(name, args).into(),
		// };
		// // Specify the query parameters
		// let var = Some(self.session().parameters.clone());
		// // Execute the function on the database
		// let mut res = self.kvs().process(func, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(res.remove(0).result?.into())
	}

	// ------------------------------
	// Methods for querying with GraphQL
	// ------------------------------

	#[cfg(target_family = "wasm")]
	async fn graphql(&self, _: GraphQlParams<'_>) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	#[cfg(not(target_family = "wasm"))]
	async fn graphql(&self, params: GraphQlParams<'_>) -> Result<Data, RpcError> {
		todo!("STU: Implement graphql in v3 protocol");
		// // Check if the user is allowed to query
		// if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// if !self.kvs().get_capabilities().allows_experimental(&ExperimentalTarget::GraphQL) {
		// 	return Err(RpcError::BadGQLConfig);
		// }

		// use serde::Serialize;

		// use crate::gql;

		// if !Self::GQL_SUPPORT {
		// 	return Err(RpcError::BadGQLConfig);
		// }

		// let Ok((query, options)) = params.needs_one_or_two() else {
		// 	return Err(RpcError::InvalidParams);
		// };

		// enum GraphQLFormat {
		// 	Json,
		// }

		// // Default to compressed output
		// let mut pretty = false;
		// // Default to graphql json format
		// let mut format = GraphQLFormat::Json;
		// // Process any secondary config options
		// match options {
		// 	// A config object was passed
		// 	SqlValue::Object(o) => {
		// 		for (k, v) in o {
		// 			match (k.as_str(), v) {
		// 				("pretty", SqlValue::Bool(b)) => pretty = b,
		// 				("format", SqlValue::Strand(s)) => match s.as_str() {
		// 					"json" => format = GraphQLFormat::Json,
		// 					_ => return Err(RpcError::InvalidParams),
		// 				},
		// 				_ => return Err(RpcError::InvalidParams),
		// 			}
		// 		}
		// 	}
		// 	// The config argument was not supplied
		// 	SqlValue::None => (),
		// 	// An invalid config argument was received
		// 	_ => return Err(RpcError::InvalidParams),
		// }
		// // Process the graphql query argument
		// let req = match query {
		// 	// It is a string, so parse the query
		// 	SqlValue::Strand(s) => match format {
		// 		GraphQLFormat::Json => {
		// 			let tmp: BatchRequest =
		// 				serde_json::from_str(s.as_str()).map_err(|_| RpcError::ParseError)?;
		// 			tmp.into_single().map_err(|_| RpcError::ParseError)?
		// 		}
		// 	},
		// 	// It is an object, so build the query
		// 	SqlValue::Object(mut o) => {
		// 		// We expect a `query` key with the graphql query
		// 		let mut tmp = match o.remove("query") {
		// 			Some(SqlValue::Strand(s)) => async_graphql::Request::new(s),
		// 			_ => return Err(RpcError::InvalidParams),
		// 		};
		// 		// We can accept a `variables` key with graphql variables
		// 		match o.remove("variables").or(o.remove("vars")) {
		// 			Some(obj @ SqlValue::Object(_)) => {
		// 				let gql_vars = gql::schema::sql_value_to_gql_value(obj.into())
		// 					.map_err(|_| RpcError::InvalidRequest)?;

		// 				tmp = tmp.variables(async_graphql::Variables::from_value(gql_vars));
		// 			}
		// 			Some(_) => return Err(RpcError::InvalidParams),
		// 			None => {}
		// 		}
		// 		// We can accept an `operation` key with a graphql operation name
		// 		match o.remove("operationName").or(o.remove("operation")) {
		// 			Some(SqlValue::Strand(s)) => tmp = tmp.operation_name(s),
		// 			Some(_) => return Err(RpcError::InvalidParams),
		// 			None => {}
		// 		}
		// 		// Return the graphql query object
		// 		tmp
		// 	}
		// 	// We received an invalid graphql query
		// 	_ => return Err(RpcError::InvalidParams),
		// };
		// // Process and cache the graphql schema
		// let schema = self
		// 	.graphql_schema_cache()
		// 	.get_schema(&self.session())
		// 	.await
		// 	.map_err(|e| RpcError::Thrown(e.to_string()))?;
		// // Execute the request against the schema
		// let res = schema.execute(req).await;
		// // Serialize the graphql response
		// let out = if pretty {
		// 	let mut buf = Vec::new();
		// 	let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
		// 	let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
		// 	res.serialize(&mut ser).ok().and_then(|_| String::from_utf8(buf).ok())
		// } else {
		// 	serde_json::to_string(&res).ok()
		// }
		// .ok_or(RpcError::Thrown("Serialization Error".to_string()))?;
		// // Output the graphql response
		// Ok(Value::Strand(out.into()).into())
	}

	// ------------------------------
	// Private methods
	// ------------------------------

	async fn query_inner(
		&self,
		query: SqlValue,
		vars: Option<BTreeMap<String, Value>>,
	) -> Result<Vec<Response>, RpcError> {
		// If no live query handler force realtime off
		if !Self::LQ_SUPPORT && self.session().rt {
			return Err(RpcError::BadLQConfig);
		}
		// Execute the query on the database
		let res = match query {
			SqlValue::Query(sql) => self.kvs().process(sql, &self.session(), vars).await?,
			SqlValue::Strand(sql) => self.kvs().execute(&sql, &self.session(), vars).await?,
			_ => {
				return Err(RpcError::from(anyhow::Error::new(Error::unreachable(
					"Unexpected query type: {query:?}",
				))));
			}
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
