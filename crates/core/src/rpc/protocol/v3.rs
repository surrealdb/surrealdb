#[cfg(not(target_family = "wasm"))]
use async_graphql::BatchRequest;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::dbs::ResponseData;
#[cfg(not(target_family = "wasm"))]
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::{QueryResult, Variables};
use crate::err::Error;
use crate::expr::{
	Cond, Duration, Fetchs, Limit, LogicalPlan, Number, Start, Timeout, Uuid, Version,
};
use crate::iam::{AccessMethod, SigninParams, SignupParams};

use crate::rpc::Method;
use crate::rpc::RpcContext;
use crate::rpc::RpcError;
#[cfg(not(target_family = "wasm"))]
use crate::rpc::request::GraphQlParams;
use crate::rpc::request::{
	AuthenticateParams, Command, CreateParams, DeleteParams, InsertParams, KillParams, LiveParams,
	QueryParams, RelateParams, RunParams, SelectParams, SetParams, UnsetParams, UpdateParams,
	UpsertParams, UseParams, VersionParams,
};
use crate::{
	dbs::capabilities::MethodTarget,
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
#[cfg(not(target_family = "wasm"))]
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;

#[expect(async_fn_in_trait)]
pub trait RpcProtocolV3: RpcContext {
	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation
	async fn execute(&self, command: Command) -> Result<ResponseData, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		// TODO(STU): DO NOT MERGE: Put this back.
		// if !self.kvs().allows_rpc_method(&MethodTarget {
		// 	method,
		// }) {
		// 	warn!("Capabilities denied RPC method call attempt, target: '{method}'");
		// 	return Err(RpcError::MethodNotAllowed);
		// }
		// Execute the desired method

		match command {
			Command::Health(_) => Ok(ResponseData::new_from_value(Value::None)),
			Command::Version(params) => self.version(params).await,
			Command::Info(_) => self.info().await,
			Command::Use(params) => self.yuse(params).await,
			Command::Signup(params) => self.signup(params).await,
			Command::Signin(params) => self.signin(params).await,
			Command::Authenticate(params) => self.authenticate(params).await,
			Command::Invalidate(params) => self.invalidate().await,
			Command::Reset(params) => self.reset().await,
			Command::Kill(params) => self.kill(params).await,
			Command::Live(params) => self.live(params).await,
			Command::Set(params) => self.set(params).await,
			Command::Unset(params) => self.unset(params).await,
			Command::Select(params) => self.select(params).await,
			Command::Insert(params) => self.insert(params).await,
			Command::Create(params) => self.create(params).await,
			Command::Upsert(params) => self.upsert(params).await,
			Command::Update(params) => self.update(params).await,
			Command::Delete(params) => self.delete(params).await,
			Command::Query(params) => self.query(params).await,
			Command::Relate(params) => self.relate(params).await,
			Command::Run(params) => self.run(params).await,
			Command::GraphQl(params) => self.graphql(params).await,
			_ => Err(RpcError::MethodNotFound),
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(
		&self,
		UseParams {
			namespace,
			database,
		}: UseParams,
	) -> Result<ResponseData, RpcError> {
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
		Ok(ResponseData::new_from_value(Value::None))
	}

	async fn signup(&self, params: SignupParams) -> Result<ResponseData, RpcError> {
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
		match out {
			Ok(value) => Ok(ResponseData::new_from_value(value)),
			Err(e) => Err(RpcError::InternalError(e)),
		}
	}

	async fn signin(&self, params: SigninParams) -> Result<ResponseData, RpcError> {
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
		match out {
			Ok(value) => Ok(ResponseData::new_from_value(value)),
			Err(e) => Err(RpcError::InternalError(e)),
		}
	}

	async fn authenticate(
		&self,
		AuthenticateParams {
			token,
		}: AuthenticateParams,
	) -> Result<ResponseData, RpcError> {
		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the current session
		let mut session = self.session().as_ref().clone();
		// Attempt authentication, mutating the session
		let out: Result<Value> =
			crate::iam::verify::token(self.kvs(), &mut session, &token).await.map(|_| Value::None);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		std::mem::drop(guard);
		// Return nothing on success
		match out {
			Ok(value) => Ok(ResponseData::new_from_value(value)),
			Err(e) => Err(RpcError::InternalError(e)),
		}
	}

	async fn invalidate(&self) -> Result<ResponseData, RpcError> {
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
		Ok(ResponseData::new_from_value(Value::None))
	}

	async fn reset(&self) -> Result<ResponseData, RpcError> {
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
		Ok(ResponseData::new_from_value(Value::None))
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<ResponseData, RpcError> {
		// Specify the SQL query string
		let plan = LogicalPlan::Select(SelectStatement {
			expr: Fields::all(),
			what: vec![Value::Param("auth".into())].into(),
			..Default::default()
		});
		// Execute the query on the database
		let mut res = self.kvs().process_plan(plan, &self.session(), None).await?;
		// Extract the first value from the result
		Ok(ResponseData::Results(res))
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(
		&self,
		SetParams {
			key,
			value,
		}: SetParams,
	) -> Result<ResponseData, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		};

		// Specify the query parameters
		let vars = Variables(map! {
			key.to_string() => Value::None,
		});
		// Compute the specified parameter
		match self.kvs().compute(value.into(), &self.session(), Some(vars)).await? {
			// Remove the variable if undefined
			Value::None => {
				// Get the context lock
				let mutex = self.lock().clone();
				// Lock the context for update
				let guard = mutex.acquire().await;
				// Clone the parameters
				let mut session = self.session().as_ref().clone();
				// Remove the set parameter
				session.variables.remove(&key);
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
				session.variables.insert(key, v);
				// Store the updated session
				self.set_session(Arc::new(session));
				// Drop the mutex guard
				std::mem::drop(guard);
			}
		};
		// Return nothing
		Ok(ResponseData::new_from_value(Value::Null))
	}

	async fn unset(
		&self,
		UnsetParams {
			key,
		}: UnsetParams,
	) -> Result<ResponseData, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}

		// Get the context lock
		let mutex = self.lock().clone();
		// Lock the context for update
		let guard = mutex.acquire().await;
		// Clone the parameters
		let mut session = self.session().as_ref().clone();
		// Remove the set parameter
		session.variables.remove(&key);
		// Store the updated session
		self.set_session(Arc::new(session));
		// Drop the mutex guard
		std::mem::drop(guard);
		// Return nothing
		Ok(ResponseData::new_from_value(Value::Null))
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(
		&self,
		KillParams {
			live_uuid,
		}: KillParams,
	) -> Result<ResponseData, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}

		// Specify the SQL query string
		let plan = LogicalPlan::Kill(KillStatement {
			id: Value::Uuid(Uuid(live_uuid)),
		});
		// Execute the query on the database
		let mut res = self.kvs().process_plan(plan, &self.session(), None).await?;
		// Extract the first query result
		Ok(ResponseData::Results(res))
	}

	async fn live(&self, params: LiveParams) -> Result<ResponseData, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}
		// Specify the query parameters
		todo!("STU: DELETE");
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
		// Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(
		&self,
		SelectParams {
			what,
			expr,
			omit,
			only,
			with,
			cond,
			split,
			group,
			order,
			start,
			limit,
			fetch,
			version,
			timeout,
			parallel,
			explain,
			tempfiles,
			variables,
		}: SelectParams,
	) -> Result<ResponseData, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}

		// Specify the SQL query string
		let plan = LogicalPlan::Select(SelectStatement {
			only: only.unwrap_or_default(),
			expr,
			what: vec![what].into(),
			start: start.map(|s| Start(Value::Number(Number::Int(s as i64)))),
			limit: limit.map(|l| Limit(Value::Number(Number::Int(l as i64)))),
			cond: cond.map(Cond),
			timeout: timeout.map(|d| Timeout(d.into())),
			version: version.map(Version),
			fetch,
			..Default::default()
		});
		// Execute the query on the database
		let mut res = self.kvs().process_plan(plan, &self.session(), Some(variables)).await?;
		// Extract the first query result
		Ok(ResponseData::Results(res))
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(&self, params: InsertParams) -> Result<ResponseData, RpcError> {
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
		// params.with_data_content(data);
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	params.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Extract the data from the Option
		// let Some(data) = params.data_expr() else {
		// 	return Err(RpcError::from(anyhow::Error::new(Error::unreachable(
		// 		"Data content was previously set, so it cannot be Option::None",
		// 	))));
		// };
		// // Specify the query parameters
		// let var = Some(params.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = InsertStatement {
		// 	into: match what.is_none_or_null() {
		// 		false => Some(what.could_be_table()),
		// 		true => None,
		// 	},
		// 	data,
		// 	output: params.output().unwrap_or_default(),
		// 	relation: params.relation().unwrap_or_default(),
		// 	timeout: params.timeout().unwrap_or_default(),
		// 	version: params.version().unwrap_or_default(),
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(&self, params: CreateParams) -> Result<ResponseData, RpcError> {
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
		// params.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	params.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	params.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// let what = what.could_be_table();
		// // Specify the query parameters
		// let var = Some(params.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = CreateStatement {
		// 	only: params.only().unwrap_or_default(),
		// 	what: vec![what.could_be_table()].into(),
		// 	data: params.data_expr(),
		// 	output: params.output().unwrap_or_default(),
		// 	timeout: params.timeout().unwrap_or_default(),
		// 	version: params.version().unwrap_or_default(),
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(&self, params: UpsertParams) -> Result<ResponseData, RpcError> {
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
		// params.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	params.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	params.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(params.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = UpsertStatement {
		// 	only: params.only().unwrap_or_default(),
		// 	what: vec![what.could_be_table()].into(),
		// 	data: params.data_expr(),
		// 	output: params.output().unwrap_or_default(),
		// 	cond: params.cond().unwrap_or_default(),
		// 	timeout: params.timeout().unwrap_or_default(),
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(&self, params: UpdateParams) -> Result<ResponseData, RpcError> {
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
		// params.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	params.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	params.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(params.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = UpdateStatement {
		// 	only: params.only().unwrap_or_default(),
		// 	what: vec![what.could_be_table()].into(),
		// 	data: params.data_expr(),
		// 	output: params.output().unwrap_or_default(),
		// 	cond: params.cond().unwrap_or_default(),
		// 	timeout: params.timeout().unwrap_or_default(),
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(&self, params: RelateParams) -> Result<ResponseData, RpcError> {
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
		// params.with_output(Output::After);
		// // Insert data
		// if !data.is_none_or_null() {
		// 	params.with_data_content(data);
		// }
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	params.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(params.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = RelateStatement {
		// 	only: params.only().unwrap_or_default(),
		// 	from,
		// 	kind: kind.could_be_table(),
		// 	with,
		// 	data: params.data_expr(),
		// 	output: params.output().unwrap_or_default(),
		// 	timeout: params.timeout().unwrap_or_default(),
		// 	uniq: params.unique().unwrap_or_default(),
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		//Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(&self, params: DeleteParams) -> Result<ResponseData, RpcError> {
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
		// params.with_output(Output::Before);
		// // Apply user options
		// if !opts_value.is_none_or_null() {
		// 	params.process_options(opts_value, self.kvs().get_capabilities())?;
		// }
		// // Specify the query parameters
		// let var = Some(params.merge_vars(&self.session().parameters));
		// // Specify the SQL query string
		// let sql = DeleteStatement {
		// 	only: params.only().unwrap_or_default(),
		// 	what: vec![what.could_be_table()].into(),
		// 	output: params.output().unwrap_or_default(),
		// 	timeout: params.timeout().unwrap_or_default(),
		// 	cond: params.cond().unwrap_or_default(),
		// 	..Default::default()
		// }
		// .into();
		// // Execute the query on the database
		// let mut res = self.kvs().process(sql, &self.session(), var).await?;
		// // Extract the first query result
		// Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(&self, _: VersionParams) -> Result<ResponseData, RpcError> {
		Ok(self.version_data())
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(
		&self,
		QueryParams {
			query,
			mut variables,
		}: QueryParams,
	) -> Result<ResponseData, RpcError> {
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(self.session().au.as_ref()) {
			return Err(RpcError::MethodNotAllowed);
		}

		// Execute the specified query
		self.query_inner(&query, Some(variables)).await
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(&self, params: RunParams) -> Result<ResponseData, RpcError> {
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
		// Ok(QueryResultData::Results(res))
	}

	// ------------------------------
	// Methods for querying with GraphQL
	// ------------------------------

	#[cfg(target_family = "wasm")]
	async fn graphql(&self, _: GraphQlParams) -> Result<ResponseData, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	#[cfg(not(target_family = "wasm"))]
	async fn graphql(&self, params: GraphQlParams) -> Result<ResponseData, RpcError> {
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
		// Ok(QueryResultData::new_from_value(Value::Strand(out.into())))
	}

	// ------------------------------
	// Private methods
	// ------------------------------

	async fn query_inner(
		&self,
		query: &str,
		vars: Option<Variables>,
	) -> Result<ResponseData, RpcError> {
		// If no live query handler force realtime off
		if !Self::LQ_SUPPORT && self.session().rt {
			return Err(RpcError::BadLQConfig);
		}
		// Execute the query on the database
		let res = self.kvs().execute(&query, &self.session(), vars).await?;

		// TODO: STU: Handle live queries in v3 protocol
		// Post-process hooks for web layer
		// for query_result in &res {
		// 	// This error should be unreachable because we shouldn't proceed if there's no handler
		// 	self.handle_live_query_results(query_result).await;
		// }
		// Return the result to the client
		Ok(ResponseData::Results(res))
	}

	// TODO: STU: Implement live query handling in v3 protocol
	// async fn handle_live_query_results(&self, res: &QueryResult) {
	// 	match &res.query_type {
	// 		QueryType::Live => {
	// 			if let Ok(Value::Uuid(lqid)) = &res.result {
	// 				self.handle_live(&lqid.0).await;
	// 			}
	// 		}
	// 		QueryType::Kill => {
	// 			if let Ok(Value::Uuid(lqid)) = &res.result {
	// 				self.handle_kill(&lqid.0).await;
	// 			}
	// 		}
	// 		_ => {}
	// 	}
	// }
}
