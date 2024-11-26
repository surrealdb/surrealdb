use crate::err::Error;
use std::{collections::BTreeMap, mem};

#[cfg(all(not(target_arch = "wasm32"), surrealdb_unstable))]
use async_graphql::BatchRequest;
use uuid::Uuid;

#[cfg(all(not(target_arch = "wasm32"), surrealdb_unstable))]
use crate::gql::SchemaCache;
use crate::{
	dbs::{capabilities::MethodTarget, QueryType, Response, Session},
	kvs::Datastore,
	rpc::args::Take,
	sql::{
		statements::{
			CreateStatement, DeleteStatement, InsertStatement, KillStatement, LiveStatement,
			RelateStatement, SelectStatement, UpdateStatement, UpsertStatement,
		},
		Array, Fields, Function, Model, Output, Query, Strand, Value,
	},
};

use super::{method::Method, response::Data, rpc_error::RpcError};

#[allow(async_fn_in_trait)]
pub trait RpcContext {
	fn kvs(&self) -> &Datastore;
	fn session(&self) -> &Session;
	fn session_mut(&mut self) -> &mut Session;
	fn vars(&self) -> &BTreeMap<String, Value>;
	fn vars_mut(&mut self) -> &mut BTreeMap<String, Value>;
	fn version_data(&self) -> Data;

	const LQ_SUPPORT: bool = false;
	fn handle_live(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle functions must be redefined if LQ_SUPPORT = true") }
	}
	fn handle_kill(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle functions must be redefined if LQ_SUPPORT = true") }
	}

	#[cfg(all(not(target_arch = "wasm32"), surrealdb_unstable))]
	const GQL_SUPPORT: bool = false;

	#[cfg(all(not(target_arch = "wasm32"), surrealdb_unstable))]
	fn graphql_schema_cache(&self) -> &SchemaCache {
		unimplemented!("graphql_schema_cache must be implemented if GQL_SUPPORT = true")
	}

	/// Executes any method on this RPC implementation
	async fn execute(&mut self, method: Method, params: Array) -> Result<Data, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		if !self.kvs().allows_rpc_method(&MethodTarget {
			method,
		}) {
			warn!("Capabilities denied RPC method call attempt, target: '{}'", method.to_str());
			return Err(RpcError::MethodNotAllowed);
		}
		// Execute the desired method
		match method {
			Method::Ping => Ok(Value::None.into()),
			Method::Info => self.info().await,
			Method::Use => self.yuse(params).await,
			Method::Signup => self.signup(params).await,
			Method::Signin => self.signin(params).await,
			Method::Invalidate => self.invalidate().await,
			Method::Authenticate => self.authenticate(params).await,
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
			Method::Unknown => Err(RpcError::MethodNotFound),
		}
	}

	/// Executes any immutable method on this RPC implementation
	async fn execute_immut(&self, method: Method, params: Array) -> Result<Data, RpcError> {
		// Check if capabilities allow executing the requested RPC method
		if !self.kvs().allows_rpc_method(&MethodTarget {
			method,
		}) {
			warn!("Capabilities denied RPC method call attempt, target: '{}'", method.to_str());
			return Err(RpcError::MethodNotAllowed);
		}
		// Execute the desired method
		match method {
			Method::Ping => Ok(Value::None.into()),
			Method::Info => self.info().await,
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
			Method::Unknown => Err(RpcError::MethodNotFound),
			_ => Err(RpcError::MethodNotFound),
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	async fn yuse(&mut self, params: Array) -> Result<Data, RpcError> {
		// For both ns+db, string = change, null = unset, none = do nothing
		// We need to be able to adjust either ns or db without affecting the other
		// To be able to select a namespace, and then list resources in that namespace, as an example
		let (ns, db) = params.needs_two()?;
		// Update the selected namespace
		match ns {
			Value::None => (),
			Value::Null => self.session_mut().ns = None,
			Value::Strand(ns) => self.session_mut().ns = Some(ns.0),
			_ => {
				return Err(RpcError::InvalidParams);
			}
		}
		// Update the selected database
		match db {
			Value::None => (),
			Value::Null => self.session_mut().db = None,
			Value::Strand(db) => self.session_mut().db = Some(db.0),
			_ => {
				return Err(RpcError::InvalidParams);
			}
		}
		// Clear any residual database
		if self.session().ns.is_none() && self.session().db.is_some() {
			self.session_mut().db = None;
		}
		// Return nothing
		Ok(Value::None.into())
	}

	async fn signup(&mut self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let mut tmp_session = mem::take(self.session_mut());

		let out: Result<Value, RpcError> =
			crate::iam::signup::signup(self.kvs(), &mut tmp_session, v)
				.await
				.map(Into::into)
				.map_err(Into::into);

		*self.session_mut() = tmp_session;
		out.map(Into::into)
	}

	// TODO(gguillemas): Remove this method in 3.0.0 and make `signinv2` the default.
	async fn signin(&mut self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let mut tmp_session = mem::take(self.session_mut());
		let out: Result<Value, RpcError> =
			crate::iam::signin::signin(self.kvs(), &mut tmp_session, v)
				.await
				// The default signin method just returns the token.
				.map(|data| data.token.into())
				.map_err(Into::into);
		*self.session_mut() = tmp_session;
		out.map(Into::into)
	}
	
	// TODO(gguillemas): This should be made the default in 3.0.0.
	// This method for signing in returns an object instead of a string, supporting additional values.
	// The original motivation for this method was the introduction of refresh tokens.
	async fn signinv2(&mut self, params: Array) -> Result<Data, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let mut tmp_session = mem::take(self.session_mut());
		let out: Result<Value, RpcError> =
			crate::iam::signin::signin(self.kvs(), &mut tmp_session, v)
				.await
				.map(Into::into)
				.map_err(Into::into);
		*self.session_mut() = tmp_session;
		out.map(Into::into)
	}

	async fn invalidate(&mut self) -> Result<Data, RpcError> {
		crate::iam::clear::clear(self.session_mut())?;
		Ok(Value::None.into())
	}

	async fn authenticate(&mut self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok(Value::Strand(token)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let mut tmp_session = mem::take(self.session_mut());
		let out: Result<(), RpcError> =
			crate::iam::verify::token(self.kvs(), &mut tmp_session, &token.0)
				.await
				.map_err(Into::into);
		*self.session_mut() = tmp_session;
		out.map(|_| Value::None.into())
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(&self) -> Result<Data, RpcError> {
		// Specify the SQL query string
		let sql = SelectStatement {
			expr: Fields::all(),
			what: vec![Value::Param("auth".into())].into(),
			..Default::default()
		}
		.into();
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), None).await?;
		// Extract the first value from the result
		Ok(res.remove(0).result?.first().into())
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(&mut self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
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
		// Return nothing
		Ok(Value::Null.into())
	}

	async fn unset(&mut self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok(Value::Strand(key)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		// Remove the set parameter
		self.vars_mut().remove(&key.0);
		// Return nothing
		Ok(Value::Null.into())
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(&mut self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let id = params.needs_one()?;
		// Specify the SQL query string
		let sql = KillStatement {
			id,
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.query_inner(Value::Query(sql), var).await?;
		// Extract the first query result
		Ok(res.remove(0).result?.into())
	}

	async fn live(&mut self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let (what, diff) = params.needs_one_or_two()?;
		// Specify the SQL query string
		let sql = LiveStatement::new_from_what_expr(
			match diff.is_true() {
				true => Fields::default(),
				false => Fields::all(),
			},
			what.could_be_table(),
		)
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.query_inner(Value::Query(sql), var).await?;
		// Extract the first query result
		Ok(res.remove(0).result?.into())
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok(what) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = SelectStatement {
			only: what.is_thing_single(),
			expr: Fields::all(),
			what: vec![what.could_be_table()].into(),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((what, data)) = params.needs_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = InsertStatement {
			into: match what.is_none_or_null() {
				false => Some(what.could_be_table()),
				true => None,
			},
			data: crate::sql::Data::SingleExpression(data),
			output: Some(Output::After),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	async fn insert_relation(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((what, data)) = params.needs_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = InsertStatement {
			relation: true,
			into: match what.is_none_or_null() {
				false => Some(what.could_be_table()),
				true => None,
			},
			data: crate::sql::Data::SingleExpression(data),
			output: Some(Output::After),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		let what = what.could_be_table();
		// Specify the SQL query string
		let sql = CreateStatement {
			only: what.is_thing_single() || what.is_table(),
			what: vec![what.could_be_table()].into(),
			data: match data.is_none_or_null() {
				false => Some(crate::sql::Data::ContentExpression(data)),
				true => None,
			},
			output: Some(Output::After),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = UpsertStatement {
			only: what.is_thing_single(),
			what: vec![what.could_be_table()].into(),
			data: match data.is_none_or_null() {
				false => Some(crate::sql::Data::ContentExpression(data)),
				true => None,
			},
			output: Some(Output::After),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = UpdateStatement {
			only: what.is_thing_single(),
			what: vec![what.could_be_table()].into(),
			data: match data.is_none_or_null() {
				false => Some(crate::sql::Data::ContentExpression(data)),
				true => None,
			},
			output: Some(Output::After),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for merging
	// ------------------------------

	async fn merge(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((what, data)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = UpdateStatement {
			only: what.is_thing_single(),
			what: vec![what.could_be_table()].into(),
			data: match data.is_none_or_null() {
				false => Some(crate::sql::Data::MergeExpression(data)),
				true => None,
			},
			output: Some(Output::After),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for patching
	// ------------------------------

	async fn patch(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((what, data, diff)) = params.needs_one_two_or_three() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = UpdateStatement {
			only: what.is_thing_single(),
			what: vec![what.could_be_table()].into(),
			data: Some(crate::sql::Data::PatchExpression(data)),
			output: match diff.is_true() {
				true => Some(Output::Diff),
				false => Some(Output::After),
			},
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((from, kind, with, data)) = params.needs_three_or_four() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = RelateStatement {
			only: from.is_single() && with.is_single(),
			from,
			kind: kind.could_be_table(),
			with,
			data: match data.is_none_or_null() {
				false => Some(crate::sql::Data::ContentExpression(data)),
				true => None,
			},
			output: Some(Output::After),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok(what) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		// Specify the SQL query string
		let sql = DeleteStatement {
			only: what.is_thing_single(),
			what: vec![what.could_be_table()].into(),
			output: Some(Output::Before),
			..Default::default()
		}
		.into();
		// Specify the query parameters
		let var = Some(self.vars().clone());
		// Execute the query on the database
		let mut res = self.kvs().process(sql, self.session(), var).await?;
		// Extract the first query result
		Ok(res
			.remove(0)
			.result
			.or_else(|e| match e {
				Error::SingleOnlyOutput => Ok(Value::None),
				e => Err(e),
			})?
			.into())
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(&self, params: Array) -> Result<Data, RpcError> {
		match params.len() {
			0 => Ok(self.version_data()),
			_ => Err(RpcError::InvalidParams),
		}
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((query, vars)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
		};
		// Check the query input type
		if !(query.is_query() || query.is_strand()) {
			return Err(RpcError::InvalidParams);
		}
		// Specify the query variables
		let vars = match vars {
			Value::Object(mut v) => Some(mrg! {v.0, &self.vars()}),
			Value::None | Value::Null => Some(self.vars().clone()),
			_ => return Err(RpcError::InvalidParams),
		};
		// Execute the specified query
		self.query_inner(query, vars).await.map(Into::into)
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(&self, params: Array) -> Result<Data, RpcError> {
		// Process the method arguments
		let Ok((name, version, args)) = params.needs_one_two_or_three() else {
			return Err(RpcError::InvalidParams);
		};
		// Parse the function name argument
		let name = match name {
			Value::Strand(Strand(v)) => v,
			_ => return Err(RpcError::InvalidParams),
		};
		// Parse any function version argument
		let version = match version {
			Value::Strand(Strand(v)) => Some(v),
			Value::None | Value::Null => None,
			_ => return Err(RpcError::InvalidParams),
		};
		// Parse the function arguments if specified
		let args = match args {
			Value::Array(Array(arr)) => arr,
			Value::None | Value::Null => vec![],
			_ => return Err(RpcError::InvalidParams),
		};
		// Specify the function to run
		let func: Query = match &name[0..4] {
			"fn::" => Function::Custom(name.chars().skip(4).collect(), args).into(),
			"ml::" => Model {
				name: name.chars().skip(4).collect(),
				version: version.ok_or(RpcError::InvalidParams)?,
				args,
			}
			.into(),
			_ => Function::Normal(name, args).into(),
		};
		// Specify the query variables
		let vars = Some(self.vars().clone());
		// Execute the function on the database
		let mut res = self.kvs().process(func, self.session(), vars).await?;
		// Extract the first query result
		Ok(res.remove(0).result?.into())
	}

	// ------------------------------
	// Methods for querying with GraphQL
	// ------------------------------

	#[cfg(any(target_arch = "wasm32", not(surrealdb_unstable)))]
	async fn graphql(&self, _: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	#[cfg(all(not(target_arch = "wasm32"), surrealdb_unstable))]
	async fn graphql(&self, params: Array) -> Result<Data, RpcError> {
		if !*GRAPHQL_ENABLE {
			return Err(RpcError::BadGQLConfig);
		}

		use serde::Serialize;

		use crate::{cnf::GRAPHQL_ENABLE, gql};

		if !Self::GQL_SUPPORT {
			return Err(RpcError::BadGQLConfig);
		}

		let Ok((query, options)) = params.needs_one_or_two() else {
			return Err(RpcError::InvalidParams);
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
			Value::Object(o) => {
				for (k, v) in o {
					match (k.as_str(), v) {
						("pretty", Value::Bool(b)) => pretty = b,
						("format", Value::Strand(s)) => match s.as_str() {
							"json" => format = GraphQLFormat::Json,
							_ => return Err(RpcError::InvalidParams),
						},
						_ => return Err(RpcError::InvalidParams),
					}
				}
			}
			// The config argument was not supplied
			Value::None => (),
			// An invalid config argument was received
			_ => return Err(RpcError::InvalidParams),
		}
		// Process the graphql query argument
		let req = match query {
			// It is a string, so parse the query
			Value::Strand(s) => match format {
				GraphQLFormat::Json => {
					let tmp: BatchRequest =
						serde_json::from_str(s.as_str()).map_err(|_| RpcError::ParseError)?;
					tmp.into_single().map_err(|_| RpcError::ParseError)?
				}
			},
			// It is an object, so build the query
			Value::Object(mut o) => {
				// We expect a `query` key with the graphql query
				let mut tmp = match o.remove("query") {
					Some(Value::Strand(s)) => async_graphql::Request::new(s),
					_ => return Err(RpcError::InvalidParams),
				};
				// We can accept a `variables` key with graphql variables
				match o.remove("variables").or(o.remove("vars")) {
					Some(obj @ Value::Object(_)) => {
						let gql_vars = gql::schema::sql_value_to_gql_value(obj)
							.map_err(|_| RpcError::InvalidRequest)?;

						tmp = tmp.variables(async_graphql::Variables::from_value(gql_vars));
					}
					Some(_) => return Err(RpcError::InvalidParams),
					None => {}
				}
				// We can accept an `operation` key with a graphql operation name
				match o.remove("operationName").or(o.remove("operation")) {
					Some(Value::Strand(s)) => tmp = tmp.operation_name(s),
					Some(_) => return Err(RpcError::InvalidParams),
					None => {}
				}
				// Return the graphql query object
				tmp
			}
			// We received an invalid graphql query
			_ => return Err(RpcError::InvalidParams),
		};
		// Process and cache the graphql schema
		let schema = self
			.graphql_schema_cache()
			.get_schema(self.session())
			.await
			.map_err(|e| RpcError::Thrown(e.to_string()))?;
		// Execute the request against the schema
		let res = schema.execute(req).await;
		// Serialize the graphql response
		let out = match pretty {
			true => {
				let mut buf = Vec::new();
				let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
				let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
				res.serialize(&mut ser).ok().and_then(|_| String::from_utf8(buf).ok())
			}
			false => serde_json::to_string(&res).ok(),
		}
		.ok_or(RpcError::Thrown("Serialization Error".to_string()))?;
		// Output the graphql response
		Ok(Value::Strand(out.into()).into())
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
			_ => return Err(fail!("Unexpected query type: {query:?}").into()),
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
