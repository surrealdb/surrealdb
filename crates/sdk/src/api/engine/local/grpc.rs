use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::stream::BoxStream;
use surrealdb_core::{
	cnf::PKG_VERSION,
	dbs::{Notification, QueryResult, Session, SessionId, Variables},
	expr::{
		Cond, Fields, Function, Limit, LogicalPlan, Model, Number, Start, Timeout, Value, Values,
		Version,
		statements::{
			CreateStatement, DeleteStatement, InsertStatement, KillStatement, LiveStatement,
			RelateStatement, SelectStatement, UpdateStatement, UpsertStatement,
		},
	},
	gql::{Pessimistic, SchemaCache},
	iam::SigninParams,
	kvs::Datastore,
	vars,
};
use surrealdb_protocol::proto::{
	rpc::v1::{self as rpc_proto, SigninRequest},
	v1 as proto,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// TODO: Will need to map grpc subscription stream ID to their actual streams.
// type Client

type LiveQuerySender = tokio::sync::mpsc::Sender<Notification>;

#[derive(Default)]
pub struct ConnectionsState {
	pub(crate) live_queries: DashMap<Uuid, LiveQuerySender>,
}

pub struct SurrealDBGrpcService {
	/// The unique id of this gRPC connection
	pub(crate) id: Uuid,
	/// The system state for all gRPC connections
	pub(crate) state: Arc<ConnectionsState>,
	/// The datastore accessible to all gRPC connections
	pub(crate) datastore: Arc<Datastore>,
	/// The persistent session for this gRPC connection
	pub(crate) sessions: Arc<DashMap<SessionId, ArcSwap<Session>>>,
	/// The live queries mapping from Live Query ID to the tx channel for the subscription stream.
	pub(crate) live_queries: Arc<DashMap<Uuid, LiveQuerySender>>,
	/// A cancellation token called when shutting down the server
	pub(crate) shutdown: CancellationToken,
	/// A cancellation token for cancelling all spawned tasks
	pub(crate) canceller: CancellationToken,
	/// The GraphQL schema cache stored in advance
	pub(crate) gql_schema: SchemaCache<Pessimistic>,
}

impl SurrealDBGrpcService {
	pub fn new(
		id: Uuid,
		datastore: Arc<Datastore>,
		state: Arc<ConnectionsState>,
		canceller: CancellationToken,
		shutdown: CancellationToken,
	) -> Self {
		Self {
			id,
			state,
			sessions: Arc::new(DashMap::new()),
			live_queries: Arc::new(DashMap::new()),
			shutdown,
			canceller,
			gql_schema: SchemaCache::new(Arc::clone(&datastore)),
			datastore,
		}
	}

	fn load_session<R>(
		&self,
		request: &tonic::Request<R>,
	) -> Result<(SessionId, Arc<Session>), tonic::Status> {
		let session_id = request
			.extensions()
			.get::<SessionId>()
			.ok_or(tonic::Status::permission_denied("No session ID found"))?;
		let session = self
			.sessions
			.get(session_id)
			.ok_or(tonic::Status::permission_denied("Session not found"))?;
		Ok((*session_id, session.value().load_full()))
	}

	fn set_session(&self, session_id: SessionId, session: Arc<Session>) {
		self.sessions.get_mut(&session_id).unwrap().store(session);
	}

	fn check_subject_permissions(&self, session: &Session) -> Result<(), tonic::Status> {
		if !self.datastore.allows_query_by_subject(session.au.as_ref()) {
			return Err(tonic::Status::permission_denied("User not allowed to query"));
		}

		Ok(())
	}
}

#[tonic::async_trait]
impl rpc_proto::surreal_db_service_server::SurrealDbService for SurrealDBGrpcService {
	type QueryStream = BoxStream<'static, Result<rpc_proto::QueryResponse, tonic::Status>>;
	type LiveStream = BoxStream<'static, Result<rpc_proto::LiveResponse, tonic::Status>>;

	async fn health(
		&self,
		_: tonic::Request<rpc_proto::HealthRequest>,
	) -> Result<tonic::Response<rpc_proto::HealthResponse>, tonic::Status> {
		Ok(tonic::Response::new(rpc_proto::HealthResponse {}))
	}

	async fn version(
		&self,
		_: tonic::Request<rpc_proto::VersionRequest>,
	) -> Result<tonic::Response<rpc_proto::VersionResponse>, tonic::Status> {
		Ok(tonic::Response::new(rpc_proto::VersionResponse {
			version: format!("surrealdb-{}", *PKG_VERSION).into(),
		}))
	}

	async fn info(
		&self,
		request: tonic::Request<rpc_proto::InfoRequest>,
	) -> Result<tonic::Response<rpc_proto::InfoResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let plan = LogicalPlan::Select(SelectStatement {
			expr: Fields::all(),
			what: vec![Value::Param("auth".into())].into(),
			..Default::default()
		});
		let res = self
			.datastore
			.process_plan(plan, &session, None)
			.await
			.map_err(|e| tonic::Status::internal(e.to_string()))?;

		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		Ok(tonic::Response::new(rpc_proto::InfoResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn r#use(
		&self,
		request: tonic::Request<rpc_proto::UseRequest>,
	) -> Result<tonic::Response<rpc_proto::UseResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::UseRequest {
			namespace,
			database,
		} = request.into_inner();

		// Check if the user is allowed to query
		if !self.datastore.allows_query_by_subject(session.au.as_ref()) {
			return Err(tonic::Status::permission_denied("User not allowed to query"));
		}
		// For both ns+db, string = change, null = unset, none = do nothing
		// We need to be able to adjust either ns or db without affecting the other

		// Clone the current session
		let mut session = session.as_ref().clone();
		// Update the selected namespace
		if namespace.is_empty() {
			session.ns = None;
			session.db = None;
		} else {
			session.ns = Some(namespace);

			if database.is_empty() {
				session.db = None;
			} else {
				session.db = Some(database);
			}
		}

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::UseResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![],
			}),
		}))
	}

	async fn signup(
		&self,
		request: tonic::Request<rpc_proto::SignupRequest>,
	) -> Result<tonic::Response<rpc_proto::SignupResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let params = request
			.into_inner()
			.try_into()
			.map_err(|e: anyhow::Error| tonic::Status::invalid_argument(e.to_string()))?;

		let mut session = session.as_ref().clone();
		let result = crate::iam::signup::signup(&self.datastore, &mut session, params)
			.await
			.map(Value::from)
			.map_err(execution_error)?;
		self.set_session(session_id, Arc::new(session));

		Ok(tonic::Response::new(rpc_proto::SignupResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![result.try_into().map_err(invalid_argument)?],
			}),
		}))
	}

	async fn signin(
		&self,
		request: tonic::Request<rpc_proto::SigninRequest>,
	) -> Result<tonic::Response<rpc_proto::SigninResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let params = request
			.into_inner()
			.try_into()
			.map_err(|e: anyhow::Error| tonic::Status::invalid_argument(e.to_string()))?;

		let mut session = session.as_ref().clone();
		// Attempt signin, mutating the session
		let out: anyhow::Result<Value> =
			crate::iam::signin::signin(&self.datastore, &mut session, params)
				.await
				.map(Value::from);
		// Store the updated session
		self.set_session(session_id, Arc::new(session));

		let value = out.map_err(|e| tonic::Status::internal(e.to_string()))?;

		let value =
			value.try_into().map_err(|e: anyhow::Error| tonic::Status::internal(e.to_string()))?;

		// Return the signin result
		Ok(tonic::Response::new(rpc_proto::SigninResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![value],
			}),
		}))
	}

	async fn authenticate(
		&self,
		request: tonic::Request<rpc_proto::AuthenticateRequest>,
	) -> Result<tonic::Response<rpc_proto::AuthenticateResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;

		let params = request.into_inner();
		// Clone the current session
		let mut session = session.as_ref().clone();
		// Attempt authentication, mutating the session
		let out: anyhow::Result<Value> =
			crate::iam::verify::token(&self.datastore, &mut session, &params.token)
				.await
				.map(|_| Value::None);
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Return nothing on success
		let value = out.map_err(|e| tonic::Status::internal(e.to_string()))?;

		let value =
			value.try_into().map_err(|e: anyhow::Error| tonic::Status::internal(e.to_string()))?;

		Ok(tonic::Response::new(rpc_proto::AuthenticateResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![value],
			}),
		}))
	}

	async fn invalidate(
		&self,
		request: tonic::Request<rpc_proto::InvalidateRequest>,
	) -> Result<tonic::Response<rpc_proto::InvalidateResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		// Clone the current session
		let mut session = session.as_ref().clone();
		// Clear the current session
		crate::iam::clear::clear(&mut session)
			.map_err(|e| tonic::Status::internal(e.to_string()))?;
		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Return nothing on success
		Ok(tonic::Response::new(rpc_proto::InvalidateResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![],
			}),
		}))
	}

	async fn query(
		&self,
		request: tonic::Request<rpc_proto::QueryRequest>,
	) -> Result<tonic::Response<Self::QueryStream>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::QueryRequest {
			query,
			variables,
		} = request.into_inner();

		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		let (tx, rx) =
			tokio::sync::mpsc::channel::<Result<rpc_proto::QueryResponse, tonic::Status>>(100);

		let datastore = Arc::clone(&self.datastore);
		tokio::spawn(async move {
			let mut res = match datastore.execute(&query, &session, variables).await {
				Ok(res) => res,
				Err(err) => {
					if let Err(err) = tx.send(Err(tonic::Status::internal(err.to_string()))).await {
						error!("Failed to send query result to client: {err}");
					}
					return;
				}
			};

			for (
				query_index,
				QueryResult {
					stats,
					values,
				},
			) in res.into_iter().enumerate()
			{
				let stats_proto = match stats.try_into() {
					Ok(stats_proto) => stats_proto,
					Err(err) => {
						error!("Failed to convert stats to proto: {}", err);
						break;
					}
				};

				let values_proto = match values {
					Ok(values) => {
						let mut values_proto = Vec::with_capacity(values.len());
						for value in values {
							let value_proto = match value.try_into() {
								Ok(value_proto) => value_proto,
								Err(err) => {
									error!("Failed to convert value to proto: {}", err);
									break;
								}
							};
							values_proto.push(value_proto);
						}
						values_proto
					}
					Err(err) => {
						error!("Failed to convert values to proto: {}", err);
						break;
					}
				};

				let query_result_proto = rpc_proto::QueryResponse {
					query_index: query_index as u32,
					batch_index: 0,
					stats: Some(stats_proto),
					values: Some(rpc_proto::ValueBatch {
						values: values_proto,
					}),
				};

				if let Err(err) = tx.send(Ok(query_result_proto)).await.map_err(execution_error) {
					error!("Failed to send query result to client: {err}");
					break;
				}
			}
		});

		let output_stream = ReceiverStream::new(rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::QueryStream))
	}

	async fn live(
		&self,
		request: tonic::Request<rpc_proto::LiveRequest>,
	) -> std::result::Result<tonic::Response<Self::LiveStream>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)
			.map_err(|e| tonic::Status::permission_denied(e.to_string()))?;

		let rpc_proto::LiveRequest {
			what,
			expr,
			cond,
		} = request.into_inner();

		let what = what
			.context("what is required")
			.map_err(invalid_argument)?
			.try_into()
			.map_err(invalid_argument)?;
		let expr = match expr {
			Some(expr) => expr.try_into().map_err(invalid_argument)?,
			None => Fields::default(),
		};
		let cond = cond.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Specify the SQL query string
		let mut live_stmt = LiveStatement::new_from_what_expr(expr, what);
		live_stmt.cond = cond;

		let live_id = live_stmt.id.0;

		// Execute the query on the database
		self.datastore
			.process_plan(LogicalPlan::Live(live_stmt), &session, None)
			.await
			.map_err(|e| tonic::Status::internal(e.to_string()))?;

		// Create a channel and register it with the live queries map
		let (live_tx, mut live_rx) = tokio::sync::mpsc::channel(100);
		self.live_queries.insert(live_id, live_tx);

		// Create a channel for the response stream which will be used for sending results and managing the lifecycle of the live query.
		let (response_tx, response_rx) = tokio::sync::mpsc::channel(100);

		// Consume the live stream and send the results to the response channel
		let live_queries = Arc::clone(&self.live_queries);
		tokio::spawn(async move {
			while let Some(notification) = live_rx.recv().await {
				let notification_proto = match notification.try_into() {
					Ok(notification_proto) => notification_proto,
					Err(err) => {
						error!("Failed to convert notification to proto: {}", err);
						live_queries.remove(&live_id);
						break;
					}
				};

				let response =
					Result::<rpc_proto::LiveResponse, tonic::Status>::Ok(notification_proto);

				match response_tx.send(response).await {
					Ok(_) => (),
					Err(err) => {
						// If the response channel is closed, remove the live query from the map
						error!("Failed to send response to client: {err}");
						live_queries.remove(&live_id);
						break;
					}
				}
			}
		});

		let output_stream = ReceiverStream::new(response_rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::LiveStream))
	}

	async fn kill(
		&self,
		request: tonic::Request<rpc_proto::KillRequest>,
	) -> Result<tonic::Response<rpc_proto::KillResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::KillRequest {
			live_id,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		// Specify the SQL query string
		let live_id = Uuid::parse_str(&live_id)
			.map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
		let plan = LogicalPlan::Kill(KillStatement {
			id: Value::uuid(live_id),
		});
		// Execute the query on the database
		let mut res = self
			.datastore
			.process_plan(plan, &session, None)
			.await
			.map_err(|e| tonic::Status::internal(e.to_string()))?;
		// Extract the first query result
		todo!("STU")
		// Ok(tonic::Response::new(rpc_proto::KillResponse {
		//     data: Some(res.into()),
		// }))
	}

	async fn reset(
		&self,
		request: tonic::Request<rpc_proto::ResetRequest>,
	) -> Result<tonic::Response<rpc_proto::ResetResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		// Clone the current session
		let mut session = session.as_ref().clone();
		// Reset the current session
		crate::iam::reset::reset(&mut session);
		// Store the updated session
		self.set_session(session_id, Arc::new(session));

		// Cleanup live queries
		// TODO: Stu: Is this still necessary?
		// self.cleanup_live_queries(session_id).await;

		// Return nothing on success
		Ok(tonic::Response::new(rpc_proto::ResetResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![],
			}),
		}))
	}

	async fn set(
		&self,
		request: tonic::Request<rpc_proto::SetRequest>,
	) -> Result<tonic::Response<rpc_proto::SetResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::SetRequest {
			key,
			value,
		} = request.into_inner();

		let value = value
			.context("value is required")
			.map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

		let value =
			Value::try_from(value).map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		// Specify the query parameters
		let vars = vars!(key.clone() => Value::None);
		// Compute the specified parameter
		let value = self
			.datastore
			.compute(value, &session, Some(vars))
			.await
			.map_err(|e| tonic::Status::internal(e.to_string()))?;

		// Store the variable if defined, remove the variable if set to None
		let mut session = session.as_ref().clone();
		if value.is_none() {
			session.variables.remove(&key);
		} else {
			session.variables.insert(key, value);
		}

		self.set_session(session_id, Arc::new(session));
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::SetResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![],
			}),
		}))
	}

	async fn unset(
		&self,
		request: tonic::Request<rpc_proto::UnsetRequest>,
	) -> Result<tonic::Response<rpc_proto::UnsetResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::UnsetRequest {
			key,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let mut session = session.as_ref().clone();
		session.variables.remove(&key);
		self.set_session(session_id, Arc::new(session));
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::UnsetResponse {
			values: Some(rpc_proto::ValueBatch {
				values: vec![],
			}),
		}))
	}

	async fn select(
		&self,
		request: tonic::Request<rpc_proto::SelectRequest>,
	) -> Result<tonic::Response<rpc_proto::SelectResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;

		let rpc_proto::SelectRequest {
			txn,
			omit,
			only,
			with,
			split,
			group,
			order,
			timeout,
			version,
			fetch,
			cond,
			expr,
			what,
			start,
			limit,
			parallel,
			explain,
			tempfiles,
			variables,
		} = request.into_inner();

		let expr = expr
			.context("expr not set")
			.map_err(invalid_argument)?
			.try_into()
			.map_err(invalid_argument)?;
		let what = Values(
			what.into_iter()
				.map(TryInto::try_into)
				.collect::<Result<Vec<_>, _>>()
				.map_err(invalid_argument)?,
		);
		let cond = cond.map(TryInto::try_into).transpose().map_err(invalid_argument)?.map(Cond);
		let start = if start >= 0 {
			Some(start.into())
		} else {
			None
		};
		let limit = if limit >= 0 {
			Some(limit.into())
		} else {
			None
		};
		let timeout = timeout.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let version = version.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let fetch = fetch.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		// Specify the SQL query string
		let plan = LogicalPlan::Select(SelectStatement {
			only,
			expr,
			what,
			start,
			limit,
			cond,
			timeout,
			version,
			fetch,
			..Default::default()
		});
		// Execute the query on the database
		let mut res = self
			.datastore
			.process_plan(plan, &session, variables)
			.await
			.map_err(execution_error)?;

		let query_result =
			res.into_iter().next().ok_or(tonic::Status::internal("No query result"))?;

		let values = query_result.values.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		Ok(tonic::Response::new(rpc_proto::SelectResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn insert(
		&self,
		request: tonic::Request<rpc_proto::InsertRequest>,
	) -> Result<tonic::Response<rpc_proto::InsertResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::InsertRequest {
			txn,
			into,
			data,
			ignore,
			update,
			output,
			timeout,
			parallel,
			relation,
			version,
			variables,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let into = into
			.context("into is required")
			.map_err(invalid_argument)?
			.try_into()
			.map_err(invalid_argument)?;
		let data = data
			.context("data is required")
			.map_err(invalid_argument)?
			.try_into()
			.map_err(invalid_argument)?;
		let update = update;
		let output = output;
		let timeout = timeout;
		let parallel = parallel;
		let relation = relation;
		let output = output.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let timeout = timeout.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let version = version.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Specify the SQL query string
		let plan = LogicalPlan::Insert(InsertStatement {
			into: Some(into),
			data,
			output,
			relation,
			timeout,
			version,
			..Default::default()
		});
		// Execute the query on the database
		let mut res = self
			.datastore
			.process_plan(plan, &session, variables)
			.await
			.map_err(execution_error)?;

		if res.len() > 1 {
			return Err(tonic::Status::internal("Multiple query results"));
		}

		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		// Extract the first query result
		Ok(tonic::Response::new(rpc_proto::InsertResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn create(
		&self,
		request: tonic::Request<rpc_proto::CreateRequest>,
	) -> Result<tonic::Response<rpc_proto::CreateResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::CreateRequest {
			txn,
			only,
			what,
			data,
			output,
			timeout,
			version,
			parallel,
			variables,
		} = request.into_inner();

		let what = Values(
			what.into_iter()
				.map(TryInto::try_into)
				.collect::<Result<Vec<_>, _>>()
				.map_err(invalid_argument)?,
		);
		let data = data.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let output = output.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let timeout = timeout.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let version = version.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Specify the SQL query string
		let sql = LogicalPlan::Create(CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
			version,
			..Default::default()
		});
		// Execute the query on the database
		let mut res =
			self.datastore.process_plan(sql, &session, variables).await.map_err(execution_error)?;
		// Extract the first query result
		if res.len() > 1 {
			return Err(tonic::Status::internal("Multiple query results"));
		}
		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		Ok(tonic::Response::new(rpc_proto::CreateResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn upsert(
		&self,
		request: tonic::Request<rpc_proto::UpsertRequest>,
	) -> Result<tonic::Response<rpc_proto::UpsertResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::UpsertRequest {
			txn,
			only,
			what,
			data,
			output,
			timeout,
			with,
			cond,
			explain,
			parallel,
			variables,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let what = Values(
			what.into_iter()
				.map(TryInto::try_into)
				.collect::<Result<Vec<_>, _>>()
				.map_err(invalid_argument)?,
		);
		let data = data.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let output = output.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let timeout = timeout.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let with = with.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let cond = cond.map(TryInto::try_into).transpose().map_err(invalid_argument)?.map(Cond);
		let explain = explain.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let parallel = parallel;
		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Specify the SQL query string
		let sql = LogicalPlan::Upsert(UpsertStatement {
			only,
			what,
			data,
			output,
			timeout,
			with,
			cond,
			explain,
			parallel,
			..Default::default()
		});
		// Execute the query on the database
		let mut res =
			self.datastore.process_plan(sql, &session, variables).await.map_err(execution_error)?;
		// Extract the first query result
		if res.len() > 1 {
			return Err(tonic::Status::internal("Multiple query results"));
		}
		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;
		Ok(tonic::Response::new(rpc_proto::UpsertResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn update(
		&self,
		request: tonic::Request<rpc_proto::UpdateRequest>,
	) -> Result<tonic::Response<rpc_proto::UpdateResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::UpdateRequest {
			txn,
			only,
			what,
			data,
			output,
			timeout,
			with,
			cond,
			explain,
			parallel,
			variables,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let what = Values(
			what.into_iter()
				.map(TryInto::try_into)
				.collect::<Result<Vec<_>, _>>()
				.map_err(invalid_argument)?,
		);
		let data = data.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let output = output.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let timeout = timeout.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let with = with.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let cond = cond.map(TryInto::try_into).transpose().map_err(invalid_argument)?.map(Cond);
		let explain = explain.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let parallel = parallel;
		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Specify the SQL query string
		let sql = LogicalPlan::Update(UpdateStatement {
			only,
			what,
			data,
			output,
			timeout,
			with,
			explain,
			..Default::default()
		});
		// Execute the query on the database
		let mut res =
			self.datastore.process_plan(sql, &session, variables).await.map_err(execution_error)?;

		if res.len() > 1 {
			return Err(tonic::Status::internal("Multiple query results"));
		}
		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		// Extract the first query result
		Ok(tonic::Response::new(rpc_proto::UpdateResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn relate(
		&self,
		request: tonic::Request<rpc_proto::RelateRequest>,
	) -> Result<tonic::Response<rpc_proto::RelateResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::RelateRequest {
			txn,
			only,
			from,
			kind,
			with,
			data,
			output,
			timeout,
			parallel,
			uniq,
			variables,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let from = from
			.context("from is required")
			.map_err(invalid_argument)?
			.try_into()
			.map_err(invalid_argument)?;
		let kind = kind
			.context("kind is required")
			.map_err(invalid_argument)?
			.try_into()
			.map_err(invalid_argument)?;
		let with = with
			.context("with is required")
			.map_err(invalid_argument)?
			.try_into()
			.map_err(invalid_argument)?;
		let data = data.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let output = output.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let timeout = timeout.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		// Specify the SQL query string
		let sql = LogicalPlan::Relate(RelateStatement {
			only,
			from,
			kind,
			with,
			data,
			output,
			timeout,
			parallel,
			uniq,
			..Default::default()
		});
		// Execute the query on the database
		let mut res =
			self.datastore.process_plan(sql, &session, variables).await.map_err(execution_error)?;

		if res.len() > 1 {
			return Err(tonic::Status::internal("Multiple query results"));
		}
		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		// Extract the first query result
		Ok(tonic::Response::new(rpc_proto::RelateResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn delete(
		&self,
		request: tonic::Request<rpc_proto::DeleteRequest>,
	) -> Result<tonic::Response<rpc_proto::DeleteResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::DeleteRequest {
			txn,
			only,
			what,
			output,
			timeout,
			with,
			cond,
			explain,
			parallel,
			variables,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let what = Values(
			what.into_iter()
				.map(TryInto::try_into)
				.collect::<Result<Vec<_>, _>>()
				.map_err(invalid_argument)?,
		);
		let output = output.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let timeout = timeout.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let with = with.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let cond = cond.map(TryInto::try_into).transpose().map_err(invalid_argument)?.map(Cond);
		let explain = explain.map(TryInto::try_into).transpose().map_err(invalid_argument)?;
		let parallel = parallel;
		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Specify the SQL query string
		let sql = LogicalPlan::Delete(DeleteStatement {
			only,
			what,
			output,
			timeout,
			with,
			cond,
			explain,
			..Default::default()
		});
		// Execute the query on the database
		let mut res =
			self.datastore.process_plan(sql, &session, variables).await.map_err(execution_error)?;

		if res.len() > 1 {
			return Err(tonic::Status::internal("Multiple query results"));
		}
		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		// Extract the first query result
		Ok(tonic::Response::new(rpc_proto::DeleteResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}

	async fn run_function(
		&self,
		request: tonic::Request<rpc_proto::RunFunctionRequest>,
	) -> Result<tonic::Response<rpc_proto::RunFunctionResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::RunFunctionRequest {
			name,
			version,
			args,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let name = name;
		let version = version;
		let args = args
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		// TODO: We need to remove this bespoke logic, this should be in the API.
		let func: LogicalPlan = match &name[0..4] {
			"fn::" => {
				LogicalPlan::Value(Function::Custom(name.chars().skip(4).collect(), args).into())
			}
			"ml::" => LogicalPlan::Value(
				Model {
					name: name.chars().skip(4).collect(),
					version,
					args,
				}
				.into(),
			),
			_ => LogicalPlan::Value(Function::Normal(name, args).into()),
		};

		// Execute the function on the database
		let mut res =
			self.datastore.process_plan(func, &session, None).await.map_err(execution_error)?;

		let values = res
			.into_iter()
			.next()
			.ok_or(tonic::Status::internal("No query result"))?
			.values
			.map_err(execution_error)?;
		let values = values
			.into_iter()
			.map(TryInto::try_into)
			.collect::<Result<Vec<_>, _>>()
			.map_err(invalid_argument)?;

		// Extract the first query result
		Ok(tonic::Response::new(rpc_proto::RunFunctionResponse {
			values: Some(rpc_proto::ValueBatch {
				values,
			}),
		}))
	}
}

fn invalid_argument(err: impl ToString) -> tonic::Status {
	tonic::Status::invalid_argument(err.to_string())
}

fn execution_error(err: impl ToString) -> tonic::Status {
	tonic::Status::internal(err.to_string())
}
