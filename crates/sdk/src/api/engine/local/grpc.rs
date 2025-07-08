use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::{StreamExt, stream::BoxStream};
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
	iam::{Action, ResourceKind, SigninParams, check::check_ns_db},
	kvs::{Datastore, LockType, TransactionType, export::Config},
	vars,
};
use surrealdb_protocol::proto::{
	rpc::v1::{self as rpc_proto, ExportMlModelResponse, SigninRequest},
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
	/// The live queries mapping from Live Query ID to the tx channel for the subscription stream.
	pub(crate) live_queries: Arc<DashMap<Uuid, LiveQuerySender>>,
}

impl ConnectionsState {
	pub fn insert_live_query(&self, live_id: Uuid, live_tx: LiveQuerySender) {
		self.live_queries.insert(live_id, live_tx);
	}

	pub fn remove_live_query(&self, live_id: &Uuid) {
		self.live_queries.remove(live_id);
	}
}

pub struct SurrealDBGrpcService {
	/// The unique id of this gRPC connection
	pub(crate) id: Uuid,
	/// The live queries mapping from Live Query ID to the tx channel for the subscription stream.
	pub(crate) connections_state: Arc<ConnectionsState>,
	/// The datastore accessible to all gRPC connections
	pub(crate) datastore: Arc<Datastore>,
	/// The persistent session for this gRPC connection
	pub(crate) sessions: Arc<DashMap<SessionId, ArcSwap<Session>>>,
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
		connections_state: Arc<ConnectionsState>,
		canceller: CancellationToken,
		shutdown: CancellationToken,
	) -> Self {
		Self {
			id,
			connections_state,
			sessions: Arc::new(DashMap::new()),
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
	type SubscribeStream = BoxStream<'static, Result<rpc_proto::SubscribeResponse, tonic::Status>>;
	type ExportSqlStream = BoxStream<'static, Result<rpc_proto::ExportSqlResponse, tonic::Status>>;
	type ExportMlModelStream =
		BoxStream<'static, Result<rpc_proto::ExportMlModelResponse, tonic::Status>>;

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
			value: Some(result.try_into().map_err(invalid_argument)?),
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

		let value_proto =
			value.try_into().map_err(|e: anyhow::Error| tonic::Status::internal(e.to_string()))?;

		// Return the signin result
		Ok(tonic::Response::new(rpc_proto::SigninResponse {
			value: Some(value_proto),
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

		let value_proto =
			value.try_into().map_err(|e: anyhow::Error| tonic::Status::internal(e.to_string()))?;

		Ok(tonic::Response::new(rpc_proto::AuthenticateResponse {
			value: Some(value_proto),
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
			session.ns = Some(namespace.clone());

			if database.is_empty() {
				session.db = None;
			} else {
				session.db = Some(database.clone());
			}
		}

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::UseResponse {
			namespace,
			database,
		}))
	}

	async fn set(
		&self,
		request: tonic::Request<rpc_proto::SetRequest>,
	) -> Result<tonic::Response<rpc_proto::SetResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::SetRequest {
			name,
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
		let vars = vars!(name.clone() => Value::None);
		// Compute the specified parameter
		let value = self
			.datastore
			.compute(value, &session, Some(vars))
			.await
			.map_err(|e| tonic::Status::internal(e.to_string()))?;

		// Store the variable if defined, remove the variable if set to None
		let mut session = session.as_ref().clone();
		if value.is_none() {
			session.variables.remove(&name);
		} else {
			session.variables.insert(name, value);
		}

		self.set_session(session_id, Arc::new(session));
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::SetResponse {}))
	}

	async fn unset(
		&self,
		request: tonic::Request<rpc_proto::UnsetRequest>,
	) -> Result<tonic::Response<rpc_proto::UnsetResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::UnsetRequest {
			name,
		} = request.into_inner();

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let mut session = session.as_ref().clone();
		session.variables.remove(&name);
		self.set_session(session_id, Arc::new(session));
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::UnsetResponse {}))
	}

	async fn invalidate(
		&self,
		request: tonic::Request<rpc_proto::InvalidateRequest>,
	) -> Result<tonic::Response<rpc_proto::InvalidateResponse>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		// Check if the user is allowed to query
		self.check_subject_permissions(&session)?;

		let mut session = session.as_ref().clone();
		crate::iam::clear::clear(&mut session);
		self.set_session(session_id, Arc::new(session));

		Ok(tonic::Response::new(rpc_proto::InvalidateResponse {}))
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
		// Return nothing on success
		Ok(tonic::Response::new(rpc_proto::ResetResponse {}))
	}

	async fn import_sql(
		&self,
		request: tonic::Request<tonic::Streaming<rpc_proto::ImportSqlRequest>>,
	) -> Result<tonic::Response<rpc_proto::ImportSqlResponse>, tonic::Status> {
		todo!("STU");
	}

	async fn export_sql(
		&self,
		request: tonic::Request<rpc_proto::ExportSqlRequest>,
	) -> Result<tonic::Response<Self::ExportSqlStream>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let export_request = request.into_inner();
		let config =
			Config::try_from(export_request).map_err(|e| tonic::Status::internal(e.to_string()))?;

		let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);

		let (tx, rx) = async_channel::bounded(1);

		let datastore = Arc::clone(&self.datastore);
		tokio::spawn(async move {
			let future = match datastore.export_with_config(&session, tx, config).await {
				Ok(future) => future,
				Err(err) => {
					error!("Failed to call export SQL with config: {err}");
					return;
				}
			};

			if let Err(err) = future.await {
				error!("Failed to export SQL: {err}");
			}
		});

		tokio::spawn(async move {
			loop {
				match rx.recv().await {
					Ok(bytes) => {
						let proto = rpc_proto::ExportSqlResponse {
							statement: std::str::from_utf8(&bytes).unwrap().to_string(),
						};
						if let Err(err) = response_tx.send(Ok(proto)).await {
							error!("Failed to send bytes to client: {err}");
							break;
						}
					}
					Err(err) => {
						error!("Failed to receive bytes from channel: {err}");
						if let Err(err) =
							response_tx.send(Err(tonic::Status::internal(err.to_string()))).await
						{
							error!("Failed to send error to client: {err}");
							break;
						}
					}
				}
			}
		});

		let output_stream = ReceiverStream::new(response_rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::ExportSqlStream))
	}

	async fn export_ml_model(
		&self,
		request: tonic::Request<rpc_proto::ExportMlModelRequest>,
	) -> Result<tonic::Response<Self::ExportMlModelStream>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::ExportMlModelRequest {
			name,
			version,
		} = request.into_inner();

		let (ns, db) = check_ns_db(&session).map_err(|e| tonic::Status::internal(e.to_string()))?;
		self.datastore
			.check(&session, Action::View, ResourceKind::Model.on_db(&ns, &db))
			.map_err(|e| tonic::Status::internal(e.to_string()))?;

		let (tx, rx) = tokio::sync::mpsc::channel(1);

		let kvs = Arc::clone(&self.datastore);
		tokio::spawn(async move {
			// Start a new readonly transaction
			let txn = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
			// Attempt to get the model definition
			let info = txn.get_db_model(&ns, &db, &name, &version).await?;
			// Export the file data in to the store
			let mut data = surrealdb_core::obs::stream(info.hash.clone()).await?;
			// Process all stream values
			while let Some(bytes) = data.next().await {
				let bytes = match bytes {
					Ok(bytes) => bytes,
					Err(err) => {
						tx.send(Err(tonic::Status::internal(err.to_string()))).await?;
						break;
					}
				};

				if let Err(err) = tx
					.send(Ok(ExportMlModelResponse {
						model: bytes.into(),
					}))
					.await
				{
					tx.send(Err(tonic::Status::internal(err.to_string()))).await?;
					break;
				}
			}

			Ok::<_, anyhow::Error>(())
		});

		let output_stream = ReceiverStream::new(rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::ExportMlModelStream))
	}

	async fn query(
		&self,
		request: tonic::Request<rpc_proto::QueryRequest>,
	) -> Result<tonic::Response<Self::QueryStream>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;
		let rpc_proto::QueryRequest {
			txn_id,
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
					error: None,
					values: values_proto,
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

	async fn subscribe(
		&self,
		request: tonic::Request<rpc_proto::SubscribeRequest>,
	) -> std::result::Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
		let (session_id, session) = self.load_session(&request)?;

		// Check if the user is allowed to query
		self.check_subject_permissions(&session)
			.map_err(|e| tonic::Status::permission_denied(e.to_string()))?;

		let rpc_proto::SubscribeRequest {
			query,
			variables,
		} = request.into_inner();

		let variables = variables.map(TryInto::try_into).transpose().map_err(invalid_argument)?;

		// Execute the query on the database
		let res = self
			.datastore
			.execute(&query, &session, variables)
			.await
			.map_err(|e| tonic::Status::internal(e.to_string()))?;

		let first_query_result =
			res.first().ok_or_else(|| tonic::Status::internal("No query results"))?;
		let values = first_query_result
			.values
			.as_ref()
			.map_err(|err| tonic::Status::internal("Query Failed: {err:?}"))?;
		let first_value = values.first().ok_or_else(|| tonic::Status::internal("No values"))?;

		let live_id = match first_value {
			Value::Uuid(id) => id.0,
			unexpected => {
				return Err(tonic::Status::internal(format!("Unexpected value: {:?}", unexpected)));
			}
		};

		// Create a channel and register it with the live queries map
		let (live_tx, mut live_rx) = tokio::sync::mpsc::channel(100);
		self.connections_state.insert_live_query(live_id, live_tx);

		// Create a channel for the response stream which will be used for sending results and managing the lifecycle of the live query.
		let (response_tx, response_rx) = tokio::sync::mpsc::channel(100);

		// Consume the live stream and send the results to the response channel
		let live_queries = Arc::clone(&self.connections_state);
		tokio::spawn(async move {
			while let Some(notification) = live_rx.recv().await {
				let notification_proto = match notification.try_into() {
					Ok(notification_proto) => notification_proto,
					Err(err) => {
						error!("Failed to convert notification to proto: {}", err);
						live_queries.remove_live_query(&live_id);
						break;
					}
				};

				let response =
					Result::<rpc_proto::SubscribeResponse, tonic::Status>::Ok(notification_proto);

				match response_tx.send(response).await {
					Ok(_) => (),
					Err(err) => {
						// If the response channel is closed, remove the live query from the map
						error!("Failed to send response to client: {err}");
						live_queries.remove_live_query(&live_id);
						break;
					}
				}
			}
		});

		let output_stream = ReceiverStream::new(response_rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::SubscribeStream))
	}
}

fn invalid_argument(err: impl ToString) -> tonic::Status {
	tonic::Status::invalid_argument(err.to_string())
}

fn execution_error(err: impl ToString) -> tonic::Status {
	tonic::Status::internal(err.to_string())
}
