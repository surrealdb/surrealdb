use std::error::Error as StdError;
use std::pin::Pin;
use std::sync::Arc;
use std::{io, mem};

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use surrealdb_core::dbs::{QueryResult, Session};
use surrealdb_core::iam::check::check_ns_db;
use surrealdb_core::iam::{Action, ResourceKind};
use surrealdb_core::kvs::{Datastore, export};
use surrealdb_core::rpc::RpcError;
use surrealdb_protocol::proto::prost_types;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::{
	SurrealDbService, SurrealDbServiceServer,
};
use surrealdb_protocol::proto::rpc::v1::{
	AuthenticateRequest, AuthenticateResponse, ExportMlModelRequest, ExportMlModelResponse,
	ExportSqlRequest, ExportSqlResponse, HealthRequest, HealthResponse, ImportSqlRequest,
	ImportSqlResponse, InvalidateRequest, InvalidateResponse, QueryError, QueryRequest,
	QueryResponse, QueryResponseKind, QueryStats, ResetRequest, ResetResponse, SetRequest,
	SetResponse, SigninRequest, SigninResponse, SignupRequest, SignupResponse, SubscribeRequest,
	SubscribeResponse, UnsetRequest, UnsetResponse, UseRequest, UseResponse, VersionRequest,
	VersionResponse,
};
use surrealdb_types::SurrealValue;
use tokio::sync::Semaphore;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::cli::Config;

const LOG: &str = "surrealdb::grpc";

/// Helper function to convert protobuf Variables to surrealdb Variables
fn proto_vars_to_surreal(
	proto_vars: surrealdb_protocol::proto::v1::Variables,
) -> surrealdb_types::Variables {
	let map: std::collections::BTreeMap<String, surrealdb_types::Value> =
		proto_vars.variables.into_iter().map(|(k, v)| (k, proto_value_to_surreal(v))).collect();
	surrealdb_types::Variables::from(map)
}

/// Helper function to convert protobuf Value to surrealdb Value
fn proto_value_to_surreal(
	proto_val: surrealdb_protocol::proto::v1::Value,
) -> surrealdb_types::Value {
	// For now, use JSON as an intermediate format since both support serde
	// This is a temporary solution - ideally we'd have direct conversion
	let json = serde_json::to_value(&proto_val).unwrap_or(serde_json::Value::Null);
	serde_json::from_value(json).unwrap_or(surrealdb_types::Value::None)
}

/// Helper function to convert surrealdb Value to protobuf Value
fn surreal_value_to_proto(
	surreal_val: surrealdb_types::Value,
) -> surrealdb_protocol::proto::v1::Value {
	// For now, use JSON as an intermediate format since both support serde
	let json = serde_json::to_value(&surreal_val).unwrap_or(serde_json::Value::Null);
	serde_json::from_value(json).unwrap_or_else(|_| surrealdb_protocol::proto::v1::Value {
		value: None,
	})
}

/// Helper function to convert protobuf AccessMethod to surrealdb Variables
/// This is a simplified implementation that uses serde for conversion
fn access_method_to_vars(
	access: surrealdb_protocol::proto::rpc::v1::AccessMethod,
) -> surrealdb_types::Variables {
	// Use serde to convert AccessMethod to Variables via JSON
	let json = serde_json::to_value(&access).unwrap_or(serde_json::Value::Null);
	if let serde_json::Value::Object(map) = json {
		let vars: std::collections::BTreeMap<String, surrealdb_types::Value> = map
			.into_iter()
			.filter_map(|(k, v)| serde_json::from_value(v).ok().map(|val| (k, val)))
			.collect();
		surrealdb_types::Variables::from(vars)
	} else {
		surrealdb_types::Variables::default()
	}
}

/// Handles notification delivery for live queries
async fn notifications(
	ds: Arc<Datastore>,
	service: Arc<SurrealDbGrpcService>,
	canceller: CancellationToken,
) {
	// Listen to the notifications channel
	if let Some(channel) = ds.notifications() {
		// Loop continuously
		loop {
			tokio::select! {
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Receive a notification on the channel
				Ok(notification) = channel.recv() => {
					// Get the id for this notification
					let id = notification.id.as_ref();
					// Get the sender for this live query
					if let Some(sender) = service.live_queries.get(id) {
						// Convert notification to SubscribeResponse
						// Create protobuf Notification from surrealdb Notification
						use surrealdb_protocol::proto::rpc::v1::Notification;
						use surrealdb_protocol::proto::v1::Uuid as ProtoUuid;
						use surrealdb_protocol::proto::v1::RecordId as ProtoRecordId;

						// Convert action enum to i32
						let action_code = match notification.action {
							surrealdb_types::Action::Create => 0,
							surrealdb_types::Action::Update => 1,
							surrealdb_types::Action::Delete => 2,
							surrealdb_types::Action::Killed => 3,
						};

						// Convert the notification record to the protobuf RecordId
						// For now, we'll convert the entire record value to the record_id field
						// TODO: Properly parse and set table/id fields from RecordId type
						let proto_record_id = match &notification.record {
							surrealdb_types::Value::RecordId(record_id) => {
								// For now, use a simplified representation
								// The actual implementation should properly convert the key
								ProtoRecordId {
									table: record_id.table.to_string(),
									id: None, // TODO: Convert record_id.key to proper protobuf type
								}
							}
							_ => {
								// If not a record ID, use defaults
								ProtoRecordId {
									table: String::new(),
									id: None,
								}
							}
						};

						let proto_notification = Notification {
							live_query_id: Some(ProtoUuid {
								value: id.to_string(),
							}),
							action: action_code,
							record_id: Some(proto_record_id),
							value: Some(surreal_value_to_proto(notification.result)),
						};
						let response = SubscribeResponse {
							notification: Some(proto_notification),
						};
						// Send the notification to the client
						// If send fails, the client has disconnected, so we can ignore the error
						let _ = sender.send(response).await;
					}
				},
			}
		}
	}
}

/// Initialize and start the gRPC server.
///
/// Sets up the Tonic gRPC server with the SurrealDB service implementation.
///
/// # Parameters
/// - `opt`: Server configuration including bind address and TLS settings
/// - `ds`: The datastore instance to serve
/// - `ct`: Cancellation token for graceful shutdown
pub async fn init(opt: &Config, ds: Arc<Datastore>, ct: CancellationToken) -> Result<()> {
	// Create the gRPC service implementation
	let grpc_service = Arc::new(SurrealDbGrpcService::new(ds.clone()));

	// Spawn the notification delivery task
	let notification_task = {
		let service = grpc_service.clone();
		let datastore = ds.clone();
		let canceller = ct.clone();
		tokio::spawn(async move {
			notifications(datastore, service, canceller).await;
		})
	};

	// Build the Tonic server
	let mut server = Server::builder();

	// If a certificate and key are specified, then setup TLS
	if let (Some(cert), Some(key)) = (&opt.crt, &opt.key) {
		// Read the certificate and key files
		let cert_pem = tokio::fs::read(cert).await?;
		let key_pem = tokio::fs::read(key).await?;

		// Create TLS identity from certificate and key
		let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);

		// Configure TLS for the server
		let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);

		// Apply TLS configuration
		server = server
			.tls_config(tls_config)
			.map_err(|e| anyhow::anyhow!("Failed to configure TLS: {}", e))?;

		// Log the server startup
		info!(target: LOG, "Started gRPC server on {} (TLS enabled)", &opt.grpc_bind);
	} else {
		// Log the server startup without TLS
		info!(target: LOG, "Started gRPC server on {}", &opt.grpc_bind);
	}

	// Add the service and start the server
	let res = server
		.add_service(SurrealDbServiceServer::from_arc(grpc_service))
		.serve_with_shutdown(opt.grpc_bind, async move {
			ct.cancelled().await;
		})
		.await;

	// Wait for notification task to complete
	let _ = notification_task.await;

	// Catch the error and try to provide some guidance
	if let Err(e) = res {
		if opt.grpc_bind.port() < 1024 {
			if let Some(io_err) = e.source().and_then(|s| s.downcast_ref::<io::Error>()) {
				if let io::ErrorKind::PermissionDenied = io_err.kind() {
					error!(target: LOG, "Binding to ports below 1024 requires privileged access or special permissions.");
				}
			}
		}
		return Err(e.into());
	}

	// Log the server shutdown
	info!(target: LOG, "gRPC server stopped. Bye!");

	Ok(())
}

pub struct SurrealDbGrpcService {
	datastore: Arc<Datastore>,
	lock: Arc<Semaphore>,
	sessions: Arc<DashMap<Uuid, Arc<Session>>>,
	live_queries: Arc<DashMap<Uuid, surrealdb::channel::Sender<SubscribeResponse>>>,
}

impl SurrealDbGrpcService {
	pub fn new(datastore: Arc<Datastore>) -> Self {
		Self {
			datastore,
			lock: Arc::new(Semaphore::new(1)),
			sessions: Arc::new(DashMap::new()),
			live_queries: Arc::new(DashMap::new()),
		}
	}

	fn extract_session_id<T>(&self, request: &Request<T>) -> std::result::Result<Uuid, Status> {
		request
			.metadata()
			.get("session_id")
			.and_then(|v| v.to_str().ok())
			.and_then(|s| Uuid::try_parse(s).ok())
			.ok_or_else(|| Status::invalid_argument("Invalid or missing session_id"))
	}

	/// Get a session by ID, creating a default one if not found
	fn get_session(&self, id: &Uuid) -> Arc<Session> {
		if let Some(session) = self.sessions.get(id) {
			session.value().clone()
		} else {
			let session = Arc::new(Session::default());
			self.sessions.insert(*id, session.clone());
			session
		}
	}

	/// Mutable access to the current session for this RPC context
	fn set_session(&self, id: Uuid, session: Arc<Session>) {
		self.sessions.insert(id, session);
	}
}

#[tonic::async_trait]
impl SurrealDbService for SurrealDbGrpcService {
	// Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;
	type ExportSqlStream =
		Pin<Box<dyn Stream<Item = Result<ExportSqlResponse, Status>> + Send + Sync>>;
	type ExportMlModelStream =
		Pin<Box<dyn Stream<Item = Result<ExportMlModelResponse, Status>> + Send + Sync>>;
	type QueryStream = Pin<Box<dyn Stream<Item = Result<QueryResponse, Status>> + Send + Sync>>;
	type SubscribeStream =
		Pin<Box<dyn Stream<Item = Result<SubscribeResponse, Status>> + Send + Sync>>;

	async fn health(
		&self,
		_request: Request<HealthRequest>,
	) -> Result<Response<HealthResponse>, Status> {
		self.datastore.health_check().await.map_err(err_to_status)?;
		Ok(Response::new(HealthResponse {}))
	}

	async fn version(
		&self,
		_request: Request<VersionRequest>,
	) -> Result<Response<VersionResponse>, Status> {
		use crate::cnf::{PKG_NAME, PKG_VERSION};
		Ok(Response::new(VersionResponse {
			version: format!("{PKG_NAME}-{}", *PKG_VERSION),
		}))
	}

	async fn signup(
		&self,
		request: Request<SignupRequest>,
	) -> Result<Response<SignupResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Get the context lock
		let mutex = self.lock.clone();
		// Lock the context for update
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Convert protobuf variables to SurrealDB variables
		let vars = req.variables.map(proto_vars_to_surreal).unwrap_or_default();

		// Attempt signup, mutating the session
		let out: std::result::Result<surrealdb_types::Value, anyhow::Error> =
			surrealdb_core::iam::signup::signup(&self.datastore, &mut session, vars)
				.await
				.map(SurrealValue::into_value);

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);

		// Return the signup result
		let value = out.map_err(err_to_status)?;
		Ok(Response::new(SignupResponse {
			value: Some(surreal_value_to_proto(value)),
		}))
	}

	async fn signin(
		&self,
		request: Request<SigninRequest>,
	) -> Result<Response<SigninResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Get the context lock
		let mutex = self.lock.clone();
		// Lock the context for update
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Convert access_method to variables
		let vars = req.access_method.map(access_method_to_vars).unwrap_or_default();

		// Attempt signin, mutating the session
		let out: std::result::Result<surrealdb_types::Value, anyhow::Error> =
			surrealdb_core::iam::signin::signin(&self.datastore, &mut session, vars)
				.await
				.map(SurrealValue::into_value);

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);

		// Return the signin result
		let value = out.map_err(err_to_status)?;
		Ok(Response::new(SigninResponse {
			value: Some(surreal_value_to_proto(value)),
		}))
	}

	async fn authenticate(
		&self,
		request: Request<AuthenticateRequest>,
	) -> Result<Response<AuthenticateResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Extract the token string
		let token = req.token;

		// Get the context lock
		let mutex = self.lock.clone();
		// Lock the context for update
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Attempt authentication, mutating the session
		let out: std::result::Result<(), anyhow::Error> =
			surrealdb_core::iam::verify::token(&self.datastore, &mut session, &token)
				.await
				.map(|_| ());

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);

		// Return nothing on success
		out.map_err(err_to_status)?;
		Ok(Response::new(AuthenticateResponse {
			value: None,
		}))
	}

	async fn r#use(&self, request: Request<UseRequest>) -> Result<Response<UseResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Get the context lock
		let mutex = self.lock.clone();
		// Lock the context for update
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Update the selected namespace
		let ns = req.namespace;
		if !ns.is_empty() {
			session.ns = Some(ns);
		} else {
			session.ns = None;
		}

		// Update the selected database
		let db = req.database;
		if !db.is_empty() {
			if session.ns.is_none() {
				return Err(Status::failed_precondition("Namespace must be set before database"));
			}
			session.db = Some(db);
		} else {
			session.db = None;
		}

		// Clear any residual database if namespace was cleared
		if session.ns.is_none() && session.db.is_some() {
			session.db = None;
		}

		// Store the updated session
		self.set_session(session_id, Arc::new(session.clone()));
		// Drop the mutex guard
		mem::drop(guard);

		Ok(Response::new(UseResponse {
			namespace: session.ns.unwrap_or_default(),
			database: session.db.unwrap_or_default(),
		}))
	}

	async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Extract name and value
		let name = req.name;
		let value = req.value;

		// Get the context lock
		let mutex = self.lock.clone();
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Check for protected params
		surrealdb_core::rpc::check_protected_param(&name).map_err(err_to_status)?;

		// Set the variable in the session
		if let Some(value) = value {
			let val = proto_value_to_surreal(value);
			session.variables.insert(name, val);
		}

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);

		Ok(Response::new(SetResponse {}))
	}

	async fn unset(
		&self,
		request: Request<UnsetRequest>,
	) -> Result<Response<UnsetResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Extract name
		let name = req.name;

		// Get the context lock
		let mutex = self.lock.clone();
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Remove the variable from the session
		session.variables.remove(&name);

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);

		Ok(Response::new(UnsetResponse {}))
	}

	async fn invalidate(
		&self,
		request: Request<InvalidateRequest>,
	) -> Result<Response<InvalidateResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;

		// Get the context lock
		let mutex = self.lock.clone();
		// Lock the context for update
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");
		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Clear the current session
		surrealdb_core::iam::clear::clear(&mut session).map_err(err_to_status)?;

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);

		Ok(Response::new(InvalidateResponse {}))
	}

	async fn reset(
		&self,
		request: Request<ResetRequest>,
	) -> Result<Response<ResetResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;

		// Get the context lock
		let mutex = self.lock.clone();
		// Lock the context for update
		let guard = mutex.acquire().await.expect("mutex should not be poisoned");

		// Clone the current session
		let mut session = self.get_session(&session_id).as_ref().clone();

		// Reset the current session
		surrealdb_core::iam::reset::reset(&mut session);

		// Store the updated session
		self.set_session(session_id, Arc::new(session));
		// Drop the mutex guard
		mem::drop(guard);

		Ok(Response::new(ResetResponse {}))
	}

	async fn import_sql(
		&self,
		request: Request<Streaming<ImportSqlRequest>>,
	) -> Result<Response<ImportSqlResponse>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let stream = request.into_inner();

		// Get the session
		let session = self.get_session(&session_id);

		// Check the permissions level
		self.datastore
			.check(
				&session,
				Action::Edit,
				ResourceKind::Any.on_level(session.au.level().to_owned()),
			)
			.map_err(err_to_status)?;

		// Convert the gRPC stream to a stream compatible with import_stream
		let byte_stream = stream.map(|result| {
			result
				.map(|msg| Bytes::from(msg.statement))
				.map_err(|e| anyhow::anyhow!("Stream error: {}", e))
		});

		// Execute the import
		self.datastore.import_stream(&session, byte_stream).await.map_err(err_to_status)?;

		Ok(Response::new(ImportSqlResponse {}))
	}

	async fn export_sql(
		&self,
		request: Request<ExportSqlRequest>,
	) -> Result<Response<Self::ExportSqlStream>, Status> {
		let session_id = self.extract_session_id(&request)?;

		// Get the session
		let session = self.get_session(&session_id);

		// Ensure a NS and DB are set
		let (nsv, dbv) = check_ns_db(&session).map_err(err_to_status)?;

		// Check the permissions level
		self.datastore
			.check(&session, Action::View, ResourceKind::Any.on_db(&nsv, &dbv))
			.map_err(err_to_status)?;

		// Create a bounded channel for receiving export chunks
		let (snd, rcv) = surrealdb::channel::bounded::<Vec<u8>>(1);

		// Use default export configuration
		let cfg = export::Config::default();

		// Start the export task
		let task =
			self.datastore.export_with_config(&session, snd, cfg).await.map_err(err_to_status)?;

		// Spawn the export task
		tokio::spawn(task);

		// Create a stream that reads from the channel and yields ExportSqlResponse messages
		let stream = futures::stream::unfold(rcv, |rcv| async move {
			match rcv.recv().await {
				Ok(bytes) => {
					// Convert Vec<u8> to String (SQL is UTF-8 text)
					match String::from_utf8(bytes) {
						Ok(statement) => Some((
							Ok(ExportSqlResponse {
								statement,
							}),
							rcv,
						)),
						Err(e) => Some((
							Err(Status::internal(format!("UTF-8 conversion error: {}", e))),
							rcv,
						)),
					}
				}
				Err(_) => None,
			}
		});

		Ok(Response::new(Box::pin(stream) as Self::ExportSqlStream))
	}

	async fn export_ml_model(
		&self,
		request: Request<ExportMlModelRequest>,
	) -> Result<Response<Self::ExportMlModelStream>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Extract model name and version from the request
		let name = req.name;
		let version = req.version;

		// Get the session
		let session = self.get_session(&session_id);

		// Ensure a NS and DB are set
		let (nsv, dbv) = check_ns_db(&session).map_err(err_to_status)?;

		// Check the permissions level
		self.datastore
			.check(&session, Action::View, ResourceKind::Model.on_db(&nsv, &dbv))
			.map_err(err_to_status)?;

		// Get the model information
		let Some(info) = self
			.datastore
			.get_db_model(&nsv, &dbv, &name, &version)
			.await
			.map_err(err_to_status)?
		else {
			return Err(Status::not_found(format!("Model {name} {version} not found")));
		};

		// Construct the object storage path
		let path = format!("ml/{nsv}/{dbv}/{name}-{version}-{}.surml", info.hash);

		// Stream from object storage
		let mut data = surrealdb_core::obs::stream(path)
			.await
			.map_err(|e| Status::internal(format!("Failed to read model file: {}", e)))?;

		// Create a channel for streaming the model data
		let (snd, rcv) = surrealdb::channel::bounded::<std::result::Result<Bytes, String>>(1);

		// Spawn a task to read from object storage and send to channel
		tokio::spawn(async move {
			while let Some(result) = data.next().await {
				match result {
					Ok(bytes) => {
						if snd.send(Ok(bytes)).await.is_err() {
							break;
						}
					}
					Err(e) => {
						let _ = snd.send(Err(e.to_string())).await;
						break;
					}
				}
			}
		});

		// Create a stream that yields ExportMlModelResponse messages
		let stream = futures::stream::unfold(rcv, |rcv| async move {
			match rcv.recv().await {
				Ok(Ok(bytes)) => Some((
					Ok(ExportMlModelResponse {
						model: bytes,
					}),
					rcv,
				)),
				Ok(Err(e)) => Some((Err(Status::internal(format!("Stream error: {}", e))), rcv)),
				Err(_) => None,
			}
		});

		Ok(Response::new(Box::pin(stream) as Self::ExportMlModelStream))
	}

	async fn query(
		&self,
		request: Request<QueryRequest>,
	) -> Result<Response<Self::QueryStream>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Check if transaction ID is provided (not supported yet)
		if req.txn_id.is_some() {
			return Err(Status::unimplemented(
				"Transaction support not yet implemented for query method",
			));
		}

		// Get the session
		let session = self.get_session(&session_id);

		// Parse variables and merge with session variables
		let vars = if let Some(proto_vars) = req.variables {
			let mut merged = session.variables.clone();
			let request_vars = proto_vars_to_surreal(proto_vars);
			merged.extend(request_vars);
			Some(merged)
		} else {
			Some(session.variables.clone())
		};

		// Execute the query
		let results =
			self.datastore.execute(&req.query, &session, vars).await.map_err(err_to_status)?;

		// Get the total number of query results
		let result_count = results.len() as u32;

		// Convert each QueryResult to a QueryResponse
		let responses: Vec<QueryResponse> = results
			.into_iter()
			.enumerate()
			.map(|(index, query_result)| {
				query_result_to_response(query_result, index as u32, result_count)
			})
			.collect();

		// Create a stream from the responses
		let stream = tokio_stream::iter(responses.into_iter().map(Ok));

		Ok(Response::new(Box::pin(stream) as Self::QueryStream))
	}

	async fn subscribe(
		&self,
		request: Request<SubscribeRequest>,
	) -> Result<Response<Self::SubscribeStream>, Status> {
		let session_id = self.extract_session_id(&request)?;
		let req = request.into_inner();

		// Get the session
		let session = self.get_session(&session_id);

		// Check permissions
		if !self.datastore.allows_query_by_subject(session.au.as_ref()) {
			return Err(Status::permission_denied("Not authorized to subscribe to live queries"));
		}

		// Extract the subscribe_to field which is a oneof enum
		let subscribe_to = req
			.subscribe_to
			.ok_or_else(|| Status::invalid_argument("Missing subscribe_to field"))?;

		use surrealdb_protocol::proto::rpc::v1::subscribe_request::SubscribeTo;

		match subscribe_to {
			// Case 1: Subscribe to an existing live query by ID
			// No automatic cleanup - user must issue KILL manually
			SubscribeTo::LiveQueryId(uuid_proto) => {
				let uuid = Uuid::parse_str(&uuid_proto.value)
					.map_err(|_| Status::invalid_argument("Invalid live query ID format"))?;

				// Create channel and register
				let (snd, rcv) = surrealdb::channel::bounded::<SubscribeResponse>(10);
				self.live_queries.insert(uuid, snd);

				// Clone reference for cleanup (only removes from map, doesn't kill query)
				let live_queries = self.live_queries.clone();

				// Create stream without automatic KILL on close
				let stream = futures::stream::unfold(
					(rcv, uuid, live_queries),
					|(rcv, uuid, live_queries)| async move {
						match rcv.recv().await {
							Ok(response) => Some((Ok(response), (rcv, uuid, live_queries))),
							Err(_) => {
								// Channel closed - only remove from map, don't kill the live query
								live_queries.remove(&uuid);
								None
							}
						}
					},
				);

				Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
			}

			// Case 2: Execute a query to create a live query, then subscribe
			// Automatic cleanup - kills the live query when stream closes
			SubscribeTo::Query(query_req) => {
				// Execute the query to create the live query
				let vars = if let Some(proto_vars) = query_req.variables {
					let mut merged = session.variables.clone();
					let request_vars = proto_vars_to_surreal(proto_vars);
					merged.extend(request_vars);
					Some(merged)
				} else {
					Some(session.variables.clone())
				};

				// Execute the query (should be a LIVE query)
				let results = self
					.datastore
					.execute(&query_req.query, &session, vars)
					.await
					.map_err(err_to_status)?;

				// Extract the live query UUID from the first result
				if results.is_empty() {
					return Err(Status::invalid_argument("Query did not return a live query ID"));
				}

				let uuid = match &results[0].result {
					Ok(surrealdb_types::Value::Uuid(uuid)) => uuid.0,
					Ok(other) => {
						return Err(Status::invalid_argument(format!(
							"Expected UUID from LIVE query, got: {:?}",
							other
						)));
					}
					Err(e) => {
						return Err(Status::internal(format!("Query execution failed: {}", e)));
					}
				};

				// Create channel and register
				let (snd, rcv) = surrealdb::channel::bounded::<SubscribeResponse>(10);
				self.live_queries.insert(uuid, snd);

				// Clone references for cleanup with automatic KILL
				let live_queries = self.live_queries.clone();
				let datastore = self.datastore.clone();
				let session_for_cleanup = session.clone();

				// Create a cancellation token to signal cleanup
				let cleanup_token = tokio_util::sync::CancellationToken::new();
				let cleanup_token_clone = cleanup_token.clone();

				// Spawn a cleanup task that waits for stream to close
				tokio::spawn(async move {
					cleanup_token_clone.cancelled().await;
					// Remove from live queries map
					live_queries.remove(&uuid);
					// Kill the live query in the datastore
					let kill_query = "KILL $live_query_id";
					let mut vars = surrealdb_types::Variables::default();
					vars.insert(
						"live_query_id".to_string(),
						surrealdb_types::Value::Uuid(surrealdb::types::Uuid(uuid)),
					);
					let _ = datastore.execute(kill_query, &session_for_cleanup, Some(vars)).await;
				});

				// Create stream that signals cleanup token when done
				let stream = futures::stream::unfold(
					(rcv, cleanup_token),
					|(rcv, cleanup_token)| async move {
						match rcv.recv().await {
							Ok(response) => Some((Ok(response), (rcv, cleanup_token))),
							Err(_) => {
								// Channel closed - trigger cleanup
								cleanup_token.cancel();
								None
							}
						}
					},
				);

				Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
			}
		}
	}
}

/// Convert a QueryResult to a QueryResponse message
fn query_result_to_response(
	query_result: QueryResult,
	query_index: u32,
	result_count: u32,
) -> QueryResponse {
	// Extract the execution time
	let duration = query_result.time;

	// Convert the result
	match query_result.result {
		Ok(value) => {
			// Convert the value to a vector of protobuf values
			let values = match value {
				surrealdb_types::Value::Array(arr) => {
					// If it's an array, convert each element
					arr.into_iter().map(surreal_value_to_proto).collect()
				}
				single_value => {
					// Otherwise, wrap the single value in a vector
					vec![surreal_value_to_proto(single_value)]
				}
			};

			// Create query stats
			let stats = Some(QueryStats {
				records_returned: values.len() as i64,
				bytes_returned: -1,  // Not tracked yet
				records_scanned: -1, // Not tracked yet
				bytes_scanned: -1,   // Not tracked yet
				execution_duration: Some(prost_types::Duration {
					seconds: duration.as_secs() as i64,
					nanos: duration.subsec_nanos() as i32,
				}),
			});

			QueryResponse {
				query_index,
				batch_index: 0,
				result_count,
				kind: QueryResponseKind::BatchedFinal as i32,
				stats,
				error: None,
				values,
			}
		}
		Err(err) => {
			// Create an error response
			let error = Some(QueryError {
				code: -1, // Generic error code
				message: err.to_string(),
			});

			QueryResponse {
				query_index,
				batch_index: 0,
				result_count,
				kind: QueryResponseKind::BatchedFinal as i32,
				stats: None,
				error,
				values: vec![],
			}
		}
	}
}

/// Convert various error types to gRPC Status
fn err_to_status(err: impl Into<anyhow::Error>) -> Status {
	let err = err.into();

	// Try to downcast to RpcError first
	if let Some(rpc_err) = err.downcast_ref::<RpcError>() {
		return match rpc_err {
			RpcError::InvalidParams(msg) => Status::invalid_argument(msg.clone()),
			RpcError::MethodNotFound => Status::unimplemented("Method not found"),
			RpcError::MethodNotAllowed => Status::permission_denied("Method not allowed"),
			RpcError::ParseError => Status::invalid_argument("Parse error"),
			RpcError::InvalidRequest => Status::invalid_argument("Invalid request"),
			RpcError::InternalError(e) => Status::internal(e.to_string()),
			RpcError::Thrown(msg) => Status::internal(msg.clone()),
			RpcError::Serialize(msg) => Status::internal(format!("Serialization error: {}", msg)),
			RpcError::Deserialize(msg) => {
				Status::invalid_argument(format!("Deserialization error: {}", msg))
			}
			RpcError::LqNotSuported => Status::unimplemented("Live queries not supported"),
			RpcError::BadLQConfig => Status::failed_precondition("Bad live query configuration"),
			RpcError::BadGQLConfig => Status::failed_precondition("Bad GraphQL configuration"),
			// Catch-all for any future RpcError variants
			_ => Status::internal(format!("RPC error: {}", rpc_err)),
		};
	}

	// For other errors, return internal error
	Status::internal(err.to_string())
}
