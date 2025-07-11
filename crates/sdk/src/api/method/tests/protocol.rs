use futures::StreamExt;
use futures::stream::BoxStream;
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;
use surrealdb_protocol::proto::v1::Value;

use surrealdb_protocol::proto::rpc::v1::ExportMlModelResponse;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::SurrealDbServiceServer;
use tokio_stream::wrappers::ReceiverStream;

pub struct TestServer;

#[tonic::async_trait]
impl rpc_proto::surreal_db_service_server::SurrealDbService for TestServer {
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
			version: "test-server".into(),
		}))
	}

	async fn signup(
		&self,
		_request: tonic::Request<rpc_proto::SignupRequest>,
	) -> Result<tonic::Response<rpc_proto::SignupResponse>, tonic::Status> {
		Ok(tonic::Response::new(rpc_proto::SignupResponse {
			value: Some(Value::string("jwt".to_string())),
		}))
	}

	async fn signin(
		&self,
		_request: tonic::Request<rpc_proto::SigninRequest>,
	) -> Result<tonic::Response<rpc_proto::SigninResponse>, tonic::Status> {
		Ok(tonic::Response::new(rpc_proto::SigninResponse {
			value: Some(Value::string("jwt".into())),
		}))
	}

	async fn authenticate(
		&self,
		_request: tonic::Request<rpc_proto::AuthenticateRequest>,
	) -> Result<tonic::Response<rpc_proto::AuthenticateResponse>, tonic::Status> {
		Ok(tonic::Response::new(rpc_proto::AuthenticateResponse {
			value: Some(Value::none()),
		}))
	}

	async fn r#use(
		&self,
		request: tonic::Request<rpc_proto::UseRequest>,
	) -> Result<tonic::Response<rpc_proto::UseResponse>, tonic::Status> {
		let rpc_proto::UseRequest {
			namespace,
			database,
		} = request.into_inner();

		// Return nothing
		Ok(tonic::Response::new(rpc_proto::UseResponse {
			namespace,
			database,
		}))
	}

	async fn set(
		&self,
		_request: tonic::Request<rpc_proto::SetRequest>,
	) -> Result<tonic::Response<rpc_proto::SetResponse>, tonic::Status> {
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::SetResponse {}))
	}

	async fn unset(
		&self,
		_request: tonic::Request<rpc_proto::UnsetRequest>,
	) -> Result<tonic::Response<rpc_proto::UnsetResponse>, tonic::Status> {
		// Return nothing
		Ok(tonic::Response::new(rpc_proto::UnsetResponse {}))
	}

	async fn invalidate(
		&self,
		_request: tonic::Request<rpc_proto::InvalidateRequest>,
	) -> Result<tonic::Response<rpc_proto::InvalidateResponse>, tonic::Status> {
		Ok(tonic::Response::new(rpc_proto::InvalidateResponse {}))
	}

	async fn reset(
		&self,
		_request: tonic::Request<rpc_proto::ResetRequest>,
	) -> Result<tonic::Response<rpc_proto::ResetResponse>, tonic::Status> {
		// Return nothing on success
		Ok(tonic::Response::new(rpc_proto::ResetResponse {}))
	}

	async fn import_sql(
		&self,
		request: tonic::Request<tonic::Streaming<rpc_proto::ImportSqlRequest>>,
	) -> Result<tonic::Response<rpc_proto::ImportSqlResponse>, tonic::Status> {
		let mut incoming_stream = request.into_inner();

		while let Some(request) = incoming_stream.next().await {
			let rpc_proto::ImportSqlRequest {
				statement,
			} = request.map_err(|e| tonic::Status::internal(e.to_string()))?;

			println!("import_sql request: {:?}", statement);
		}

		Ok(tonic::Response::new(rpc_proto::ImportSqlResponse {}))
	}

	async fn export_sql(
		&self,
		request: tonic::Request<rpc_proto::ExportSqlRequest>,
	) -> Result<tonic::Response<Self::ExportSqlStream>, tonic::Status> {
		let export_request = request.into_inner();
		println!("export_sql request: {:?}", export_request);

		let (tx, rx) = tokio::sync::mpsc::channel(1);

		tokio::spawn(async move {
			tx.send(Ok(rpc_proto::ExportSqlResponse {
				statement: "sql".into(),
			}))
			.await
			.unwrap();
		});

		let output_stream = ReceiverStream::new(rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::ExportSqlStream))
	}

	async fn export_ml_model(
		&self,
		request: tonic::Request<rpc_proto::ExportMlModelRequest>,
	) -> Result<tonic::Response<Self::ExportMlModelStream>, tonic::Status> {
		let rpc_proto::ExportMlModelRequest {
			name,
			version,
		} = request.into_inner();

		let (tx, rx) = tokio::sync::mpsc::channel(1);

		tokio::spawn(async move {
			tx.send(Ok(ExportMlModelResponse {
				model: "model".into(),
			}))
			.await
			.unwrap();
		});

		let output_stream = ReceiverStream::new(rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::ExportMlModelStream))
	}

	async fn query(
		&self,
		request: tonic::Request<rpc_proto::QueryRequest>,
	) -> Result<tonic::Response<Self::QueryStream>, tonic::Status> {
		let rpc_proto::QueryRequest {
			// TODO: Pass transaction id to execute.
			txn_id: _,
			query,
			variables,
		} = request.into_inner();

		let (tx, rx) =
			tokio::sync::mpsc::channel::<Result<rpc_proto::QueryResponse, tonic::Status>>(100);

		tokio::spawn(async move {
			tx.send(Ok(rpc_proto::QueryResponse {
				query_index: 0,
				batch_index: 0,
				stats: None,
				error: None,
				values: vec![Value::string("value".to_string())],
			}))
			.await
			.unwrap();
		});

		let output_stream = ReceiverStream::new(rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::QueryStream))
	}

	async fn subscribe(
		&self,
		request: tonic::Request<rpc_proto::SubscribeRequest>,
	) -> std::result::Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
		let rpc_proto::SubscribeRequest {
			query,
			variables,
		} = request.into_inner();

		// Create a channel for the response stream which will be used for sending results and managing the lifecycle of the live query.
		let (response_tx, response_rx) = tokio::sync::mpsc::channel(100);

		// Consume the live stream and send the results to the response channel
		tokio::spawn(async move {
			response_tx
				.send(Ok(rpc_proto::SubscribeResponse {
					notification: Some(rpc_proto::Notification::default()),
				}))
				.await
				.unwrap();
		});

		let output_stream = ReceiverStream::new(response_rx);

		Ok(tonic::Response::new(Box::pin(output_stream) as Self::SubscribeStream))
	}
}

impl TestServer {
	pub async fn serve()
	-> (tonic::transport::Channel, tokio::task::JoinHandle<Result<(), tonic::transport::Error>>) {
		let (client, server) = tokio::io::duplex(10);

		let service = SurrealDbServiceServer::new(TestServer);
		let server_handle = tokio::spawn(async move {
			tonic::transport::Server::builder()
				.add_service(service)
				.serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
				.await
		});

		let mut client = Some(client);
		let channel = tonic::transport::Endpoint::try_from("http://localhost:50052")
			.unwrap()
			.connect_with_connector(tower::service_fn(move |_: http::Uri| {
				let client = client.take();
				async move {
					if let Some(client) = client {
						Ok(hyper_util::rt::TokioIo::new(client))
					} else {
						Err(std::io::Error::other("Client was already taken"))
					}
				}
			}))
			.await
			.unwrap();

		(channel, server_handle)
	}
}
