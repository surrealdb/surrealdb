use std::sync::Arc;

use dashmap::DashMap;
use futures::stream::BoxStream;
use surrealdb_core::{dbs::Session, gql::{Pessimistic, SchemaCache}, kvs::Datastore};
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use arc_swap::ArcSwap;


// TODO: Will need to map grpc subscription stream ID to their actual streams.
// type Client

/// Mapping of LIVE Query ID to grpc subscription stream ID.
type LiveQueries = DashMap<Uuid, Uuid>;

#[derive(Default)]
pub struct ConnectionsState {
    pub(crate) live_queries: LiveQueries,
}


pub struct SurrealDBGrpcService {
    /// The unique id of this gRPC connection
    pub(crate) id: Uuid,
    /// The system state for all gRPC connections
    pub(crate) state: Arc<ConnectionsState>,
    /// The datastore accessible to all gRPC connections
    pub(crate) datastore: Arc<Datastore>,
    /// The persistent session for this gRPC connection
    pub(crate) session: ArcSwap<Session>,
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
		session: Session,
		datastore: Arc<Datastore>,
		state: Arc<ConnectionsState>,
        canceller: CancellationToken,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            id,
            state,
            session: ArcSwap::from(Arc::new(session)),
            shutdown,
            canceller,
            gql_schema: SchemaCache::new(Arc::clone(&datastore)),
            datastore,
        }
    }
}

#[tonic::async_trait]
impl rpc_proto::surreal_db_service_server::SurrealDbService for SurrealDBGrpcService {
    type QueryStream = BoxStream<'static, Result<rpc_proto::QueryResponse, tonic::Status>>;
    type LiveStream = BoxStream<'static, Result<rpc_proto::LiveResponse, tonic::Status>>;

    async fn health(
        &self,
        request: tonic::Request<rpc_proto::HealthRequest>,
    ) -> Result<tonic::Response<rpc_proto::HealthResponse>, tonic::Status> {
        todo!()
    }

    async fn version(
        &self,
        request: tonic::Request<rpc_proto::VersionRequest>,
    ) -> Result<tonic::Response<rpc_proto::VersionResponse>, tonic::Status> {
        todo!()
    }

    async fn info(
        &self,
        request: tonic::Request<rpc_proto::InfoRequest>,
    ) -> Result<tonic::Response<rpc_proto::InfoResponse>, tonic::Status> {
        todo!()
    }

    async fn r#use(
        &self,
        request: tonic::Request<rpc_proto::UseRequest>,
    ) -> Result<tonic::Response<rpc_proto::UseResponse>, tonic::Status> {
        todo!()
    }

    async fn signup(
        &self,
        request: tonic::Request<rpc_proto::SignupRequest>,
    ) -> Result<tonic::Response<rpc_proto::SignupResponse>, tonic::Status> {
        todo!()
    }

    async fn signin(
        &self,
        request: tonic::Request<rpc_proto::SigninRequest>,
    ) -> Result<tonic::Response<rpc_proto::SigninResponse>, tonic::Status> {
        todo!()
    }

    async fn authenticate(
        &self,
        request: tonic::Request<rpc_proto::AuthenticateRequest>,
    ) -> Result<tonic::Response<rpc_proto::AuthenticateResponse>, tonic::Status> {
        todo!()
    }

    async fn invalidate(
        &self,
        request: tonic::Request<rpc_proto::InvalidateRequest>,
    ) -> Result<tonic::Response<rpc_proto::InvalidateResponse>, tonic::Status> {
        todo!()
    }

    async fn reset(
        &self,
        request: tonic::Request<rpc_proto::ResetRequest>,
    ) -> Result<tonic::Response<rpc_proto::ResetResponse>, tonic::Status> {
        todo!()
    }

    async fn kill(
        &self,
        request: tonic::Request<rpc_proto::KillRequest>,
    ) -> Result<tonic::Response<rpc_proto::KillResponse>, tonic::Status> {
        todo!()
    }

    async fn live(
        &self,
        request: tonic::Request<rpc_proto::LiveRequest>,
    ) -> std::result::Result<tonic::Response<Self::LiveStream>, tonic::Status> {
        todo!()
    }

    async fn set(
        &self,
        request: tonic::Request<rpc_proto::SetRequest>,
    ) -> Result<tonic::Response<rpc_proto::SetResponse>, tonic::Status> {
        todo!()
    }

    async fn unset(
        &self,
        request: tonic::Request<rpc_proto::UnsetRequest>,
    ) -> Result<tonic::Response<rpc_proto::UnsetResponse>, tonic::Status> {
        todo!()
    }

    async fn query(
        &self,
        request: tonic::Request<rpc_proto::QueryRequest>,
    ) -> Result<tonic::Response<Self::QueryStream>, tonic::Status> {
        todo!()
    }

    async fn select(
        &self,
        request: tonic::Request<rpc_proto::SelectRequest>,
    ) -> Result<tonic::Response<rpc_proto::SelectResponse>, tonic::Status> {
        todo!()
    }

    async fn insert(
        &self,
        request: tonic::Request<rpc_proto::InsertRequest>,
    ) -> Result<tonic::Response<rpc_proto::InsertResponse>, tonic::Status> {
        todo!()
    }

    async fn create(
        &self,
        request: tonic::Request<rpc_proto::CreateRequest>,
    ) -> Result<tonic::Response<rpc_proto::CreateResponse>, tonic::Status> {
        todo!()
    }

    async fn upsert(
        &self,
        request: tonic::Request<rpc_proto::UpsertRequest>,
    ) -> Result<tonic::Response<rpc_proto::UpsertResponse>, tonic::Status> {
        todo!()
    }

    async fn update(
        &self,
        request: tonic::Request<rpc_proto::UpdateRequest>,
    ) -> Result<tonic::Response<rpc_proto::UpdateResponse>, tonic::Status> {
        todo!()
    }

    async fn delete(
        &self,
        request: tonic::Request<rpc_proto::DeleteRequest>,
    ) -> Result<tonic::Response<rpc_proto::DeleteResponse>, tonic::Status> {
        todo!()
    }

    async fn relate(
        &self,
        request: tonic::Request<rpc_proto::RelateRequest>,
    ) -> Result<tonic::Response<rpc_proto::RelateResponse>, tonic::Status> {
        todo!()
    }

    async fn run_function(
        &self,
        request: tonic::Request<rpc_proto::RunFunctionRequest>,
    ) -> Result<tonic::Response<rpc_proto::RunFunctionResponse>, tonic::Status> {
        todo!()
    }

    async fn graphql(
        &self,
        request: tonic::Request<rpc_proto::GraphqlRequest>,
    ) -> Result<tonic::Response<rpc_proto::GraphqlResponse>, tonic::Status> {
        todo!()
    }

    
    
}
