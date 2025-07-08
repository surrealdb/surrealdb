#[cfg(not(target_family = "wasm"))]
use crate::gql::SchemaCache;
use crate::rpc::response::V1Data;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::Method;
use super::RpcError;
use super::RpcProtocolV1;
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::rpc::protocol::v1::types::V1Array;

#[expect(async_fn_in_trait)]
pub trait RpcContext {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore;
	/// Retrieves the modification lock for this RPC context
	fn lock(&self) -> Arc<Semaphore>;
	/// The current session for this RPC context
	fn session(&self) -> Arc<Session>;
	/// Mutable access to the current session for this RPC context
	fn set_session(&self, session: Arc<Session>);
	/// The version information for this RPC context
	fn version_data(&self) -> V1Data;

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled by default
	const LQ_SUPPORT: bool = false;

	/// Handles the execution of a LIVE statement
	fn handle_live(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_live function must be implemented if LQ_SUPPORT = true") }
	}
	/// Handles the execution of a KILL statement
	fn handle_kill(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_kill function must be implemented if LQ_SUPPORT = true") }
	}
	/// Handles the cleanup of live queries
	fn cleanup_lqs(&self) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("cleanup_lqs function must be implemented if LQ_SUPPORT = true") }
	}

	// ------------------------------
	// GraphQL
	// ------------------------------

	/// GraphQL queries are disabled by default
	#[cfg(not(target_family = "wasm"))]
	const GQL_SUPPORT: bool = false;

	/// Returns the GraphQL schema cache used in GraphQL queries
	#[cfg(not(target_family = "wasm"))]
	fn graphql_schema_cache(&self) -> &SchemaCache {
		unimplemented!("graphql_schema_cache function must be implemented if GQL_SUPPORT = true")
	}

	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation
	async fn execute(
		&self,
		version: Option<u8>,
		_txn: Option<Uuid>,
		method: Method,
		params: V1Array,
	) -> Result<V1Data, RpcError>
	where
		Self: RpcProtocolV1,
	{
		match version {
			Some(1) => RpcProtocolV1::execute(self, method, params).await,
			_ => Err(RpcError::Thrown(format!("Unsupported RPC version: {version:?}"))),
		}
	}
}
