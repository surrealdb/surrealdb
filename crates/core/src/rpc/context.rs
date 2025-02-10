#[cfg(all(not(target_family = "wasm"), surrealdb_unstable))]
use crate::gql::SchemaCache;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::Data;
use super::Method;
use super::RpcError;
use super::RpcProtocolV1;
use super::RpcProtocolV2;
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::sql::Array;

#[allow(async_fn_in_trait)]
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
	fn version_data(&self) -> Data;

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
	#[cfg(all(not(target_family = "wasm"), surrealdb_unstable))]
	const GQL_SUPPORT: bool = false;

	/// Returns the GraphQL schema cache used in GraphQL queries
	#[cfg(all(not(target_family = "wasm"), surrealdb_unstable))]
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
		method: Method,
		params: Array,
	) -> Result<Data, RpcError>
	where
		Self: RpcProtocolV1,
		Self: RpcProtocolV2,
	{
		match version {
			Some(1) => RpcProtocolV1::execute(self, method, params).await,
			Some(2) => RpcProtocolV2::execute(self, method, params).await,
			_ => RpcProtocolV1::execute(self, method, params).await,
		}
	}
}
