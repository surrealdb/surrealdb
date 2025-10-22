use std::sync::Arc;

use tokio::sync::Semaphore;
use uuid::Uuid;

use super::{DbResult, Method, RpcError, RpcProtocolV1};
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::types::PublicArray;

//#[cfg(not(target_family = "wasm"))]
//use crate::gql::SchemaCache;

#[expect(async_fn_in_trait)]
pub trait RpcContext {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore;
	/// Retrieves the modification lock for this RPC context
	fn lock(&self) -> Arc<Semaphore>;
	/// The current session for this RPC context
	fn get_session(&self, id: Option<&Uuid>) -> Arc<Session>;
	/// Mutable access to the current session for this RPC context
	fn set_session(&self, id: Option<Uuid>, session: Arc<Session>);
	/// Deletes a session
	fn del_session(&self, id: &Uuid);
	// Lists all sessions
	fn list_sessions(&self) -> Vec<Uuid>;
	/// The version information for this RPC context
	fn version_data(&self) -> DbResult;

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled by default
	const LQ_SUPPORT: bool = false;

	/// Handles the execution of a LIVE statement
	fn handle_live(
		&self,
		_lqid: &Uuid,
		_session_id: Option<Uuid>,
	) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_live function must be implemented if LQ_SUPPORT = true") }
	}
	/// Handles the execution of a KILL statement
	fn handle_kill(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_kill function must be implemented if LQ_SUPPORT = true") }
	}

	/// Handles the cleanup of live queries
	fn cleanup_lqs(
		&self,
		session_id: Option<&Uuid>,
	) -> impl std::future::Future<Output = ()> + Send;
	fn cleanup_all_lqs(&self) -> impl std::future::Future<Output = ()> + Send;

	// ------------------------------
	// GraphQL
	// ------------------------------

	// GraphQL queries are disabled by default
	//#[cfg(not(target_family = "wasm"))]
	//const GQL_SUPPORT: bool = false;

	// Returns the GraphQL schema cache used in GraphQL queries
	//#[cfg(not(target_family = "wasm"))]
	//fn graphql_schema_cache(&self) -> &SchemaCache {
	//unimplemented!("graphql_schema_cache function must be implemented if
	// GQL_SUPPORT = true")
	//}

	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation
	async fn execute(
		&self,
		version: Option<u8>,
		_txn: Option<Uuid>,
		session: Option<Uuid>,
		method: Method,
		params: PublicArray,
	) -> Result<DbResult, RpcError>
	where
		Self: RpcProtocolV1,
	{
		match version {
			Some(1) => RpcProtocolV1::execute(self, session, method, params).await,
			_ => RpcProtocolV1::execute(self, session, method, params).await,
		}
	}
}
