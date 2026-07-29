mod error;
mod protocol;

pub mod format;

pub use error::{
	bad_gql_config, bad_lq_config, deserialize, internal_error, invalid_params, invalid_request,
	lq_not_supported, method_not_allowed, method_not_found, parse_error, query_timeout_error,
	serialize, session_exists, session_expired, session_not_found, thrown, too_many_transactions,
	types_error_from_anyhow,
};
pub use format::Format;
pub use protocol::RpcProtocol;
use surrealdb_cnf::PROTECTED_PARAM_NAMES;
pub use surrealdb_rpc::{DbResponse, DbResult, DbResultStats, Method, Request, args, request};
use surrealdb_types::{Error as TypesError, SurrealValue};

/// Decode a binary RPC response payload into a [`DbResponse`].
///
/// Lives in core (rather than as an inherent method on `DbResponse`) because it
/// depends on the flatbuffers decoder in [`format`], which stays in core.
pub fn db_response_from_bytes(bytes: &[u8]) -> Result<DbResponse, TypesError> {
	let value =
		format::flatbuffers::decode(bytes).map_err(|e| TypesError::internal(e.to_string()))?;
	DbResponse::from_value(value)
}

pub fn check_protected_param(key: &str) -> Result<(), surrealdb_types::Error> {
	if PROTECTED_PARAM_NAMES.contains(&key) {
		return Err(invalid_params(format!("Cannot set protected variable: {key}")));
	}
	Ok(())
}
