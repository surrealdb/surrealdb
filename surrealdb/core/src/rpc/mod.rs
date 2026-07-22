mod args;
mod error;
mod method;
mod protocol;
mod response;

pub mod format;
pub mod request;

pub use error::{
	bad_gql_config, bad_lq_config, deserialize, internal_error, invalid_params, invalid_request,
	lq_not_supported, method_not_allowed, method_not_found, parse_error, serialize, session_exists,
	session_expired, session_not_found, thrown, too_many_transactions, types_error_from_anyhow,
};
pub use format::Format;
pub use method::Method;
pub use protocol::RpcProtocol;
pub use request::Request;
pub use response::{DbResponse, DbResult, DbResultStats};

use crate::cnf::PROTECTED_PARAM_NAMES;

pub fn check_protected_param(key: &str) -> Result<(), surrealdb_types::Error> {
	if PROTECTED_PARAM_NAMES.contains(&key) {
		return Err(invalid_params(format!("Cannot set protected variable: {key}")));
	}
	Ok(())
}
