mod args;
mod context;
mod error;
mod method;
mod protocol;
mod response;

pub mod format;
pub mod request;

pub use context::RpcContext;
pub use error::RpcError;
pub use format::Format;
pub use method::Method;
pub use protocol::v1::RpcProtocolV1;
pub use request::Request;
pub use response::{DbResponse, DbResult, DbResultError, DbResultStats};

use crate::cnf::PROTECTED_PARAM_NAMES;

pub fn check_protected_param(key: &str) -> Result<(), RpcError> {
	if PROTECTED_PARAM_NAMES.contains(&key) {
		return Err(RpcError::InvalidParams(format!("Cannot set protected variable: {key}")));
	}
	Ok(())
}
