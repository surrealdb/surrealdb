mod args;
mod context;
mod error;
mod method;
mod protocol;
mod response;

pub mod format;
pub mod request;
pub(crate) mod statement_options;

pub use context::RpcContext;
pub use error::RpcError;
pub use format::Format;
pub use method::Method;
pub use protocol::v1::RpcProtocolV1;
pub use protocol::v2::RpcProtocolV2;
pub use request::Request;
pub use response::Data;

use crate::cnf::PROTECTED_PARAM_NAMES;

pub fn check_protected_param(key: &str) -> Result<(), RpcError> {
	if PROTECTED_PARAM_NAMES.contains(&key) {
		return Err(RpcError::InvalidParams(format!("Cannot set protected variable: {key}")));
	}
	Ok(())
}
