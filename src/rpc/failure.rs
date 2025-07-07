use revision::Revisioned;
use revision::revisioned;
use serde::Serialize;
use std::borrow::Cow;
use surrealdb::rpc::RpcError;
use surrealdb_core::expr::Value;

// #[derive(Clone, Debug, Serialize)]
// pub struct Failure {
// 	pub(crate) code: i64,
// 	pub(crate) message: Cow<'static, str>,
// }

// impl From<&str> for Failure {
// 	fn from(err: &str) -> Self {
// 		Failure::custom(err.to_string())
// 	}
// }

// impl From<RpcError> for Failure {
// 	fn from(err: RpcError) -> Self {
// 		match err {
// 			RpcError::ParseError => Failure::PARSE_ERROR,
// 			RpcError::InvalidRequest(_) => Failure::INVALID_REQUEST,
// 			RpcError::MethodNotFound => Failure::METHOD_NOT_FOUND,
// 			RpcError::InvalidParams => Failure::INVALID_PARAMS,
// 			RpcError::InternalError(_) => Failure::custom(err.to_string()),
// 			RpcError::Thrown(_) => Failure::custom(err.to_string()),
// 			_ => Failure::custom(err.to_string()),
// 		}
// 	}
// }

// impl From<Failure> for Value {
// 	fn from(err: Failure) -> Self {
// 		map! {
// 			String::from("code") => Value::from(err.code),
// 			String::from("message") => Value::from(err.message.to_string()),
// 		}
// 		.into()
// 	}
// }

// impl Failure {
// 	pub const PARSE_ERROR: Failure = Failure {
// 		code: -32700,
// 		message: Cow::Borrowed("Parse error"),
// 	};

// 	pub const INVALID_REQUEST: Failure = Failure {
// 		code: -32600,
// 		message: Cow::Borrowed("Invalid Request"),
// 	};

// 	pub const METHOD_NOT_FOUND: Failure = Failure {
// 		code: -32601,
// 		message: Cow::Borrowed("Method not found"),
// 	};

// 	pub const INVALID_PARAMS: Failure = Failure {
// 		code: -32602,
// 		message: Cow::Borrowed("Invalid params"),
// 	};

// 	pub const INTERNAL_ERROR: Failure = Failure {
// 		code: -32603,
// 		message: Cow::Borrowed("Internal error"),
// 	};

// 	pub fn custom<S>(message: S) -> Failure
// 	where
// 		Cow<'static, str>: From<S>,
// 	{
// 		Failure {
// 			code: -32000,
// 			message: message.into(),
// 		}
// 	}
// }
