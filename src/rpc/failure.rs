use std::borrow::Cow;

use revision::{Revisioned, revisioned};
use serde::Serialize;

use crate::core::rpc::RpcError;
use crate::core::val::Value;

#[derive(Clone, Debug, Serialize)]
pub struct Failure {
	pub(crate) code: i64,
	pub(crate) message: Cow<'static, str>,
}

impl Failure {
	pub fn into_value(self) -> Value {
		map! {
			String::from("code") => Value::from(self.code),
			String::from("message") => Value::from(self.message.to_string()),
		}
		.into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Serialize)]
struct Inner {
	code: i64,
	message: String,
}

impl Revisioned for Failure {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		let inner = Inner {
			code: self.code,
			message: self.message.as_ref().to_owned(),
		};
		inner.serialize_revisioned(writer)
	}

	fn deserialize_revisioned<R: std::io::Read>(_reader: &mut R) -> Result<Self, revision::Error> {
		unreachable!("deserialization not supported for this type")
	}

	fn revision() -> u16 {
		1
	}
}

impl From<&str> for Failure {
	fn from(err: &str) -> Self {
		Failure::custom(err.to_string())
	}
}

impl From<RpcError> for Failure {
	fn from(err: RpcError) -> Self {
		match err {
			RpcError::ParseError => Failure::PARSE_ERROR,
			RpcError::InvalidRequest => Failure::INVALID_REQUEST,
			RpcError::MethodNotFound => Failure::METHOD_NOT_FOUND,
			RpcError::InvalidParams(_) => Failure::INVALID_PARAMS,
			RpcError::InternalError(_) => Failure::custom(err.to_string()),
			RpcError::Thrown(_) => Failure::custom(err.to_string()),
			_ => Failure::custom(err.to_string()),
		}
	}
}

impl Failure {
	pub const PARSE_ERROR: Failure = Failure {
		code: -32700,
		message: Cow::Borrowed("Parse error"),
	};

	pub const INVALID_REQUEST: Failure = Failure {
		code: -32600,
		message: Cow::Borrowed("Invalid Request"),
	};

	pub const METHOD_NOT_FOUND: Failure = Failure {
		code: -32601,
		message: Cow::Borrowed("Method not found"),
	};

	pub const INVALID_PARAMS: Failure = Failure {
		code: -32602,
		message: Cow::Borrowed("Invalid params"),
	};

	/*
	pub const INTERNAL_ERROR: Failure = Failure {
		code: -32603,
		message: Cow::Borrowed("Internal error"),
	};
	*/

	pub fn custom<S>(message: S) -> Failure
	where
		Cow<'static, str>: From<S>,
	{
		Failure {
			code: -32000,
			message: message.into(),
		}
	}
}
