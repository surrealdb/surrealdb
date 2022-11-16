#[cfg(feature = "ws")]
use crate::protocol::ws::Failure;
use serde::Deserialize;
use serde::Serialize;
use std::error;
use std::fmt;

/// Categories of errors returned by the client
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
	/// Tried to use a connection before it's initialized
	ConnectionUninitialized,
	/// Tried to use a request ID already being used by another query
	DuplicateRequestId,
	/// The query returned an error
	Query,
	/// Parse error
	ParseError,
	/// Tried to use a range query on an unsupported resource
	RangeUnsupported,
	/// Socket error
	Socket,
	/// Syntax unsupported
	SyntaxUnsupported,
	/// Invalid request
	InvalidRequest,
	/// Invalid params
	InvalidParams,
	/// Internal error
	InternalError,
	/// Deserialization error
	Deserialization,
	/// Serialization error
	Serialization,
}

impl ErrorKind {
	/// Sets a message on an error kind
	pub fn with_message(self, message: impl Into<String>) -> Error {
		Error {
			kind: self,
			message: message.into(),
		}
	}

	/// Constructs an error from an error kind and context
	pub fn with_context(self, context: impl fmt::Display) -> Error {
		let message = match self {
			ErrorKind::DuplicateRequestId => {
				format!("request ID {context} is already being used by another query")
			}
			ErrorKind::Query => format!("failed to perform query; {context}"),
			ErrorKind::RangeUnsupported => format!("range not supported for {context}"),
			ErrorKind::Socket => format!("socket error; {context}"),
			ErrorKind::SyntaxUnsupported => format!("{context} syntax is not supported"),
			_ => context.to_string(),
		};
		Error {
			kind: self,
			message,
		}
	}
}

/// Error type returned by the client
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Error {
	kind: ErrorKind,
	message: String,
}

impl Error {
	/// Returns the kind of an error
	pub const fn kind(&self) -> ErrorKind {
		self.kind
	}
}

impl error::Error for Error {}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.message)
	}
}

#[cfg(feature = "ws")]
impl From<Failure> for Error {
	fn from(failure: Failure) -> Self {
		match failure.code {
			-32600 => ErrorKind::InvalidRequest.with_message(failure.message),
			-32602 => ErrorKind::InvalidParams.with_message(failure.message),
			-32603 => ErrorKind::InternalError.with_message(failure.message),
			-32700 => ErrorKind::ParseError.with_message(failure.message),
			_ => ErrorKind::Query.with_message(failure.message),
		}
	}
}

impl From<serde_pack::decode::Error> for Error {
	fn from(error: serde_pack::decode::Error) -> Self {
		ErrorKind::Deserialization.with_context(error)
	}
}

impl From<serde_pack::encode::Error> for Error {
	fn from(error: serde_pack::encode::Error) -> Self {
		ErrorKind::Serialization.with_context(error)
	}
}

#[cfg(all(feature = "ws", not(target_arch = "wasm32")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "ws", not(target_arch = "wasm32")))))]
impl From<tokio_tungstenite::tungstenite::Error> for Error {
	fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
		ErrorKind::Socket.with_context(error)
	}
}

impl From<surrealdb::Error> for Error {
	fn from(error: surrealdb::Error) -> Self {
		ErrorKind::Query.with_context(error)
	}
}

impl From<semver::Error> for Error {
	fn from(error: semver::Error) -> Self {
		ErrorKind::ParseError.with_context(error)
	}
}

impl<T> From<flume::SendError<T>> for Error {
	fn from(error: flume::SendError<T>) -> Self {
		ErrorKind::Socket.with_context(error)
	}
}

impl From<flume::RecvError> for Error {
	fn from(error: flume::RecvError) -> Self {
		ErrorKind::Socket.with_context(error)
	}
}

impl From<url::ParseError> for Error {
	fn from(error: url::ParseError) -> Self {
		ErrorKind::ParseError.with_context(error)
	}
}

impl From<std::io::Error> for Error {
	fn from(error: std::io::Error) -> Self {
		ErrorKind::InternalError.with_context(error)
	}
}

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
impl From<reqwest::Error> for Error {
	fn from(error: reqwest::Error) -> Self {
		ErrorKind::Socket.with_context(error)
	}
}

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
impl From<serde_json::Error> for Error {
	fn from(error: serde_json::Error) -> Self {
		ErrorKind::Socket.with_context(error)
	}
}

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
impl From<reqwest::header::ToStrError> for Error {
	fn from(error: reqwest::header::ToStrError) -> Self {
		ErrorKind::ParseError.with_context(error)
	}
}

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
impl From<reqwest::header::InvalidHeaderValue> for Error {
	fn from(error: reqwest::header::InvalidHeaderValue) -> Self {
		ErrorKind::ParseError.with_context(error)
	}
}

#[cfg(all(feature = "ws", target_arch = "wasm32"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "ws", target_arch = "wasm32"))))]
impl From<ws_stream_wasm::WsErr> for Error {
	fn from(error: ws_stream_wasm::WsErr) -> Self {
		ErrorKind::Socket.with_context(error)
	}
}

#[cfg(all(feature = "ws", target_arch = "wasm32"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "ws", target_arch = "wasm32"))))]
impl From<pharos::PharErr> for Error {
	fn from(error: pharos::PharErr) -> Self {
		ErrorKind::Socket.with_context(error)
	}
}
