//! Different embedded and remote database engines

pub mod any;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
))]
pub mod local;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
	feature = "protocol-http",
	feature = "protocol-ws",
))]
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

struct IntervalStream {
	inner: Interval,
}

impl IntervalStream {
	fn new(interval: Interval) -> Self {
		Self {
			inner: interval,
		}
	}
}

impl Stream for IntervalStream {
	type Item = Instant;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Instant>> {
		self.inner.poll_tick(cx).map(Some)
	}
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
	feature = "protocol-http",
	feature = "protocol-ws",
))]
pub(crate) enum SessionError {
	NotFound(Uuid),
	Remote(String),
}

/// Convert a session error into the public error type.
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
	feature = "protocol-http",
	feature = "protocol-ws",
))]
pub(crate) fn session_error_to_error(e: SessionError) -> surrealdb_types::Error {
	use surrealdb_types::{Error as TypesError, NotFoundError};
	match e {
		SessionError::NotFound(id) => TypesError::not_found(
			format!("Session not found: {id}"),
			NotFoundError::Session {
				id: Some(id.to_string()),
			},
		),
		SessionError::Remote(msg) => TypesError::internal(msg),
	}
}
