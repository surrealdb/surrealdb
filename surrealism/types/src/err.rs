//! Error handling utilities for prefixing errors with context.

use std::fmt::Display;
use std::num::TryFromIntError;
use std::time::Duration;

#[derive(thiserror::Error, Debug)]
pub enum SurrealismError {
	#[cfg(feature = "host")]
	#[error("WASM compilation failed: {0}")]
	Compilation(wasmtime::Error),
	#[cfg(feature = "host")]
	#[error("WASM instantiation failed: {0}")]
	Instantiation(wasmtime::Error),
	#[error("Function call error: {0}")]
	FunctionCallError(String),
	#[error(
		"Module execution timed out (epoch interrupt). Effective timeout: {effective:?}, context timeout: {context_timeout:?}, module limit: {module_limit:?}"
	)]
	Timeout {
		effective: Option<Duration>,
		context_timeout: Option<Duration>,
		module_limit: Option<Duration>,
	},
	#[error(
		"No Surrealism invocation slot became free within {waited:?}; all {slots} are in use. \
		 Raise SURREAL_SURREALISM_MAX_CONCURRENT_INVOCATIONS, or give the query a longer timeout."
	)]
	NoInvocationSlot {
		waited: Duration,
		slots: usize,
	},
	#[error(
		"A nested Surrealism invocation found none of the {slots} invocation slots free. A call \
		 made from inside an extension never waits for a slot, because the calls holding them may \
		 be waiting on it."
	)]
	NestedInvocationNoSlot {
		slots: usize,
	},
	#[error(
		"Unsupported ABI version {got}: this server runs ABI {oldest} to {latest}. Rebuild the \
		 extension with an SDK for one of them"
	)]
	UnsupportedAbi {
		got: u32,
		oldest: u32,
		latest: u32,
	},
	#[error("Integer conversion error: {0}")]
	IntConversion(#[from] TryFromIntError),
	#[cfg(feature = "host")]
	#[error("Wasmtime error: {0}")]
	Wasmtime(#[from] wasmtime::Error),
	#[error("Other error: {0}")]
	Other(#[from] anyhow::Error),
}

pub type SurrealismResult<T> = std::result::Result<T, SurrealismError>;

pub trait PrefixErr<T> {
	fn prefix_err<F, S>(self, f: F) -> SurrealismResult<T>
	where
		F: FnOnce() -> S,
		S: Display;
}

impl<T, E: Display> PrefixErr<T> for std::result::Result<T, E> {
	fn prefix_err<F, S>(self, f: F) -> SurrealismResult<T>
	where
		F: FnOnce() -> S,
		S: Display,
	{
		self.map_err(|e| SurrealismError::Other(anyhow::anyhow!("{}: {}", f(), e)))
	}
}

/// Why a host call made through a `transaction` handle failed.
///
/// Mirrors the `host-error` variant in the WIT. Classifying these lets the host
/// count capability denials separately from a plugin's own errors, and lets a
/// plugin match on the class instead of parsing a message.
///
/// A denied call never reaches the datastore, so this is an observability and
/// ergonomics boundary rather than a security one.
#[derive(thiserror::Error, Debug)]
pub enum HostCallError {
	/// The module's declared capabilities forbid this call.
	#[error("permission denied: {0}")]
	PermissionDenied(String),
	/// The query or function name did not parse or resolve.
	#[error("invalid request: {0}")]
	InvalidRequest(String),
	/// The datastore refused or failed the operation. A write attempted through
	/// a read-only transaction arrives here.
	#[error("datastore error: {0}")]
	Datastore(String),
	/// The host's deadline elapsed while serving this call.
	#[error("deadline exceeded")]
	DeadlineExceeded,
	/// Anything else, including serialization failures.
	#[error("{0}")]
	Other(String),
}

/// Unclassified failures become [`HostCallError::Other`], so an implementation
/// can keep using `?` on `anyhow` errors and classify only where it matters.
impl From<anyhow::Error> for HostCallError {
	fn from(e: anyhow::Error) -> Self {
		Self::Other(e.to_string())
	}
}

/// Convenience alias for a host call's result.
pub type HostCallResult<T> = Result<T, HostCallError>;
