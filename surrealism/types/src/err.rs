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
		"No Surrealism controller became available within {waited:?}. The module's pool holds \
		 {pool_size} controller(s), all of them checked out. A function that calls back into its \
		 own module needs a free slot for the nested call while the outer one is still holding \
		 its own; raise the module's `max_pool_size` above the call depth, or remove the \
		 re-entrant call."
	)]
	ControllerPoolExhausted {
		waited: Duration,
		pool_size: usize,
	},
	#[error(
		"Unsupported ABI version: expected {expected}, got {got}. Rebuild the extension with the \
		 SurrealDB 4.0 SDK"
	)]
	UnsupportedAbi {
		expected: u32,
		got: u32,
	},
	#[error("Integer conversion error: {0}")]
	IntConversion(#[from] TryFromIntError),
	#[cfg(feature = "host")]
	#[error("Wasmtime error: {0}")]
	Wasmtime(#[from] wasmtime::Error),
	#[error("Other error: {0}")]
	Other(#[from] anyhow::Error),
}

impl SurrealismError {
	/// Whether this error represents a WASM trap — an unrecoverable fault
	/// during execution that leaves the instance in an undefined state.
	///
	/// After a trap the controller must be dropped (not returned to the pool).
	/// Non-trap errors (guest `Err` returns, host-side encoding failures, etc.)
	/// go through normal control flow and leave the instance reusable.
	pub fn is_trap(&self) -> bool {
		match self {
			// Epoch interrupt mid-execution — WASM state is undefined.
			Self::Timeout {
				..
			} => true,
			// Instantiation failure — no usable instance exists.
			#[cfg(feature = "host")]
			Self::Instantiation(_) => true,
			// Wasmtime error from call_async (actual trap) or typed() (type
			// mismatch). Conservative: treat all as traps since the instance
			// may be in an unknown state.
			#[cfg(feature = "host")]
			Self::Wasmtime(_) => true,
			// Compilation failure — no instance was ever created.
			#[cfg(feature = "host")]
			Self::Compilation(_) => false,
			// Guest returned Err through normal control flow.
			Self::FunctionCallError(_) => false,
			// Host-side errors (encoding, decoding, config) — no WASM ran.
			Self::UnsupportedAbi {
				..
			} => false,
			Self::IntConversion(_) => false,
			// No controller was ever obtained, so no instance ran.
			Self::ControllerPoolExhausted {
				..
			} => false,
			Self::Other(_) => false,
		}
	}
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
