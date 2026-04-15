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
	#[error("Unsupported ABI version: expected {expected}, got {got}")]
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
