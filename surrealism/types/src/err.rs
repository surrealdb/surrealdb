//! Error handling utilities for adding context to errors.

use std::fmt::Display;
use std::num::TryFromIntError;

#[derive(thiserror::Error, Debug)]
pub enum SurrealismError {
	#[error("WASM compilation failed: {0}")]
	Compilation(wasmtime::Error),
	#[error("Memory allocation failed")]
	AllocFailed,
	#[error("Memory deallocation failed")]
	FreeFailed,
	#[error("Memory out of bounds: {0}")]
	OutOfBounds(String),
	#[error("Function call error: {0}")]
	FunctionCallError(String),
	#[error("Integer conversion error: {0}")]
	IntConversion(#[from] TryFromIntError),
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
