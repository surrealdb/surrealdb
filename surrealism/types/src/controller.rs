//! Memory management abstractions for WASM linear memory.
//!
//! Provides synchronous (guest-side) and asynchronous (host-side) interfaces
//! for allocating, freeing, and accessing regions of WASM linear memory.

use anyhow::Result;
#[cfg(feature = "host")]
use async_trait::async_trait;

#[cfg(feature = "host")]
use crate::err::SurrealismResult;

/// Synchronous memory controller for the WASM guest side.
pub trait MemoryController {
	fn alloc(&mut self, len: u32) -> Result<u32>;
	fn free(&mut self, ptr: u32, len: u32) -> Result<()>;
	fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8];
}

/// Asynchronous memory controller for the host side (wasmtime).
///
/// Alloc/free are async because they call back into WASM exports.
/// `mut_mem` is sync since it only accesses the linear memory buffer.
#[cfg(feature = "host")]
#[async_trait]
pub trait AsyncMemoryController: Send {
	async fn alloc(&mut self, len: u32) -> SurrealismResult<u32>;
	async fn free(&mut self, ptr: u32, len: u32) -> SurrealismResult<()>;
	fn mut_mem(&mut self, ptr: u32, len: u32) -> SurrealismResult<&mut [u8]>;
}
