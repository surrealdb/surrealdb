use anyhow::Result;
#[cfg(feature = "host")]
use async_trait::async_trait;

// Guest side (sync trait for WASM guest code)
pub trait MemoryController {
	fn alloc(&mut self, len: u32, align: u32) -> Result<u32>;
	fn free(&mut self, ptr: u32, len: u32) -> Result<()>;
	fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8];
}

// Host side (async trait for Wasmtime runtime)
#[cfg(feature = "host")]
#[async_trait]
pub trait AsyncMemoryController: Send {
	async fn alloc(&mut self, len: u32, align: u32) -> Result<u32>;
	async fn free(&mut self, ptr: u32, len: u32) -> Result<()>;
	fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8];
}
