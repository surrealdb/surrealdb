//! Memory transfer traits for moving data across WASM boundaries.
//!
//! All transferred data follows this layout:
//! ```text
//! [4-byte length (u32, LE)][data bytes]
//! ```

use std::ops::{Deref, DerefMut};

use anyhow::Result;
#[cfg(feature = "host")]
use async_trait::async_trait;

#[cfg(feature = "host")]
use crate::controller::AsyncMemoryController;
use crate::controller::MemoryController;

/// Synchronous transfer for the WASM guest side.
///
/// `transfer()` serializes, allocates, writes, and returns a pointer.
/// `receive()` reads, deserializes, and frees.
pub trait Transfer {
	fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr>;

	fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self>
	where
		Self: Sized;
}

/// Asynchronous transfer for the host side (wasmtime).
#[cfg(feature = "host")]
#[async_trait]
pub trait AsyncTransfer: Send {
	async fn transfer(self, controller: &mut dyn AsyncMemoryController) -> Result<Ptr>;

	async fn receive(ptr: Ptr, controller: &mut dyn AsyncMemoryController) -> Result<Self>
	where
		Self: Sized;
}

/// Type-safe wrapper around a WASM memory pointer (`u32`).
pub struct Ptr(u32);

impl Deref for Ptr {
	type Target = u32;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Ptr {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl From<u32> for Ptr {
	fn from(ptr: u32) -> Self {
		Ptr(ptr)
	}
}

impl From<Ptr> for u32 {
	fn from(ptr: Ptr) -> Self {
		ptr.0
	}
}

impl TryFrom<i32> for Ptr {
	type Error = anyhow::Error;

	fn try_from(value: i32) -> Result<Self> {
		if value < 0 {
			Err(anyhow::anyhow!("Invalid pointer: {}", value))
		} else {
			Ok(Ptr(value as u32))
		}
	}
}
