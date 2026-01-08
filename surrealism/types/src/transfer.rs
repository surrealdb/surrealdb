//! Memory transfer traits for moving data across WASM boundaries.
//!
//! This module provides the core abstractions for transferring data between the WASM guest
//! and the host runtime. It defines synchronous (guest-side) and asynchronous (host-side)
//! traits for managing the memory transfer protocol.
//!
//! # Architecture
//!
//! The transfer protocol works in two phases:
//!
//! 1. **Transfer** (into WASM memory):
//!    - Allocate memory in WASM linear memory
//!    - Write serialized data into allocated region
//!    - Return a pointer to the data
//!
//! 2. **Receive** (from WASM memory):
//!    - Read data from WASM linear memory using a pointer
//!    - Deserialize the data
//!    - Free the allocated memory
//!
//! # Memory Layout
//!
//! All transferred data follows this format:
//! ```text
//! [4-byte length (u32, LE)][data bytes]
//! ```
//!
//! The length prefix allows the receiver to know how much memory to read before
//! deserializing the actual data.

use std::ops::{Deref, DerefMut};

use anyhow::Result;
#[cfg(feature = "host")]
use async_trait::async_trait;

#[cfg(feature = "host")]
use crate::controller::AsyncMemoryController;
use crate::controller::MemoryController;

/// Synchronous trait for transferring data across WASM boundaries (guest side).
///
/// This trait is used on the WASM guest side where all operations are synchronous.
/// It handles both sending data to the host (`transfer`) and receiving data from
/// the host (`receive`).
///
/// # Memory Management
///
/// Implementations must properly manage memory:
/// - `transfer()` allocates and writes data, returning a pointer
/// - `receive()` reads and deallocates data from a pointer
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::{Transfer, MemoryController};
///
/// fn send_data(data: String, controller: &mut dyn MemoryController) -> u32 {
///     let ptr = data.transfer(controller).unwrap();
///     *ptr
/// }
/// ```
pub trait Transfer {
	/// Transfer this value into WASM linear memory.
	///
	/// This method serializes the value, allocates memory in the WASM linear memory,
	/// writes the serialized data, and returns a pointer to the allocated region.
	///
	/// # Parameters
	///
	/// - `controller`: Memory controller for allocation and access
	///
	/// # Returns
	///
	/// A [`Ptr`] pointing to the allocated memory region containing the serialized data.
	///
	/// # Errors
	///
	/// Returns an error if allocation fails or serialization fails.
	fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr>;

	/// Receive a value from WASM linear memory.
	///
	/// This method reads serialized data from the given pointer, deserializes it,
	/// and frees the allocated memory.
	///
	/// # Parameters
	///
	/// - `ptr`: Pointer to the memory region containing serialized data
	/// - `controller`: Memory controller for deallocation and access
	///
	/// # Returns
	///
	/// The deserialized value.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The pointer is invalid
	/// - The data is malformed
	/// - Deserialization fails
	/// - Memory deallocation fails
	fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self>
	where
		Self: Sized;
}

/// Asynchronous trait for transferring data across WASM boundaries (host side).
///
/// This trait is used on the host side (Wasmtime runtime) where operations may be
/// asynchronous. It provides the same functionality as [`Transfer`] but with async methods.
///
/// # Feature Gate
///
/// This trait is only available when the `host` feature is enabled.
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::{AsyncTransfer, AsyncMemoryController};
///
/// async fn send_data(data: String, controller: &mut dyn AsyncMemoryController) -> u32 {
///     let ptr = data.transfer(controller).await.unwrap();
///     *ptr
/// }
/// ```
#[cfg(feature = "host")]
#[async_trait]
pub trait AsyncTransfer: Send {
	/// Transfer this value into WASM linear memory (async).
	///
	/// This method serializes the value, allocates memory in the WASM linear memory,
	/// writes the serialized data, and returns a pointer to the allocated region.
	///
	/// # Parameters
	///
	/// - `controller`: Async memory controller for allocation and access
	///
	/// # Returns
	///
	/// A [`Ptr`] pointing to the allocated memory region containing the serialized data.
	///
	/// # Errors
	///
	/// Returns an error if allocation fails or serialization fails.
	async fn transfer(self, controller: &mut dyn AsyncMemoryController) -> Result<Ptr>;

	/// Receive a value from WASM linear memory (async).
	///
	/// This method reads serialized data from the given pointer, deserializes it,
	/// and frees the allocated memory.
	///
	/// # Parameters
	///
	/// - `ptr`: Pointer to the memory region containing serialized data
	/// - `controller`: Async memory controller for deallocation and access
	///
	/// # Returns
	///
	/// The deserialized value.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The pointer is invalid
	/// - The data is malformed
	/// - Deserialization fails
	/// - Memory deallocation fails
	async fn receive(ptr: Ptr, controller: &mut dyn AsyncMemoryController) -> Result<Self>
	where
		Self: Sized;
}

/// A type-safe wrapper around a WASM memory pointer.
///
/// This newtype ensures that raw `u32` values aren't accidentally used as pointers,
/// and provides safe conversions to and from various numeric types.
///
/// # Memory Safety
///
/// While this type provides type safety at the Rust level, it does not guarantee
/// memory safety. The underlying pointer must be valid within the WASM linear memory,
/// and the memory region it points to must contain properly formatted data.
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
