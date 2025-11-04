//! Memory management abstractions for WASM linear memory.
//!
//! This module defines traits for allocating and deallocating memory in WASM linear memory,
//! as well as accessing that memory. It provides both synchronous (guest-side) and
//! asynchronous (host-side) interfaces.
//!
//! # Memory Model
//!
//! WASM uses a linear memory model - a contiguous array of bytes that can grow over time.
//! The memory controller abstractions allow:
//!
//! - **Allocation**: Reserve a region of memory with specific size and alignment
//! - **Deallocation**: Free a previously allocated region
//! - **Access**: Read and write to memory regions via byte slices
//!
//! # Safety
//!
//! While these traits provide safe Rust interfaces, they operate on raw memory pointers.
//! Implementations must ensure:
//! - Allocated pointers are valid and within bounds
//! - Deallocations match previous allocations
//! - Memory access doesn't exceed allocated regions
//! - Alignment requirements are respected

use anyhow::Result;
#[cfg(feature = "host")]
use async_trait::async_trait;

/// Synchronous memory controller for WASM linear memory (guest side).
///
/// This trait provides the interface for managing memory allocations within a WASM module.
/// It's designed to be used on the guest side where all operations are synchronous.
///
/// # Implementation Notes
///
/// Implementers typically:
/// - Use the WASM allocator (`__alloc`, `__free` exports)
/// - Track allocations for cleanup
/// - Validate alignment and bounds
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::MemoryController;
///
/// struct MyController {
///     memory: Vec<u8>,
/// }
///
/// impl MemoryController for MyController {
///     fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
///         // Allocate aligned memory...
///         Ok(ptr)
///     }
///
///     fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
///         // Deallocate memory...
///         Ok(())
///     }
///
///     fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8] {
///         // Return mutable slice to memory region...
///         &mut self.memory[ptr as usize..(ptr + len) as usize]
///     }
/// }
/// ```
pub trait MemoryController {
	/// Allocate a region of WASM linear memory.
	///
	/// # Parameters
	///
	/// - `len`: Number of bytes to allocate
	/// - `align`: Alignment requirement in bytes (must be a power of 2)
	///
	/// # Returns
	///
	/// A pointer (`u32`) to the start of the allocated region.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - Allocation fails (out of memory)
	/// - Alignment is invalid (not a power of 2)
	/// - Memory limit would be exceeded
	fn alloc(&mut self, len: u32) -> Result<u32>;

	/// Free a previously allocated region of memory.
	///
	/// # Parameters
	///
	/// - `ptr`: Pointer to the start of the region (from a previous `alloc` call)
	/// - `len`: Size of the region in bytes (must match the original allocation)
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The pointer is invalid or wasn't allocated
	/// - The length doesn't match the original allocation
	/// - Double-free is detected
	///
	/// # Safety Notes
	///
	/// After calling `free`, the pointer and any references to that memory region
	/// become invalid and must not be used.
	fn free(&mut self, ptr: u32, len: u32) -> Result<()>;

	/// Get mutable access to a region of WASM linear memory.
	///
	/// # Parameters
	///
	/// - `ptr`: Pointer to the start of the region
	/// - `len`: Length of the region in bytes
	///
	/// # Returns
	///
	/// A mutable slice to the memory region.
	///
	/// # Panics
	///
	/// May panic if the pointer or length are out of bounds. Implementations should
	/// validate bounds before returning the slice.
	fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8];
}

/// Asynchronous memory controller for WASM linear memory (host side).
///
/// This trait provides an async interface for managing memory allocations when
/// interacting with a WASM module from the host (runtime). It mirrors [`MemoryController`]
/// but with async methods to support asynchronous runtimes like Tokio.
///
/// # Feature Gate
///
/// This trait is only available when the `host` feature is enabled.
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::AsyncMemoryController;
/// use async_trait::async_trait;
///
/// struct MyAsyncController {
///     // ... state ...
/// }
///
/// #[async_trait]
/// impl AsyncMemoryController for MyAsyncController {
///     async fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
///         // Call WASM allocator function asynchronously...
///         Ok(ptr)
///     }
///
///     async fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
///         // Call WASM deallocator function asynchronously...
///         Ok(())
///     }
///
///     fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8] {
///         // Access WASM memory (synchronous)...
///         &mut memory[ptr as usize..(ptr + len) as usize]
///     }
/// }
/// ```
#[cfg(feature = "host")]
#[async_trait]
pub trait AsyncMemoryController: Send {
	/// Allocate a region of WASM linear memory (async).
	///
	/// # Parameters
	///
	/// - `len`: Number of bytes to allocate
	/// - `align`: Alignment requirement in bytes (must be a power of 2)
	///
	/// # Returns
	///
	/// A pointer (`u32`) to the start of the allocated region.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - Allocation fails (out of memory)
	/// - Alignment is invalid (not a power of 2)
	/// - WASM function call fails
	async fn alloc(&mut self, len: u32) -> Result<u32>;

	/// Free a previously allocated region of memory (async).
	///
	/// # Parameters
	///
	/// - `ptr`: Pointer to the start of the region (from a previous `alloc` call)
	/// - `len`: Size of the region in bytes (must match the original allocation)
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The pointer is invalid or wasn't allocated
	/// - The length doesn't match the original allocation
	/// - WASM function call fails
	async fn free(&mut self, ptr: u32, len: u32) -> Result<()>;

	/// Get mutable access to a region of WASM linear memory.
	///
	/// Note: This method is synchronous even in the async trait because memory
	/// access itself doesn't require async operations - only the allocation/deallocation
	/// involves calling WASM functions.
	///
	/// # Parameters
	///
	/// - `ptr`: Pointer to the start of the region
	/// - `len`: Length of the region in bytes
	///
	/// # Returns
	///
	/// A mutable slice to the memory region.
	///
	/// # Panics
	///
	/// May panic if the pointer or length are out of bounds.
	fn mut_mem(&mut self, ptr: u32, len: u32) -> Result<&mut [u8]>;
}
