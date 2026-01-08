use anyhow::Result;
use surrealism_types::controller::MemoryController;

use crate::memory::{__sr_alloc, __sr_free};

/// A controller struct that manages memory operations in a WASM environment.
///
/// This struct implements the `MemoryController` trait, providing methods for
/// allocating, freeing, and accessing mutable memory slices. It acts as a bridge
/// between Rust code and external memory management functions (e.g., `__sr_alloc`
/// and `__sr_free`), which are likely defined in a WASM host or runtime.
pub struct Controller {}

impl MemoryController for Controller {
	/// Allocates a block of memory with the specified length and alignment.
	///
	/// This method calls an external allocation function (`__sr_alloc`) to reserve
	/// memory in the WASM linear memory space.
	///
	/// # Parameters
	/// - `len`: The length of the memory block to allocate (in bytes).
	/// - `align`: The alignment requirement for the allocated memory (in bytes).
	///
	/// # Returns
	/// A `Result` containing the starting pointer (as `u32`) to the allocated memory
	/// on success, or an error if allocation fails.
	///
	/// # Errors
	/// Returns an error if the underlying allocation function fails (though in this
	/// implementation, it assumes success and wraps the result in `Ok`).
	fn alloc(&mut self, len: u32) -> Result<u32> {
		let result = __sr_alloc(len);
		if result == 0 {
			anyhow::bail!("Memory allocation failed");
		}
		Ok(result)
	}

	/// Frees a previously allocated block of memory.
	///
	/// This method calls an external free function (`__sr_free`) to release the memory
	/// block starting at the given pointer.
	///
	/// # Parameters
	/// - `ptr`: The starting pointer to the memory block to free.
	/// - `len`: The length of the memory block being freed (in bytes).
	///
	/// # Returns
	/// A `Result` indicating success (`Ok(())`) or failure.
	///
	/// # Errors
	/// In this implementation, it assumes success and always returns `Ok(())`. Potential
	/// errors from the underlying free function are not propagated.
	fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
		let result = __sr_free(ptr, len);
		if result == 0 {
			anyhow::bail!("Memory deallocation failed");
		}
		Ok(())
	}

	/// Returns a mutable slice to a region of memory.
	///
	/// This method creates a mutable reference to a slice of bytes in memory starting
	/// at the given pointer and spanning the specified length. It uses unsafe Rust
	/// to construct the slice from raw parts.
	///
	/// # Parameters
	/// - `ptr`: The starting pointer to the memory region.
	/// - `len`: The length of the memory region (in bytes).
	///
	/// # Returns
	/// A mutable slice (`&mut [u8]`) representing the memory region.
	///
	/// # Safety
	/// This function is unsafe because it assumes the pointer is valid, properly aligned,
	/// and that the memory region is mutable and not accessed concurrently. Incorrect
	/// usage may lead to undefined behavior, such as memory corruption or data races.
	fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8] {
		unsafe {
			let ptr = ptr as usize as *mut u8;
			std::slice::from_raw_parts_mut(ptr, len as usize)
		}
	}
}
