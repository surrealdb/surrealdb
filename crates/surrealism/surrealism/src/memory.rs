/// Allocates a block of memory with the specified size and alignment.
///
/// This function is exposed as a C-compatible export (via `extern "C"`) and is not mangled,
/// making it callable from external code (e.g., WASM host or FFI). It uses Rust's global
/// allocator to reserve memory and returns a pointer offset as a `u32`.
///
/// # Parameters
/// - `len`: The size of the memory block to allocate, in bytes.
///
/// # Returns
/// A `u32` representing the starting offset (pointer) of the allocated memory.
/// Returns `0` if allocation fails (e.g., out-of-memory condition).
///
/// # Panics
/// Panics if the provided size and alignment do not form a valid `Layout` (e.g., alignment
/// is not a power of two or size overflows when padded).
///
/// # Safety
/// This function is unsafe because it performs raw allocation, and the caller must ensure
/// proper deallocation using `__sr_free` to avoid memory leaks. The returned pointer must
/// be valid for the WASM linear memory context if used in such environments.
#[unsafe(no_mangle)]
pub extern "C" fn __sr_alloc(len: u32) -> u32 {
	let layout = match std::alloc::Layout::from_size_align(len as usize, 8) {
		Ok(layout) => layout,
		Err(_) => return 0, // invalid layout
	};

	let ptr = unsafe { std::alloc::alloc(layout) };

	if ptr.is_null() {
		0 // signal OOM or allocation failure
	} else {
		ptr as usize as u32 // cast pointer to offset
	}
}

/// Deallocates a previously allocated block of memory.
///
/// This function is exposed as a C-compatible export (via `extern "C"`) and is not mangled,
/// making it callable from external code. It releases the memory block using Rust's global
/// allocator. Note that the alignment is hardcoded to 8 bytes, which may differ from the
/// original allocation alignment.
///
/// # Parameters
/// - `ptr`: The starting offset (pointer) of the memory block to deallocate.
/// - `len`: The size of the memory block being deallocated, in bytes.
///
/// # Safety
/// This function is unsafe because it performs raw deallocation. The caller must ensure:
/// - The `ptr` is a valid pointer previously returned by `__sr_alloc`.
/// - The `len` matches the originally allocated size.
/// - No further access to the memory occurs after deallocation. Incorrect usage may lead to
///   undefined behavior, such as double-free or use-after-free.
#[unsafe(no_mangle)]
pub extern "C" fn __sr_free(ptr: u32, len: u32) -> u32 {
	let layout = match std::alloc::Layout::from_size_align(len as usize, 8) {
		Ok(layout) => layout,
		Err(_) => return 0, // invalid layout - return 0 to indicate failure
	};

	let ptr = ptr as usize as *mut u8;
	unsafe {
		std::alloc::dealloc(ptr, layout);
	}
	1 // success
}
