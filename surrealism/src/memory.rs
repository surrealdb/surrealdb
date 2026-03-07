/// Allocate `len` bytes (alignment 8) in WASM linear memory.
/// Returns the pointer offset, or `0` on failure.
///
/// # Safety
/// Caller must pair with `__sr_free` using the same pointer and length.
#[unsafe(no_mangle)]
pub extern "C" fn __sr_alloc(len: u32) -> u32 {
	let layout = match std::alloc::Layout::from_size_align(len as usize, 8) {
		Ok(layout) => layout,
		Err(_) => return 0,
	};

	let ptr = unsafe { std::alloc::alloc(layout) };

	if ptr.is_null() {
		0
	} else {
		ptr as usize as u32
	}
}

/// Free a block previously allocated by `__sr_alloc`.
/// Returns `1` on success, `0` on failure.
///
/// # Safety
/// `ptr` must have been returned by `__sr_alloc` with the same `len`.
/// The memory must not be accessed after this call.
#[unsafe(no_mangle)]
pub extern "C" fn __sr_free(ptr: u32, len: u32) -> u32 {
	let layout = match std::alloc::Layout::from_size_align(len as usize, 8) {
		Ok(layout) => layout,
		Err(_) => return 0,
	};

	let ptr = ptr as usize as *mut u8;
	unsafe {
		std::alloc::dealloc(ptr, layout);
	}
	1
}
