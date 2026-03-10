#![cfg(all(feature = "allocator", not(feature = "allocation-tracking")))]

use std::alloc::{GlobalAlloc, Layout, System};

/// This structure implements a wrapper around the system allocator,
/// or a user-specified allocator. It is a thin shim, enabling some
/// functions, but without tracking the current memory allocated.
pub struct NotrackAlloc<Alloc = System> {
	alloc: Alloc,
}

impl<A> NotrackAlloc<A> {
	#[inline]
	pub const fn new(alloc: A) -> Self {
		Self {
			alloc,
		}
	}
}

impl<A: GlobalAlloc> NotrackAlloc<A> {
	/// Returns the current total allocated bytes.
	pub fn memory_allocated(&self) -> usize {
		0
	}

	/// Ensures that local allocations are flushed to the global tracking counter.
	pub fn flush_local_allocations(&self) {
		// Does nothing
	}

	/// Checks if the current usage exceeds a configured threshold.
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for NotrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		// SAFETY: We are forwarding to the underlying allocator with the same layout
		unsafe { self.alloc.alloc(layout) }
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		// SAFETY: We are forwarding to the underlying allocator with the same layout
		unsafe { self.alloc.alloc_zeroed(layout) }
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		// SAFETY: We are forwarding to the underlying allocator with the same ptr and layout
		unsafe { self.alloc.dealloc(ptr, layout) };
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		// SAFETY: We are forwarding to the underlying allocator with the same arguments
		unsafe { self.alloc.realloc(ptr, layout, new_size) }
	}
}
