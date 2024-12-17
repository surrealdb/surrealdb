#![cfg(feature = "allocator")]

use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::{AtomicUsize, Ordering};

/// This atomic counter monitors the amount of memory
/// (in bytes) that is currently allocated for this
/// process at this time.
#[cfg(feature = "allocation-tracking")]
static CURRENT: AtomicUsize = AtomicUsize::new(0);

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, allowing the memory use to be
/// checked at runtime.
#[derive(Debug)]
pub struct TrackAlloc<Alloc = System> {
	alloc: Alloc,
}

impl<A> TrackAlloc<A> {
	#[inline]
	pub const fn new(alloc: A) -> Self {
		Self {
			alloc,
		}
	}
}

impl<A> TrackAlloc<A> {
	/// Returns the number of bytes that are allocated to the process
	#[cfg(feature = "allocation-tracking")]
	pub fn current_usage(&self) -> usize {
		CURRENT.load(Ordering::Relaxed)
	}
	/// Returns the number of bytes that are allocated to the process
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn current_usage(&self) -> usize {
		0
	}
	/// Checks whether the allocator is above the memory limit threshold
	#[cfg(feature = "allocation-tracking")]
	pub fn is_beyond_threshold(&self) -> bool {
		match *crate::cnf::MEMORY_THRESHOLD {
			0 => false,
			v => self.current_usage() > v,
		}
	}
	/// Checks whether the allocator is above the memory limit threshold
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}
}

#[cfg(feature = "allocation-tracking")]
unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ret = self.alloc.alloc(layout);
		if !ret.is_null() {
			CURRENT.fetch_add(layout.size(), Ordering::Relaxed);
		}
		ret
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ret = self.alloc.alloc_zeroed(layout);
		if !ret.is_null() {
			CURRENT.fetch_add(layout.size(), Ordering::Relaxed);
		}
		ret
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
		self.alloc.dealloc(ptr, layout);
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let ret = self.alloc.realloc(ptr, layout, new_size);
		if !ret.is_null() {
			CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
			CURRENT.fetch_add(new_size, Ordering::Relaxed);
		}
		ret
	}
}

#[cfg(not(feature = "allocation-tracking"))]
unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		self.alloc.alloc(layout)
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		self.alloc.alloc_zeroed(layout)
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		self.alloc.dealloc(ptr, layout);
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		self.alloc.realloc(ptr, layout, new_size)
	}
}
