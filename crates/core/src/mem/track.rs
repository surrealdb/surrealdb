#![cfg(all(feature = "allocator", feature = "allocation-tracking"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::mem::registry;

static GLOBAL_TOTAL_BYTES: AtomicI64 = AtomicI64::new(0);

const BATCH_THRESHOLD: i64 = 12 * 1024; // Flush every 12KB

const MAX_DEPTH: u32 = 3; // Max recursion depth for tracking

thread_local! {
	/// Per-thread accumulation buffer for batched updates
	static THREAD_STATE: ThreadState = const { ThreadState::new() };

	/// Recursion depth counter to prevent infinite recursion with nested allocations
	static RECURSION_DEPTH: Cell<u32> = const { Cell::new(0) };
}

struct ThreadState {
	local_bytes: Cell<i64>,
}

impl ThreadState {
	const fn new() -> Self {
		Self {
			local_bytes: Cell::new(0),
		}
	}

	fn flush_to_global(&self) {
		let delta = self.local_bytes.get();
		if delta != 0 {
			GLOBAL_TOTAL_BYTES.fetch_add(delta, Ordering::Relaxed);
			self.local_bytes.set(0);
		}
	}
}

/// This structure implements a wrapper around the system allocator,
/// or around a user-specified allocator. It tracks the current memory
/// which is allocated, allowing the memory use to be checked at runtime.
///
/// # Important Note on Thread Pools
///
/// This allocator automatically batches thread allocations and syncs to
/// the global counter after a threshold is reached. Threads are tracked
/// using thread-local storage, and the global counter is updated atomically.
///
/// While ThreadState does not implement Drop (due to Rust's restriction
/// that "the global allocator may not use TLS with destructors"), unflushed
/// thread-local bytes are periodically synced via the batch threshold mechanism.
/// At thread termination, any remaining unflushed bytes may not be reflected in
/// the global counter resulting in a potential discrepancy between the actual
/// allocated memory and the reported memory. With Tokio threads, this is not
/// a problem as the `flush_local_allocations` function is called for each
/// thread before the thread is dropped.
///
/// For other thread pools, it is recommended to call `flush_local_allocations`
/// before the thread is dropped, where possible.
///
/// # Design Features
///
/// - Lock-free operations for zero contention
/// - Batched updates to reduce atomic operations
/// - Recursion depth tracking prevents infinite recursion while tracking nested allocations
/// - O(1) usage queries regardless of thread count
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

impl<A: GlobalAlloc> TrackAlloc<A> {
	/// Returns the current total allocated bytes.
	pub fn memory_allocated(&self) -> usize {
		// Get the heap memory allocated
		let heap_memory = GLOBAL_TOTAL_BYTES.load(Ordering::Relaxed).max(0) as usize;
		// Get the external memory allocated
		let external_memory = registry::memory_reporters_allocated_total();
		// Return the total memory allocated
		heap_memory + external_memory
	}

	/// Ensures that local allocations are flushed to the global tracking counter.
	pub fn flush_local_allocations(&self) {
		THREAD_STATE.with(|state| {
			state.flush_to_global();
		});
	}

	/// Checks if the current usage exceeds a configured threshold.
	pub fn is_beyond_threshold(&self) -> bool {
		match *crate::cnf::MEMORY_THRESHOLD {
			0 => false,
			v => self.memory_allocated() > v,
		}
	}

	fn add(&self, size: usize) {
		// Track the current recursion depth
		let depth = RECURSION_DEPTH.with(|d| {
			let current = d.get();
			if current >= MAX_DEPTH {
				return MAX_DEPTH;
			}
			d.set(current + 1);
			current
		});
		// Don't recursively track too deep
		if depth >= MAX_DEPTH {
			return;
		}
		// Update the tracked byte count
		THREAD_STATE.with(|state| {
			let bytes = state.local_bytes.get() + size as i64;
			state.local_bytes.set(bytes);
			if bytes >= BATCH_THRESHOLD {
				state.flush_to_global();
			}
		});
		// Decrement the recursion depth
		RECURSION_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
	}

	fn sub(&self, size: usize) {
		// Track the current recursion depth
		let depth = RECURSION_DEPTH.with(|d| {
			let current = d.get();
			if current >= MAX_DEPTH {
				return MAX_DEPTH;
			}
			d.set(current + 1);
			current
		});
		// Don't recursively track too deep
		if depth >= MAX_DEPTH {
			return;
		}
		// Update the tracked byte count
		THREAD_STATE.with(|state| {
			let bytes = state.local_bytes.get() - size as i64;
			state.local_bytes.set(bytes);
			if bytes <= -BATCH_THRESHOLD {
				state.flush_to_global();
			}
		});
		// Decrement the recursion depth
		RECURSION_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
	}
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ret = unsafe { self.alloc.alloc(layout) };
		if !ret.is_null() {
			self.add(layout.size());
		}
		ret
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { self.alloc.dealloc(ptr, layout) };
		self.sub(layout.size());
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ret = unsafe { self.alloc.alloc_zeroed(layout) };
		if !ret.is_null() {
			self.add(layout.size());
		}
		ret
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let ret = unsafe { self.alloc.realloc(ptr, layout, new_size) };
		if !ret.is_null() {
			self.sub(layout.size());
			self.add(new_size);
		}
		ret
	}
}
