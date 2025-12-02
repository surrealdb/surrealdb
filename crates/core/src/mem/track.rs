#![cfg(feature = "allocator")]

use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "allocation-tracking")]
use std::cell::Cell;
#[cfg(feature = "allocation-tracking")]
use std::sync::Weak;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::{AtomicI64, Ordering};

#[cfg(feature = "allocation-tracking")]
use parking_lot::RwLock;

#[cfg(feature = "allocation-tracking")]
static GLOBAL_TOTAL_BYTES: AtomicI64 = AtomicI64::new(0);

#[cfg(feature = "allocation-tracking")]
const BATCH_THRESHOLD: i64 = 12 * 1024; // Flush every 12KB

#[cfg(feature = "allocation-tracking")]
const MAX_DEPTH: u32 = 3; // Max recursion depth for tracking

#[cfg(feature = "allocation-tracking")]
static MEMORY_REPORTERS: RwLock<Vec<Weak<dyn MemoryReporter>>> = RwLock::new(Vec::new());

/// Trait for objects that can report their memory usage to the global allocator tracker
pub trait MemoryReporter: Send + Sync {
	/// Returns the amount of memory currently allocated by this object
	fn memory_allocated(&self) -> usize;
}

#[cfg(feature = "allocation-tracking")]
/// Register a memory reporter to be included in total memory tracking
pub fn register_memory_reporter(reporter: Weak<dyn MemoryReporter>) {
	// Acquire the write lock
	let mut reporters = MEMORY_REPORTERS.write();
	// Clean up dead weak references while we're here
	reporters.retain(|r| r.strong_count() > 0);
	// Add the reporter to the list
	reporters.push(reporter);
}

#[cfg(not(feature = "allocation-tracking"))]
/// Register a memory reporter to be included in total memory tracking
pub fn register_memory_reporter(_: Weak<dyn MemoryReporter>) {
	// Does nothing when allocation tracking is disabled
}

#[cfg(feature = "allocation-tracking")]
/// Clean up dead weak references from the memory reporter registry
pub fn cleanup_memory_reporters() {
	let mut reporters = MEMORY_REPORTERS.write();
	reporters.retain(|r| r.strong_count() > 0);
}

#[cfg(not(feature = "allocation-tracking"))]
/// Clean up dead weak references from the memory reporter registry
pub fn cleanup_memory_reporters() {
	// Does nothing when allocation tracking is disabled
}

#[cfg(feature = "allocation-tracking")]
thread_local! {
	/// Per-thread accumulation buffer for batched updates
	static THREAD_STATE: ThreadState = const { ThreadState::new() };

	/// Recursion depth counter to prevent infinite recursion with nested allocations
	static RECURSION_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[cfg(feature = "allocation-tracking")]
struct ThreadState {
	local_bytes: Cell<i64>,
}

#[cfg(feature = "allocation-tracking")]
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

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, allowing the memory use to be
/// checked at runtime.
///
/// # Important Note on Thread Pools
///
/// This allocator automatically handles thread cleanup via Drop.
/// No manual intervention needed when threads terminate.
///
/// Note: While ThreadState does not implement Drop (due to Rust's restriction
/// that "the global allocator may not use TLS with destructors"), unflushed
/// thread-local bytes are periodically synced via the batch threshold mechanism.
/// At thread termination, any remaining unflushed bytes may not be reflected in
/// the global counter, which is fine for approximate memory tracking purposes.
///
/// # Design Features
///
/// - Lock-free operations for zero contention
/// - Batched updates to reduce atomic operations
/// - Recursion depth tracking prevents infinite recursion while tracking nested allocations
/// - O(1) usage queries regardless of thread count
/// - Automatic cleanup when threads exit
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

impl<A: GlobalAlloc> TrackAlloc<A> {
	/// Returns the current total allocated bytes.
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn memory_allocated(&self) -> usize {
		0
	}

	/// Ensures that local allocations are flushed to the global tracking counter.
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn flush_local_allocations(&self) {
		// Does nothing
	}

	/// Checks if the current usage exceeds a configured threshold.
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}

	/// Returns the current total allocated bytes.
	#[cfg(feature = "allocation-tracking")]
	pub fn memory_allocated(&self) -> usize {
		// Get the heap memory allocated
		let heap_memory = GLOBAL_TOTAL_BYTES.load(Ordering::Relaxed).max(0) as usize;
		// Get the external memory allocated
		let external_memory = MEMORY_REPORTERS
			.read()
			.iter()
			.filter_map(|weak| weak.upgrade())
			.map(|reporter| reporter.memory_allocated())
			.sum::<usize>();
		// Return the total memory allocated
		heap_memory + external_memory
	}

	/// Returns only the heap allocated bytes (excluding external memory reporters).
	#[cfg(feature = "allocation-tracking")]
	pub fn heap_memory_allocated(&self) -> usize {
		GLOBAL_TOTAL_BYTES.load(Ordering::Relaxed).max(0) as usize
	}

	/// Ensures that local allocations are flushed to the global tracking counter.
	#[cfg(feature = "allocation-tracking")]
	pub fn flush_local_allocations(&self) {
		THREAD_STATE.with(|state| {
			state.flush_to_global();
		});
	}

	/// Checks if the current usage exceeds a configured threshold.
	#[cfg(feature = "allocation-tracking")]
	pub fn is_beyond_threshold(&self) -> bool {
		match *crate::cnf::MEMORY_THRESHOLD {
			0 => false,
			v => self.memory_allocated() > v,
		}
	}

	#[cfg(feature = "allocation-tracking")]
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

	#[cfg(feature = "allocation-tracking")]
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

#[cfg(feature = "allocation-tracking")]
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
