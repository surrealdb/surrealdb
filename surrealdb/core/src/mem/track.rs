#![cfg(all(feature = "allocator", feature = "allocation-tracking"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicI64, Ordering};

use common::mem_registry as registry;

use super::usable::ReservedSize;

/// Bytes the program asked for, summed over live allocations.
///
/// The logical size of the process's data: portable across allocators, and the
/// right figure for attribution — "how much are these structures using".
static GLOBAL_REQUESTED_BYTES: AtomicI64 = AtomicI64::new(0);

/// Bytes the allocator reserved, summed over live allocations.
///
/// Requests are rounded up to a size class, so this is the physical footprint
/// and is always at least [`GLOBAL_REQUESTED_BYTES`]. It is allocator- and
/// therefore architecture-specific, which is why the two are tracked
/// separately: this is the figure to compare against an operating-system or
/// container memory limit, and the gap between them is internal fragmentation.
static GLOBAL_RESERVED_BYTES: AtomicI64 = AtomicI64::new(0);

const BATCH_THRESHOLD: i64 = 12 * 1024; // Flush every 12KB

const MAX_DEPTH: u32 = 3; // Max recursion depth for tracking

thread_local! {
	/// Per-thread accumulation buffer for batched updates
	static THREAD_STATE: ThreadState = const { ThreadState::new() };

	/// Recursion depth counter to prevent infinite recursion with nested allocations
	static RECURSION_DEPTH: Cell<u32> = const { Cell::new(0) };
}

struct ThreadState {
	local_requested: Cell<i64>,
	local_reserved: Cell<i64>,
}

impl ThreadState {
	const fn new() -> Self {
		Self {
			local_requested: Cell::new(0),
			local_reserved: Cell::new(0),
		}
	}

	/// Publishes both pending deltas.
	///
	/// The two stores are independent, so a concurrent reader can observe one
	/// counter updated and the other not. Both figures are advisory — they
	/// inform a threshold decision and a metric, neither of which needs a
	/// consistent snapshot — so the counters are never read as a pair that must
	/// agree.
	fn flush_to_global(&self) {
		let requested = self.local_requested.replace(0);
		if requested != 0 {
			GLOBAL_REQUESTED_BYTES.fetch_add(requested, Ordering::Relaxed);
		}
		let reserved = self.local_reserved.replace(0);
		if reserved != 0 {
			GLOBAL_RESERVED_BYTES.fetch_add(reserved, Ordering::Relaxed);
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
	/// Bytes the program requested, summed over live Rust-heap allocations.
	///
	/// Portable across allocators and architectures, because it is the logical
	/// size of the data rather than the space the allocator set aside for it.
	/// Use this to attribute memory to the structures holding it, and to
	/// compare one build against another. It understates the process footprint,
	/// so it must not be used to decide whether a memory limit is near.
	pub fn memory_requested(&self) -> usize {
		GLOBAL_REQUESTED_BYTES.load(Ordering::Relaxed).max(0) as usize
	}

	/// Bytes the allocator reserved, summed over live Rust-heap allocations.
	///
	/// Requests are rounded up to a size class, so this is at least
	/// [`Self::memory_requested`]; the difference is internal fragmentation.
	/// Allocator- and architecture-specific by nature. Excludes external
	/// reporters — see [`Self::memory_allocated`] for the figure the memory
	/// threshold uses.
	pub fn memory_reserved(&self) -> usize {
		GLOBAL_RESERVED_BYTES.load(Ordering::Relaxed).max(0) as usize
	}

	/// The total tracked bytes, and the figure `SURREAL_MEMORY_THRESHOLD` is
	/// compared against: reserved Rust-heap bytes plus every registered
	/// external memory reporter.
	///
	/// Built on [`Self::memory_reserved`] rather than
	/// [`Self::memory_requested`] because the threshold exists to keep the
	/// process inside an operating-system or container memory limit, and the
	/// kernel accounts for what the allocator holds, not what the program
	/// asked for.
	///
	/// Still a lower bound on resident memory: it excludes allocator arena
	/// metadata, slab-level fragmentation, and pages the allocator has kept
	/// rather than returned to the operating system. External reporters cover
	/// memory allocated outside Rust's global allocator, which the heap terms
	/// cannot see at all — currently RocksDB's caches. Memory that no term
	/// covers, notably WebAssembly linear memory, is absent entirely.
	pub fn memory_allocated(&self) -> usize {
		// Get the reserved heap memory
		let heap_memory = self.memory_reserved();
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
		self.beyond_threshold_by().is_some()
	}

	/// The tracked total and the threshold it exceeded, or `None` when the
	/// guard is disabled or the total is within it.
	///
	/// Reporting a refusal needs both figures, and computing the total is not
	/// cheap — it walks the reporter registry and sums the RocksDB cache across
	/// its shards. Returning them from the check keeps a refusal to one
	/// computation rather than one per caller that wants to name the numbers.
	pub fn beyond_threshold_by(&self) -> Option<(usize, usize)> {
		match *surrealdb_cnf::MEMORY_THRESHOLD {
			0 => None,
			threshold => {
				let allocated = self.memory_allocated();
				(allocated > threshold).then_some((allocated, threshold))
			}
		}
	}

	fn add(&self, requested: usize, reserved: usize) {
		self.record(requested as i64, reserved as i64);
	}

	fn sub(&self, requested: usize, reserved: usize) {
		self.record(-(requested as i64), -(reserved as i64));
	}

	/// Accumulate a signed delta against both thread-local counters, publishing
	/// them once either one has drifted a batch's worth from the global totals.
	fn record(&self, requested: i64, reserved: i64) {
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
		// Update the tracked byte counts
		THREAD_STATE.with(|state| {
			let requested = state.local_requested.get().saturating_add(requested);
			let reserved = state.local_reserved.get().saturating_add(reserved);
			state.local_requested.set(requested);
			state.local_reserved.set(reserved);
			// Each accumulator needs its own trigger. `reserved >= requested`
			// holds for a single allocation but not for the signed running
			// totals: allocating small objects while releasing large ones moves
			// the two by different amounts, so either can reach the threshold
			// while the other sits near zero. Bounding only one leaves the other
			// free to lag the global total without limit.
			if requested.saturating_abs() >= BATCH_THRESHOLD
				|| reserved.saturating_abs() >= BATCH_THRESHOLD
			{
				state.flush_to_global();
			}
		});
		// Decrement the recursion depth
		RECURSION_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
	}
}

/// Each arm records two figures per allocation: the requested
/// [`Layout::size`], and the size the allocator reserves for that layout.
///
/// Both figures are functions of the layout, and the deallocation path receives
/// the layout its allocation was made with, so the value subtracted always
/// equals the value added and neither counter drifts. Deriving the reserved
/// size from the layout rather than from the pointer also keeps the allocator's
/// sized-deallocation fast path intact, which a metadata lookup before the free
/// would defeat.
unsafe impl<A: GlobalAlloc + ReservedSize> GlobalAlloc for TrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ret = unsafe { self.alloc.alloc(layout) };
		if !ret.is_null() {
			self.add(layout.size(), self.alloc.reserved_size(layout));
		}
		ret
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		let reserved = self.alloc.reserved_size(layout);
		unsafe { self.alloc.dealloc(ptr, layout) };
		self.sub(layout.size(), reserved);
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ret = unsafe { self.alloc.alloc_zeroed(layout) };
		if !ret.is_null() {
			self.add(layout.size(), self.alloc.reserved_size(layout));
		}
		ret
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let ret = unsafe { self.alloc.realloc(ptr, layout, new_size) };
		// On failure the original allocation is untouched and still counted.
		if !ret.is_null() {
			// SAFETY: `realloc` requires `new_size` to be a valid size for
			// `layout`'s alignment, which is what the returned block was
			// allocated with.
			let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
			self.sub(layout.size(), self.alloc.reserved_size(layout));
			self.add(new_size, self.alloc.reserved_size(new_layout));
		}
		ret
	}
}

#[cfg(test)]
mod tests {
	use std::alloc::{Layout, alloc, dealloc};

	use super::*;
	use crate::mem::ALLOC;

	/// Sizes chosen to fall between size-class boundaries on both jemalloc and
	/// mimalloc, so the allocator has to round each one up.
	const AWKWARD_SIZES: [usize; 6] = [1, 3, 17, 100, 1000, 6000];

	/// Alignments spanning the three routes an allocator can take: below
	/// `max_align_t`, at it, and above it — where both allocators stop serving
	/// the request from a natural block. The over-aligned cases are the ones
	/// that distinguish a layout-derived size from a pointer query.
	const ALIGNMENTS: [usize; 4] = [8, 16, 64, 4096];

	/// Every (size, alignment) pair the engine can ask for.
	fn layouts() -> impl Iterator<Item = Layout> {
		AWKWARD_SIZES.into_iter().flat_map(|size| {
			ALIGNMENTS
				.into_iter()
				.filter_map(move |align| Layout::from_size_align(size, align).ok())
		})
	}

	/// The reserved size must never be below the requested size: it is the size
	/// class the request was rounded up into.
	#[test]
	fn reserved_size_covers_the_requested_size() {
		for layout in layouts() {
			let reserved = ALLOC.alloc.reserved_size(layout);
			assert!(
				reserved >= layout.size(),
				"reserved {reserved} bytes for a {}-byte request at align {}",
				layout.size(),
				layout.align(),
			);
		}
	}

	/// The reserved size must depend only on the layout.
	///
	/// The allocator adds it on allocation and subtracts it on release, so a
	/// figure that varied between those two points — as a query against the
	/// returned pointer does for an over-aligned allocation, where the reported
	/// size excludes a per-allocation alignment adjustment — would drift the
	/// tracked total by the difference on every allocation the process makes.
	/// Allocation traffic runs between the two reads so a pointer-derived
	/// implementation cannot pass by coincidence.
	#[test]
	fn reserved_size_depends_only_on_the_layout() {
		for layout in layouts() {
			let first = ALLOC.alloc.reserved_size(layout);

			// SAFETY: non-zero size; every allocation below is released.
			let churn: Vec<*mut u8> = layouts()
				.map(|l| {
					let ptr = unsafe { alloc(l) };
					assert!(!ptr.is_null(), "allocation of {} bytes failed", l.size());
					ptr
				})
				.collect();
			for (ptr, l) in churn.into_iter().zip(layouts()) {
				// SAFETY: `ptr` came from `alloc` with the same layout.
				unsafe { dealloc(ptr, l) };
			}

			let second = ALLOC.alloc.reserved_size(layout);
			assert_eq!(
				first,
				second,
				"reserved size changed across allocation traffic for {} bytes at align {}",
				layout.size(),
				layout.align(),
			);
		}
	}

	/// Reserved bytes must never fall below requested bytes: every live
	/// allocation contributes at least its requested size to both counters, so
	/// the reserved total dominates. An inversion would mean one counter had
	/// drifted.
	#[test]
	fn reserved_total_dominates_requested_total() {
		// Hold a mixture of awkward sizes so fragmentation is non-trivial.
		let held: Vec<Vec<u8>> = AWKWARD_SIZES.iter().map(|&s| Vec::with_capacity(s)).collect();
		ALLOC.flush_local_allocations();

		let requested = ALLOC.memory_requested();
		let reserved = ALLOC.memory_reserved();
		assert!(
			reserved >= requested,
			"reserved {reserved} bytes is below requested {requested} bytes"
		);

		drop(held);
	}
}
