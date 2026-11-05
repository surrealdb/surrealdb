#![cfg(not(feature = "allocator"))]

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, and the total memory allocated
/// across the duration of the programme. This
/// memory use can then be checked at runtime.
#[derive(Debug)]
pub struct FakeAlloc;

impl Default for FakeAlloc {
	fn default() -> Self {
		Self::new()
	}
}

impl FakeAlloc {
	#[inline]
	pub const fn new() -> Self {
		Self {}
	}
}

impl FakeAlloc {
	/// Always `0`: this allocator does not track allocations.
	pub fn memory_requested(&self) -> usize {
		0
	}

	/// Always `0`: this allocator does not track allocations.
	pub fn memory_reserved(&self) -> usize {
		0
	}

	/// Always `0`: this allocator does not track allocations, so the memory
	/// threshold cannot be enforced and [`Self::is_beyond_threshold`] is
	/// always `false` regardless of how `SURREAL_MEMORY_THRESHOLD` is set.
	pub fn memory_allocated(&self) -> usize {
		0
	}

	/// Ensures that local allocations are flushed to the global tracking counter.
	pub fn flush_local_allocations(&self) {
		// Does nothing
	}

	/// Always `false`: without allocation tracking there is nothing to compare
	/// against a threshold, so no query is ever refused for memory.
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}

	/// Always `None`: see [`Self::is_beyond_threshold`].
	pub fn beyond_threshold_by(&self) -> Option<(usize, usize)> {
		None
	}
}
